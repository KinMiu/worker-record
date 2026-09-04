package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/log"

	"github.com/KinMiu/worker-record/config"
	"github.com/KinMiu/worker-record/pkg/handler/consumer"
	apperr "github.com/KinMiu/worker-record/pkg/handler/err"
	apphttp "github.com/KinMiu/worker-record/pkg/handler/http"
	"github.com/KinMiu/worker-record/pkg/handler/message_broker"
	"github.com/KinMiu/worker-record/pkg/router"
	"github.com/KinMiu/worker-record/pkg/service"
)

// @title Worker Record RTSP Direct Chunk Recorder API
// @version 1.0
// @description Direct Chunk CCTV Recorder & Event Publisher Worker for Way Kambas Wildlife Surveillance System
// @BasePath /
func main() {
	log.Info("=================================================================")
	log.Info("  CCTV Direct Chunk Recording & Event Publisher Worker")
	log.Info("  Way Kambas Wildlife Surveillance System (PT. LSKK Standard)")
	log.Info("=================================================================")

	// Phase 1: Load Environment Variables
	if err := config.LoadEnv(); err != nil {
		log.Warn("[MAIN] No .env file found, using system environment variables")
	}

	workerID := config.WORKER_ID.GetValueOrDefault("worker_recorder_01")
	httpPort := config.PORT.GetValueOrDefault("3002")
	ffmpegPath := config.FFMPEG_PATH.GetValueOrDefault("ffmpeg")
	recordStoragePath := config.RECORD_STORAGE_PATH.GetValueOrDefault("/opt/recordings/queue")

	log.Infof("[MAIN] Worker ID          : %s", workerID)
	log.Infof("[MAIN] HTTP Port          : %s", httpPort)
	log.Infof("[MAIN] API Base URL       : %s", config.API_BASE_URL.GetValue())
	log.Infof("[MAIN] MQTT Broker        : %s", config.MQTT_BROKER.GetValueOrDefault("tcp://localhost:1883"))
	log.Infof("[MAIN] RabbitMQ URL       : %s", config.GetRabbitMQURL())
	log.Infof("[MAIN] RabbitMQ Queue     : %s", config.RABBITMQ_QUEUE_NAME.GetValueOrDefault("cctv.recordings"))
	log.Infof("[MAIN] Record Storage Path: %s", recordStoragePath)
	log.Infof("[MAIN] Playback Base URL  : %s", config.RECORDING_BASE_URL.GetValueOrDefault("http://127.0.0.1:9000/recordings"))
	log.Infof("[MAIN] Chunk Duration     : %ds (%.1fm)", config.SEGMENT_DURATION_SECONDS.GetValueInt(300), float64(config.SEGMENT_DURATION_SECONDS.GetValueInt(300))/60.0)
	log.Infof("[MAIN] S3 Upload Enabled  : %t", config.ENABLE_S3_UPLOAD.GetValueBool(false))
	log.Infof("[MAIN] FFmpeg Path        : %s", ffmpegPath)

	if _, err := exec.LookPath(ffmpegPath); err != nil {
		log.Warnf("[WARNING] FFmpeg executable '%s' was not found in system PATH. Ensure FFmpeg is installed!", ffmpegPath)
	} else {
		log.Infof("[MAIN] FFmpeg executable verified: %s", ffmpegPath)
	}

	if err := os.MkdirAll(recordStoragePath, 0755); err != nil {
		log.Warnf("[WARNING] Could not create storage root directory %s: %v", recordStoragePath, err)
	}

	// Phase 2: Initialize RabbitMQ Message Broker (Publisher)
	rmqBroker, err := message_broker.InitRabbitMQBroker()
	if err != nil {
		log.Warnf("[MAIN] Initial RabbitMQ connection failed: %v. (Auto-reconnect active)", err)
	}

	// Phase 3: Initialize Services & Upload Pool
	s3UploaderService := service.NewS3UploaderService()
	uploadPoolService := service.NewUploadPoolService(s3UploaderService, rmqBroker, 4, 200)

	uploadPoolCtx, uploadPoolCancel := context.WithCancel(context.Background())
	uploadPoolService.Start(uploadPoolCtx)

	recorderManagerService := service.NewRecorderManagerService(uploadPoolService)
	deviceClientService := service.NewDeviceClientService()
	healthHandler := apphttp.NewHealthHandler(recorderManagerService)
	cameraConsumer := consumer.NewCameraConsumer(deviceClientService, recorderManagerService)

	// Phase 4: Initialize Fiber HTTP Server
	app := fiber.New(fiber.Config{
		ErrorHandler: apperr.ErrorHandler,
		AppName:      "Worker Record Chunk Streamer v1.0",
	})
	router.SetupRoutes(app, healthHandler)

	go func() {
		addr := fmt.Sprintf(":%s", httpPort)
		log.Infof("[MAIN] Starting Fiber HTTP server on %s...", addr)
		if err := app.Listen(addr); err != nil {
			log.Infof("[MAIN] Fiber server listener stopped: %v", err)
		}
	}()

	// Phase 5: Initial Bootstrap - Fetch devices from backend REST API
	log.Info("[MAIN] Bootstrapping camera list from REST API...")
	devices, err := deviceClientService.FetchDevices()
	if err != nil {
		log.Warnf("[MAIN] Initial device bootstrap from REST API failed: %v. Relying on MQTT events...", err)
	} else {
		log.Infof("[MAIN] Retrieved %d device(s). Starting Direct Chunk recorder loops...", len(devices))
		recorderManagerService.ReconcileDevices(devices)
	}

	// Phase 6: Initialize MQTT Broker & Register Synchronization Consumers
	mqttBroker, err := message_broker.InitMQTTBroker()
	if err != nil {
		log.Warnf("[MAIN] Failed to connect to MQTT broker on startup: %v. Broker will retry in background...", err)
	} else {
		// Specific worker topic (e.g. workers/worker_recorder_01/events)
		workerTopic := fmt.Sprintf("workers/%s/events", workerID)
		_ = mqttBroker.ConsumeMQTTTopic(workerTopic, 1, cameraConsumer.HandleEventMessage)

		// Custom configured camera topic if specified
		customTopic := config.MQTT_CAMERA_TOPIC.GetValue()
		if customTopic != "" && customTopic != workerTopic {
			_ = mqttBroker.ConsumeMQTTTopic(customTopic, 1, cameraConsumer.HandleEventMessage)
		}

		// Broadcast topic across all workers
		broadcastTopic := "workers/events"
		_ = mqttBroker.ConsumeMQTTTopic(broadcastTopic, 1, cameraConsumer.HandleEventMessage)
	}

	log.Info("=================================================================")
	log.Info("  Worker initialized and running. Direct chunk recording active...")
	log.Info("  Awaiting MQTT events / RabbitMQ tasks / HTTP requests...")
	log.Info("  Press Ctrl+C or send SIGTERM to terminate.")
	log.Info("=================================================================")

	// Phase 7: Graceful Shutdown Handling (signal.NotifyContext)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	<-ctx.Done()
	log.Info("[MAIN] Received shutdown signal. Initiating graceful shutdown sequence...")

	// Step A: Disconnect MQTT client
	if message_broker.DefaultMQTTBroker != nil {
		message_broker.DefaultMQTTBroker.Disconnect()
	}

	// Step B: Stop all camera recording loops (wait for in-flight FFmpeg to finalize)
	recorderManagerService.StopAll()

	// Step C: Drain and terminate upload worker pool
	uploadPoolCancel()
	uploadPoolService.DrainAndStop()

	// Step D: Close RabbitMQ publisher connection
	if message_broker.DefaultRabbitMQBroker != nil {
		message_broker.DefaultRabbitMQBroker.Close()
	}

	// Step E: Shutdown Fiber HTTP server with timeout
	if err := app.ShutdownWithTimeout(3 * time.Second); err != nil {
		log.Errorf("[MAIN] Fiber HTTP server shutdown error: %v", err)
	}

	log.Info("=================================================================")
	log.Info("  CCTV Recording Worker shut down cleanly. Goodbye!")
	log.Info("=================================================================")
}
