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

	// Phase 2: Initialize RabbitMQ Message Broker
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
		log.Warnf("[MAIN] Initial device bootstrap from REST API failed: %v. Check API_BASE_URL or connectivity.", err)
	} else {
		log.Infof("[MAIN] Retrieved %d device(s). Starting Direct Chunk recorder loops...", len(devices))
		recorderManagerService.ReconcileDevices(devices)
	}

	log.Info("=================================================================")
	log.Info("  Worker initialized and running. Direct chunk recording active...")
	log.Info("  Press Ctrl+C or send SIGTERM to terminate.")
	log.Info("=================================================================")

	// Phase 6: Graceful Shutdown Handling (signal.NotifyContext)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	<-ctx.Done()
	log.Info("[MAIN] Received shutdown signal. Initiating graceful shutdown sequence...")

	// Step A: Stop all camera recording loops (wait for in-flight FFmpeg to finalize)
	recorderManagerService.StopAll()

	// Step B: Drain and terminate upload worker pool
	uploadPoolCancel()
	uploadPoolService.DrainAndStop()

	// Step C: Close RabbitMQ publisher connection
	if message_broker.DefaultRabbitMQBroker != nil {
		message_broker.DefaultRabbitMQBroker.Close()
	}

	// Step D: Shutdown Fiber HTTP server with timeout
	if err := app.ShutdownWithTimeout(3 * time.Second); err != nil {
		log.Errorf("[MAIN] Fiber HTTP server shutdown error: %v", err)
	}

	log.Info("=================================================================")
	log.Info("  CCTV Recording Worker shut down cleanly. Goodbye!")
	log.Info("=================================================================")
}
