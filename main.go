package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/KinMiu/worker-record/internal/client"
	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/publisher"
	"github.com/KinMiu/worker-record/internal/recorder"
	"github.com/KinMiu/worker-record/internal/uploader"
	"github.com/KinMiu/worker-record/internal/watcher"
)

func main() {
	log.Println("=================================================================")
	log.Println("  CCTV Local Recording & Event Publisher Worker")
	log.Println("  Way Kambas Wildlife Surveillance System (Golang)")
	log.Println("=================================================================")

	// 1. Load configuration from .env and environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Configuration initialization failed: %v", err)
	}

	log.Printf("[MAIN] Worker ID          : %s", cfg.WorkerID)
	log.Printf("[MAIN] API Base URL       : %s", cfg.APIBaseURL)
	log.Printf("[MAIN] API Key Header     : %s", cfg.APIKeyHeader)
	if cfg.APIKey != "" {
		log.Printf("[MAIN] API Key            : [CONFIGURED]")
	} else {
		log.Printf("[MAIN] API Key            : [NOT SET]")
	}
	if cfg.APIAuthToken != "" {
		log.Printf("[MAIN] API Auth Token     : [CONFIGURED]")
	} else {
		log.Printf("[MAIN] API Auth Token     : [NOT SET]")
	}
	log.Printf("[MAIN] RabbitMQ URL       : %s", cfg.RabbitMQURL)
	log.Printf("[MAIN] RabbitMQ Queue     : %s", cfg.RabbitMQQueueName)
	log.Printf("[MAIN] Record Storage Path: %s", cfg.RecordStoragePath)
	log.Printf("[MAIN] Playback Base URL  : %s", cfg.RecordingBaseURL)
	log.Printf("[MAIN] Segment Duration   : %d second(s) (%.1f minutes)", cfg.SegmentDurationSeconds, float64(cfg.SegmentDurationSeconds)/60.0)
	log.Printf("[MAIN] Scan Interval      : %d second(s)", cfg.ScanIntervalSeconds)
	log.Printf("[MAIN] Retry Interval     : %d second(s)", cfg.RetryIntervalSeconds)
	log.Printf("[MAIN] S3 Upload Enabled  : %t", cfg.EnableS3Upload)
	log.Printf("[MAIN] FFmpeg Path        : %s", cfg.FFmpegPath)

	// 2. Verify FFmpeg binary availability
	if _, err := exec.LookPath(cfg.FFmpegPath); err != nil {
		log.Printf("[WARNING] FFmpeg executable '%s' was not found in system PATH. Please ensure FFmpeg is installed!", cfg.FFmpegPath)
	} else {
		log.Printf("[MAIN] FFmpeg executable verified: %s", cfg.FFmpegPath)
	}

	// 3. Ensure local root recording directory exists
	if err := os.MkdirAll(cfg.RecordStoragePath, 0755); err != nil {
		log.Printf("[WARNING] Could not create storage root directory %s: %v", cfg.RecordStoragePath, err)
	}

	// 4. Initialize RabbitMQ Publisher
	rmqPublisher := publisher.NewRabbitMQPublisher(cfg)
	if err := rmqPublisher.Connect(); err != nil {
		log.Printf("[WARNING] Initial RabbitMQ connection failed: %v. (Auto-reconnect will retry in background)", err)
	}

	// 5. Initialize S3 Uploader, Recorder Manager, and File Watcher
	s3Uploader := uploader.NewS3Uploader(cfg)
	recorderMgr := recorder.NewRecorderManager(cfg)
	fileWatcher := watcher.NewFileWatcher(cfg, recorderMgr, rmqPublisher, s3Uploader)

	// 6. Bootstrap camera devices from Central REST API
	apiClient := client.NewAPIClient(cfg)
	log.Println("[MAIN] Bootstrapping camera list from REST API...")
	devices, err := apiClient.FetchDevices()
	if err != nil {
		log.Printf("[WARNING] Initial device bootstrap from REST API failed: %v. Check API_BASE_URL or connectivity.", err)
	} else {
		log.Printf("[MAIN] Retrieved %d device(s). Starting camera recorder processes...", len(devices))
		recorderMgr.ReconcileDevices(devices)
	}

	// 7. Start File Watcher background goroutine
	rootCtx, rootCancel := context.WithCancel(context.Background())
	go fileWatcher.Start(rootCtx)

	log.Println("=================================================================")
	log.Println("  Worker initialized and running. Awaiting recordings & events...")
	log.Println("  Press Ctrl+C or send SIGTERM to terminate.")
	log.Println("=================================================================")

	// 8. Graceful Shutdown listener
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-shutdownSig
	log.Printf("[MAIN] Received shutdown signal (%s). Commencing graceful shutdown...", sig.String())

	// Step A: Stop file watcher loop
	rootCancel()

	// Step B: Stop all camera recording runners and wait for FFmpeg child processes to flush & exit
	recorderMgr.StopAll()

	// Step C: Close RabbitMQ publisher connection
	rmqPublisher.Close()

	// Allow brief moment for disk buffers to synchronize
	time.Sleep(500 * time.Millisecond)

	log.Println("=================================================================")
	log.Println("  CCTV Recording Worker shut down cleanly. Goodbye!")
	log.Println("=================================================================")
}
