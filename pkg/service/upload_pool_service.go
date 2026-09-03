package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2/log"

	"github.com/KinMiu/worker-record/config"
	"github.com/KinMiu/worker-record/pkg/dto"
	"github.com/KinMiu/worker-record/pkg/handler/message_broker"
)

// UploadTask represents a completed video chunk ready for asynchronous upload & event publishing.
type UploadTask struct {
	DeviceID   string
	DeviceName string
	MacAddress string
	FileName   string
	FilePath   string
	FileSize   int64
	Duration   int
	CreatedAt  time.Time
}

// UploadPoolService manages concurrent background workers uploading recorded segments to S3/MinIO
// and dispatching events to RabbitMQ.
type UploadPoolService struct {
	s3Uploader  *S3UploaderService
	broker      *message_broker.RabbitMQBroker
	queue       chan UploadTask
	workerCount int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	mu          sync.Mutex
}

// NewUploadPoolService creates a new asynchronous upload pool instance.
func NewUploadPoolService(s3Uploader *S3UploaderService, broker *message_broker.RabbitMQBroker, workers, queueSize int) *UploadPoolService {
	if workers <= 0 {
		workers = 4
	}
	if queueSize <= 0 {
		queueSize = 200
	}

	return &UploadPoolService{
		s3Uploader:  s3Uploader,
		broker:      broker,
		queue:       make(chan UploadTask, queueSize),
		workerCount: workers,
	}
}

// Start launches the background worker goroutines.
func (p *UploadPoolService) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)

	log.Infof("[UPLOAD-POOL] Starting async upload worker pool (%d workers, buffer size %d)...",
		p.workerCount, cap(p.queue))

	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

// Enqueue submits a completed recording chunk to the upload pipeline.
func (p *UploadPoolService) Enqueue(task UploadTask) bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		log.Warnf("[UPLOAD-POOL] Rejected task %s: pool is closed", task.FileName)
		return false
	}
	p.mu.Unlock()

	select {
	case p.queue <- task:
		log.Infof("[UPLOAD-POOL] Enqueued chunk %s (Device: %s, Size: %.2f MB, Duration: %ds)",
			task.FileName, task.DeviceName, float64(task.FileSize)/(1024*1024), task.Duration)
		return true
	default:
		// Queue full fallback: execute in detached goroutine
		log.Warnf("[UPLOAD-POOL] Queue buffer full (%d items)! Processing %s in overflow goroutine",
			cap(p.queue), task.FileName)
		go p.processTask(0, task)
		return true
	}
}

func (p *UploadPoolService) workerLoop(workerID int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			for task := range p.queue {
				p.processTask(workerID, task)
			}
			return
		case task, ok := <-p.queue:
			if !ok {
				return
			}
			p.processTask(workerID, task)
		}
	}
}

func (p *UploadPoolService) processTask(workerID int, task UploadTask) {
	normalizedPath := filepath.ToSlash(task.FilePath)
	createdAtISO := task.CreatedAt.UTC().Format(time.RFC3339Nano)

	cleanBaseURL := strings.TrimRight(config.RECORDING_BASE_URL.GetValueOrDefault("http://127.0.0.1:9000/recordings"), "/")
	fileURL := fmt.Sprintf("%s/%s/%s", cleanBaseURL, task.DeviceID, task.FileName)

	event := dto.RecordingCompletedEventDTO{
		Event:      "RECORDING_COMPLETED",
		DeviceID:   task.DeviceID,
		DeviceName: task.DeviceName,
		MacAddress: task.MacAddress,
		FileName:   task.FileName,
		Path:       normalizedPath,
		URL:        fileURL,
		Size:       task.FileSize,
		Duration:   task.Duration,
		CreatedAt:  createdAtISO,
	}

	// 1. Upload segment to S3 / MinIO Storage (if enabled)
	if config.ENABLE_S3_UPLOAD.GetValueBool(false) && p.s3Uploader != nil {
		s3Key := fmt.Sprintf("%s/%s", task.DeviceID, task.FileName)
		uploadCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			err = p.s3Uploader.UploadSegment(uploadCtx, task.FilePath, s3Key)
			if err == nil {
				break
			}
			log.Warnf("[UPLOAD-POOL][WORKER-%d] S3 upload attempt %d/3 for %s failed: %v",
				workerID, attempt, task.FileName, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			log.Errorf("[UPLOAD-POOL][WORKER-%d] S3 upload ultimately failed for %s: %v",
				workerID, task.FileName, err)
			return
		}
	}

	// 2. Publish event to RabbitMQ
	if p.broker != nil {
		pubCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			err = p.broker.PublishRecordingCompleted(pubCtx, event)
			if err == nil {
				break
			}
			log.Warnf("[UPLOAD-POOL][WORKER-%d] RabbitMQ publish attempt %d/3 for %s failed: %v",
				workerID, attempt, task.FileName, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			log.Errorf("[UPLOAD-POOL][WORKER-%d] RabbitMQ publish failed for %s: %v",
				workerID, task.FileName, err)
			return
		}
	}

	// 3. Local file cleanup
	if config.ENABLE_S3_UPLOAD.GetValueBool(false) && config.S3_DELETE_LOCAL_AFTER_UPLOAD.GetValueBool(false) {
		if _, err := os.Stat(task.FilePath); err == nil {
			_ = os.Remove(task.FilePath)
		}
	}

	log.Infof("[UPLOAD-POOL][WORKER-%d] Successfully processed chunk %s (Device: %s, Size: %.2f MB)",
		workerID, task.FileName, task.DeviceName, float64(task.FileSize)/(1024*1024))
}

// DrainAndStop gracefully waits for queued tasks to finish and terminates workers.
func (p *UploadPoolService) DrainAndStop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.queue)
	p.mu.Unlock()

	log.Info("[UPLOAD-POOL] Draining pending upload queue...")
	p.wg.Wait()
	log.Info("[UPLOAD-POOL] All upload workers stopped cleanly.")
}
