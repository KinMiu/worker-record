package uploader

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/models"
	"github.com/KinMiu/worker-record/internal/publisher"
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

// UploadPool manages concurrent background workers uploading recorded segments to S3/MinIO
// and dispatching events to RabbitMQ.
type UploadPool struct {
	cfg         *config.Config
	s3Uploader  *S3Uploader
	publisher   *publisher.RabbitMQPublisher
	queue       chan UploadTask
	workerCount int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	mu          sync.Mutex
}

// NewUploadPool creates a new asynchronous upload pool.
func NewUploadPool(cfg *config.Config, s3Uploader *S3Uploader, pub *publisher.RabbitMQPublisher, workers, queueSize int) *UploadPool {
	if workers <= 0 {
		workers = 4
	}
	if queueSize <= 0 {
		queueSize = 100
	}

	return &UploadPool{
		cfg:         cfg,
		s3Uploader:  s3Uploader,
		publisher:   pub,
		queue:       make(chan UploadTask, queueSize),
		workerCount: workers,
	}
}

// Start launches the background worker goroutines.
func (p *UploadPool) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)

	log.Printf("[UPLOAD-POOL] Starting async upload worker pool (%d workers, buffer size %d)...",
		p.workerCount, cap(p.queue))

	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

// Enqueue submits a completed recording chunk to the upload pipeline.
func (p *UploadPool) Enqueue(task UploadTask) bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		log.Printf("[UPLOAD-POOL][WARNING] Rejected task %s: pool is closed", task.FileName)
		return false
	}
	p.mu.Unlock()

	select {
	case p.queue <- task:
		log.Printf("[UPLOAD-POOL] Enqueued chunk %s (Device: %s, Size: %.2f MB, Duration: %ds)",
			task.FileName, task.DeviceName, float64(task.FileSize)/(1024*1024), task.Duration)
		return true
	default:
		// Queue full fallback: execute in a detached goroutine to avoid dropping recordings
		log.Printf("[UPLOAD-POOL][WARNING] Queue buffer full (%d items)! Processing %s in overflow goroutine",
			cap(p.queue), task.FileName)
		go p.processTask(0, task)
		return true
	}
}

func (p *UploadPool) workerLoop(workerID int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining tasks in queue before exiting
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

func (p *UploadPool) processTask(workerID int, task UploadTask) {
	normalizedPath := filepath.ToSlash(task.FilePath)
	createdAtISO := task.CreatedAt.UTC().Format(time.RFC3339Nano)

	cleanBaseURL := strings.TrimRight(p.cfg.RecordingBaseURL, "/")
	fileURL := fmt.Sprintf("%s/%s/%s", cleanBaseURL, task.DeviceID, task.FileName)

	event := models.RecordingCompletedEvent{
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
	if p.cfg.EnableS3Upload && p.s3Uploader != nil {
		s3Key := fmt.Sprintf("%s/%s", task.DeviceID, task.FileName)
		uploadCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			err = p.s3Uploader.UploadSegment(uploadCtx, task.FilePath, s3Key)
			if err == nil {
				break
			}
			log.Printf("[UPLOAD-POOL][WORKER-%d][WARNING] S3 upload attempt %d/3 for %s failed: %v",
				workerID, attempt, task.FileName, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			log.Printf("[UPLOAD-POOL][WORKER-%d][ERROR] S3 upload ultimately failed for %s: %v",
				workerID, task.FileName, err)
			return
		}
	}

	// 2. Publish event to RabbitMQ
	if p.publisher != nil {
		pubCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			err = p.publisher.PublishRecordingCompleted(pubCtx, event)
			if err == nil {
				break
			}
			log.Printf("[UPLOAD-POOL][WORKER-%d][WARNING] RabbitMQ publish attempt %d/3 for %s failed: %v",
				workerID, attempt, task.FileName, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			log.Printf("[UPLOAD-POOL][WORKER-%d][ERROR] RabbitMQ publish failed for %s: %v",
				workerID, task.FileName, err)
			return
		}
	}

	// 3. Local file cleanup if S3 upload is disabled or S3 did not delete it
	if !p.cfg.EnableS3Upload {
		// If local-only mode, keep or manage disk quotas
	} else if p.cfg.S3DeleteLocal {
		if _, err := os.Stat(task.FilePath); err == nil {
			_ = os.Remove(task.FilePath)
		}
	}

	log.Printf("[UPLOAD-POOL][WORKER-%d] Successfully processed chunk %s (Device: %s, Size: %.2f MB)",
		workerID, task.FileName, task.DeviceName, float64(task.FileSize)/(1024*1024))
}

// DrainAndStop gracefully waits for all queued upload tasks to finish and terminates workers.
func (p *UploadPool) DrainAndStop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.queue)
	p.mu.Unlock()

	log.Println("[UPLOAD-POOL] Draining pending upload queue...")
	p.wg.Wait()
	log.Println("[UPLOAD-POOL] All upload workers stopped cleanly.")
}
