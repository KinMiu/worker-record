package watcher

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/models"
	"github.com/KinMiu/worker-record/internal/publisher"
	"github.com/KinMiu/worker-record/internal/recorder"
	"github.com/KinMiu/worker-record/internal/uploader"
)

// FileWatcher periodically scans recording directories to detect completed video segments
// and dispatches notification events to RabbitMQ and optional S3 storage.
type FileWatcher struct {
	cfg            *config.Config
	recorderMgr    *recorder.RecorderManager
	rmqPublisher   *publisher.RabbitMQPublisher
	s3Uploader     *uploader.S3Uploader
	mu             sync.RWMutex
	processedFiles map[string]bool
	fileModTracker map[string]fileStatInfo
}

type fileStatInfo struct {
	size    int64
	modTime time.Time
	seenAt  time.Time
}

// NewFileWatcher creates a new FileWatcher instance.
func NewFileWatcher(
	cfg *config.Config,
	recorderMgr *recorder.RecorderManager,
	rmqPublisher *publisher.RabbitMQPublisher,
	s3Uploader *uploader.S3Uploader,
) *FileWatcher {
	return &FileWatcher{
		cfg:            cfg,
		recorderMgr:    recorderMgr,
		rmqPublisher:   rmqPublisher,
		s3Uploader:     s3Uploader,
		processedFiles: make(map[string]bool),
		fileModTracker: make(map[string]fileStatInfo),
	}
}

// Start begins the periodic scanner loop until context is canceled.
func (w *FileWatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.cfg.ScanIntervalSeconds) * time.Second)
	defer ticker.Stop()

	log.Printf("[WATCHER] Directory scanner started (Interval: %ds, Storage: %s)",
		w.cfg.ScanIntervalSeconds, w.cfg.RecordStoragePath)

	for {
		select {
		case <-ctx.Done():
			log.Println("[WATCHER] Directory scanner stopped.")
			return
		case <-ticker.C:
			w.scanStorage(ctx)
		}
	}
}

func (w *FileWatcher) scanStorage(ctx context.Context) {
	deviceMap := w.recorderMgr.GetDeviceMap()

	// Read storage directory
	entries, err := os.ReadDir(w.cfg.RecordStoragePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[WATCHER][WARNING] Failed to read storage root directory %s: %v", w.cfg.RecordStoragePath, err)
		}
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		deviceID := entry.Name()
		deviceDir := filepath.Join(w.cfg.RecordStoragePath, deviceID)

		dev, hasDev := deviceMap[deviceID]
		deviceName := deviceID
		macAddress := ""
		if hasDev {
			if dev.Name != "" {
				deviceName = dev.Name
			}
			macAddress = dev.MacAddress
		}

		w.scanDeviceDirectory(ctx, deviceID, deviceName, macAddress, deviceDir)
	}
}

func (w *FileWatcher) scanDeviceDirectory(ctx context.Context, deviceID, deviceName, macAddress, deviceDir string) {
	entries, err := os.ReadDir(deviceDir)
	if err != nil {
		return
	}

	type mp4File struct {
		name    string
		path    string
		size    int64
		modTime time.Time
	}

	var mp4List []mp4File

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".mp4") {
			continue
		}

		filePath := filepath.Join(deviceDir, name)

		// Check if already processed
		w.mu.RLock()
		alreadyProcessed := w.processedFiles[filePath]
		w.mu.RUnlock()

		if alreadyProcessed {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Discard 0-byte or corrupt mini-files
		if info.Size() <= 1024 {
			continue
		}

		mp4List = append(mp4List, mp4File{
			name:    name,
			path:    filePath,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}

	if len(mp4List) == 0 {
		return
	}

	// Sort files by filename / timestamp ascending
	sort.Slice(mp4List, func(i, j int) bool {
		return mp4List[i].name < mp4List[j].name
	})

	now := time.Now()

	for i, f := range mp4List {
		isCompleted := false

		// Rule 1: A segment is completed if a newer MP4 file exists in the same folder
		if i < len(mp4List)-1 {
			isCompleted = true
		} else {
			// Rule 2: If it is the latest file, check if size and modification time have remained stable
			w.mu.Lock()
			stat, tracked := w.fileModTracker[f.path]
			if !tracked {
				w.fileModTracker[f.path] = fileStatInfo{
					size:    f.size,
					modTime: f.modTime,
					seenAt:  now,
				}
			} else {
				// If size has not changed for at least (ScanInterval + 5) seconds
				stagnantDuration := now.Sub(stat.seenAt)
				if stat.size == f.size && stagnantDuration >= time.Duration(w.cfg.ScanIntervalSeconds+5)*time.Second {
					isCompleted = true
					delete(w.fileModTracker, f.path)
				} else if stat.size != f.size {
					// File is still being actively written, update size and timestamp
					w.fileModTracker[f.path] = fileStatInfo{
						size:    f.size,
						modTime: f.modTime,
						seenAt:  now,
					}
				}
			}
			w.mu.Unlock()
		}

		if isCompleted {
			w.handleCompletedSegment(ctx, deviceID, deviceName, macAddress, f.name, f.path, f.size, f.modTime)
		}
	}
}

func (w *FileWatcher) handleCompletedSegment(
	ctx context.Context,
	deviceID, deviceName, macAddress, fileName, filePath string,
	size int64,
	modTime time.Time,
) {
	// Parse timestamp from filename (format: 2006-01-02_15-04-05.mp4)
	baseName := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	parsedTime, err := time.ParseInLocation("2006-01-02_15-04-05", baseName, time.Local)
	if err != nil {
		parsedTime = modTime
	}

	createdAtISO := parsedTime.UTC().Format(time.RFC3339Nano)

	// Standardize file path with forward slashes for cross-platform compatibility
	normalizedPath := filepath.ToSlash(filePath)

	// Construct public/storage playback URL
	cleanBaseURL := strings.TrimRight(w.cfg.RecordingBaseURL, "/")
	fileURL := fmt.Sprintf("%s/%s/%s", cleanBaseURL, deviceID, fileName)

	event := models.RecordingCompletedEvent{
		Event:      "RECORDING_COMPLETED",
		DeviceID:   deviceID,
		DeviceName: deviceName,
		MacAddress: macAddress,
		FileName:   fileName,
		Path:       normalizedPath,
		URL:        fileURL,
		Size:       size,
		Duration:   w.cfg.SegmentDurationSeconds,
		CreatedAt:  createdAtISO,
	}

	// 1. Upload segment to S3 / MinIO Storage (if enabled)
	if w.cfg.EnableS3Upload {
		s3Key := fmt.Sprintf("%s/%s", deviceID, fileName)
		if err := w.s3Uploader.UploadSegment(ctx, filePath, s3Key); err != nil {
			log.Printf("[WATCHER][ERROR] Failed to upload %s to S3/MinIO: %v. Will retry on next scan.", fileName, err)
			return
		}
	}

	// 2. Publish event to RabbitMQ
	if err := w.rmqPublisher.PublishRecordingCompleted(ctx, event); err != nil {
		log.Printf("[WATCHER][ERROR] Failed to publish recording event for %s: %v. Will retry on next scan.", fileName, err)
		return
	}

	// Mark as processed
	w.mu.Lock()
	w.processedFiles[filePath] = true
	delete(w.fileModTracker, filePath)
	w.mu.Unlock()

	log.Printf("[WATCHER] Segment verified & published: %s (Device: %s, Size: %.2f MB)",
		fileName, deviceName, float64(size)/(1024*1024))
}
