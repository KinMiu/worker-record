package recorder

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/models"
	"github.com/KinMiu/worker-record/internal/uploader"
)

type deviceRunner struct {
	device models.Device
	cancel context.CancelFunc
}

// RecorderManager coordinates concurrent Direct Chunk FFmpeg recording processes for active cameras.
type RecorderManager struct {
	cfg        *config.Config
	uploadPool *uploader.UploadPool
	mu         sync.RWMutex
	runners    map[string]*deviceRunner
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewRecorderManager initializes a new RecorderManager wired to the async upload pool.
func NewRecorderManager(cfg *config.Config, uploadPool *uploader.UploadPool) *RecorderManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RecorderManager{
		cfg:        cfg,
		uploadPool: uploadPool,
		runners:    make(map[string]*deviceRunner),
		rootCtx:    ctx,
		rootCancel: cancel,
	}
}

// UpsertDevice starts or restarts a recording runner for the specified device.
func (rm *RecorderManager) UpsertDevice(dev models.Device) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rtspURL := dev.GetEffectiveRTSPURL()
	isActive := dev.IsDeviceActive()

	existing, exists := rm.runners[dev.ID]

	// If device is inactive or RTSP URL is missing, stop runner if active
	if !isActive || rtspURL == "" {
		if exists {
			log.Printf("[RECORDER] Device %s (%s) is inactive/missing URL. Stopping recorder...", dev.ID, dev.Name)
			existing.cancel()
			delete(rm.runners, dev.ID)
		}
		return
	}

	// If already running with exact same parameters, update metadata only
	if exists {
		if existing.device.GetEffectiveRTSPURL() == rtspURL && existing.device.IsDeviceActive() == isActive {
			existing.device.Name = dev.Name
			existing.device.MacAddress = dev.MacAddress
			return
		}

		log.Printf("[RECORDER] Device %s (%s) configuration changed. Restarting recorder...", dev.ID, dev.Name)
		existing.cancel()
		delete(rm.runners, dev.ID)
	}

	// Ensure destination directory exists
	deviceStorageDir := filepath.Join(rm.cfg.RecordStoragePath, dev.ID)
	if err := os.MkdirAll(deviceStorageDir, 0755); err != nil {
		log.Printf("[RECORDER][ERROR] Failed to create storage directory %s: %v", deviceStorageDir, err)
		return
	}

	// Spawn new recording goroutine
	devCtx, devCancel := context.WithCancel(rm.rootCtx)
	rm.runners[dev.ID] = &deviceRunner{
		device: dev,
		cancel: devCancel,
	}

	rm.wg.Add(1)
	go rm.runDeviceLoop(devCtx, dev)
}

// RemoveDevice halts and cleans up a device's recording process.
func (rm *RecorderManager) RemoveDevice(deviceID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if runner, exists := rm.runners[deviceID]; exists {
		log.Printf("[RECORDER] Removing recorder for device %s (%s)...", deviceID, runner.device.Name)
		runner.cancel()
		delete(rm.runners, deviceID)
	}
}

// ReconcileDevices reconciles running recording sessions with the latest device list.
func (rm *RecorderManager) ReconcileDevices(devices []models.Device) {
	newMap := make(map[string]models.Device)
	for _, dev := range devices {
		newMap[dev.ID] = dev
	}

	rm.mu.RLock()
	var toRemove []string
	for id := range rm.runners {
		if _, exists := newMap[id]; !exists {
			toRemove = append(toRemove, id)
		}
	}
	rm.mu.RUnlock()

	for _, id := range toRemove {
		rm.RemoveDevice(id)
	}

	for _, dev := range devices {
		rm.UpsertDevice(dev)
	}

	log.Printf("[RECORDER] Reconciled devices. Currently recording %d active camera(s)", rm.ActiveRecorderCount())
}

// ActiveRecorderCount returns the number of currently active recording runners.
func (rm *RecorderManager) ActiveRecorderCount() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.runners)
}

// IsDeviceRecording returns whether a device currently has an active recording runner.
func (rm *RecorderManager) IsDeviceRecording(deviceID string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	_, exists := rm.runners[deviceID]
	return exists
}

// GetDeviceMap returns a snapshot copy of all currently registered devices.
func (rm *RecorderManager) GetDeviceMap() map[string]models.Device {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	copyMap := make(map[string]models.Device, len(rm.runners))
	for id, runner := range rm.runners {
		copyMap[id] = runner.device
	}
	return copyMap
}

// StopAll stops all active recorders and waits for child processes to exit.
func (rm *RecorderManager) StopAll() {
	log.Println("[RECORDER] Stopping all camera recorders...")
	rm.rootCancel()

	rm.mu.Lock()
	for id, runner := range rm.runners {
		runner.cancel()
		delete(rm.runners, id)
	}
	rm.mu.Unlock()

	rm.wg.Wait()
	log.Println("[RECORDER] All camera recorders stopped cleanly.")
}

// runDeviceLoop manages sequential discrete 5-minute chunk recording with zero gap and auto-reconnect.
func (rm *RecorderManager) runDeviceLoop(ctx context.Context, dev models.Device) {
	defer rm.wg.Done()

	rtspURL := dev.GetEffectiveRTSPURL()
	deviceStorageDir := filepath.Join(rm.cfg.RecordStoragePath, dev.ID)

	log.Printf("[RECORDER][%s] Direct Chunk Recorder initialized for '%s' (Chunk: %ds, Source: %s)",
		dev.ID, dev.Name, rm.cfg.SegmentDurationSeconds, rtspURL)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[RECORDER][%s] Recorder loop stopped for '%s'", dev.ID, dev.Name)
			return
		default:
		}

		nowUTC := time.Now().UTC()
		fileName := fmt.Sprintf("%s.mp4", nowUTC.Format("2006-01-02_15-04-05"))
		filePath := filepath.Join(deviceStorageDir, fileName)

		log.Printf("[RECORDER][%s] Recording chunk '%s' for '%s' (%ds target)...",
			dev.ID, fileName, dev.Name, rm.cfg.SegmentDurationSeconds)

		// Discrete FFmpeg process for target duration (-t N) with stream-copy, extradata extraction and faststart moov atom
		args := []string{
			"-y",
			"-hide_banner",
			"-loglevel", "warning",
			"-rtsp_transport", "tcp",
			"-timeout", "15000000", // 15 seconds socket timeout in microseconds
			"-buffer_size", "1024000", // 1MB buffer
			"-fflags", "+genpts+discardcorrupt",
			"-i", rtspURL,
			"-t", fmt.Sprintf("%d", rm.cfg.SegmentDurationSeconds),
			"-map", "0:v:0",
			"-map", "0:a?",
			"-c:v", "copy",
			"-c:a", "copy",
			"-bsf:v", "dump_extra",
			"-tag:v", "avc1",
			"-avoid_negative_ts", "make_zero",
			"-movflags", "+faststart",
			filePath,
		}

		cmd := exec.CommandContext(ctx, rm.cfg.FFmpegPath, args...)
		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf

		startTime := time.Now()
		err := cmd.Run()
		recordedDuration := time.Since(startTime)
		recordedSec := int(recordedDuration.Seconds())
		if recordedSec < 1 {
			recordedSec = 1
		}

		// Minimum threshold to consider a recording valid (ignore <15s and <512KB handshake glitches)
		minValidDuration := 15 * time.Second
		minValidSize := int64(512 * 1024) // 512KB minimum

		fileInfo, statErr := os.Stat(filePath)
		if statErr == nil && fileInfo.Size() >= minValidSize && recordedDuration >= minValidDuration {
			// Chunk is valid and finalized with moov atom: enqueue to upload pool
			task := uploader.UploadTask{
				DeviceID:   dev.ID,
				DeviceName: dev.Name,
				MacAddress: dev.MacAddress,
				FileName:   fileName,
				FilePath:   filePath,
				FileSize:   fileInfo.Size(),
				Duration:   recordedSec,
				CreatedAt:  nowUTC,
			}

			if rm.uploadPool != nil {
				rm.uploadPool.Enqueue(task)
			}
		} else {
			// Clean up stub if empty or below threshold
			if statErr == nil {
				_ = os.Remove(filePath)
				if fileInfo.Size() > 0 {
					log.Printf("[RECORDER][%s] Discarded micro-fragment '%s' (Size: %.2f KB, Duration: %v)",
						dev.ID, fileName, float64(fileInfo.Size())/1024.0, recordedDuration.Round(time.Second))
				}
			}
		}

		// Handle shutdown
		if ctx.Err() != nil {
			log.Printf("[RECORDER][%s] Recorder context cancelled during '%s'", dev.ID, fileName)
			return
		}

		// Determine next step: immediate rollover vs reconnect backoff
		if err != nil || recordedDuration < 30*time.Second {
			stderrMsg := strings.TrimSpace(stderrBuf.String())
			if stderrMsg != "" {
				log.Printf("[RECORDER][%s][WARNING] FFmpeg error on '%s': %s", dev.ID, dev.Name, stderrMsg)
			}

			retryWait := time.Duration(rm.cfg.RetryIntervalSeconds) * time.Second
			log.Printf("[RECORDER][%s] Camera '%s' disconnected/errored after %v. Reconnecting in %v...",
				dev.ID, dev.Name, recordedDuration.Round(time.Second), retryWait)

			select {
			case <-ctx.Done():
				return
			case <-time.After(retryWait):
			}
		} else {
			// Full chunk recorded cleanly: brief 500ms socket cooldown before dialing next chunk
			log.Printf("[RECORDER][%s] Chunk '%s' completed successfully (%v). Rollover to next chunk...",
				dev.ID, fileName, recordedDuration.Round(time.Second))
			time.Sleep(500 * time.Millisecond)
		}
	}
}
