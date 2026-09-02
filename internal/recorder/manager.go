package recorder

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/models"
)

type deviceRunner struct {
	device models.Device
	cancel context.CancelFunc
}

// RecorderManager coordinates concurrent FFmpeg recording processes for active cameras.
type RecorderManager struct {
	cfg        *config.Config
	mu         sync.RWMutex
	runners    map[string]*deviceRunner
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewRecorderManager initializes a new RecorderManager.
func NewRecorderManager(cfg *config.Config) *RecorderManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RecorderManager{
		cfg:        cfg,
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
			log.Printf("[RECORDER] Device %s (%s) is now inactive/missing URL. Stopping recorder...", dev.ID, dev.Name)
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

// runDeviceLoop executes the segmented FFmpeg stream copy with an automatic reconnect loop.
func (rm *RecorderManager) runDeviceLoop(ctx context.Context, dev models.Device) {
	defer rm.wg.Done()

	rtspURL := dev.GetEffectiveRTSPURL()
	deviceStorageDir := filepath.Join(rm.cfg.RecordStoragePath, dev.ID)
	outputPattern := filepath.Join(deviceStorageDir, "%Y-%m-%d_%H-%M-%S.mp4")

	log.Printf("[RECORDER][%s] Recorder started for '%s' -> Saving segments to: %s",
		dev.ID, dev.Name, outputPattern)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[RECORDER][%s] Recorder stopped by context cancellation", dev.ID)
			return
		default:
		}

		startTime := time.Now()
		log.Printf("[RECORDER][%s] Launching FFmpeg segmented recorder (Segment: %ds, Source: %s)...",
			dev.ID, rm.cfg.SegmentDurationSeconds, rtspURL)

		// FFmpeg pass-through segmented MP4 (-c copy, 0% CPU re-encoding overhead)
		// ffmpeg -loglevel warning -rtsp_transport tcp -stimeout 5000000 -fflags +genpts+nobuffer -avoid_negative_ts make_zero
		//        -i <rtspEndpoint> -c copy -f segment -segment_time 300 -segment_format mp4 -reset_timestamps 1 -strftime 1 <output_pattern>
		args := []string{
			"-hide_banner",
			"-loglevel", "warning",
			"-rtsp_transport", "tcp",
			"-timeout", "5000000", // 5 seconds socket timeout in microseconds (FFmpeg 5+/6+/7+)
			"-fflags", "+genpts+nobuffer",
			"-avoid_negative_ts", "make_zero",
			"-i", rtspURL,
			"-c", "copy",
			"-f", "segment",
			"-segment_time", fmt.Sprintf("%d", rm.cfg.SegmentDurationSeconds),
			"-segment_format", "mp4",
			"-reset_timestamps", "1",
			"-strftime", "1",
			outputPattern,
		}

		cmd := exec.CommandContext(ctx, rm.cfg.FFmpegPath, args...)

		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf

		err := cmd.Run()
		duration := time.Since(startTime)

		if ctx.Err() != nil {
			log.Printf("[RECORDER][%s] FFmpeg process terminated cleanly (context canceled)", dev.ID)
			return
		}

		if err != nil {
			log.Printf("[RECORDER][%s] FFmpeg exited with error after %v: %v | Stderr: %s",
				dev.ID, duration.Round(time.Second), err, stderrBuf.String())
		} else {
			log.Printf("[RECORDER][%s] FFmpeg process finished after %v", dev.ID, duration.Round(time.Second))
		}

		// Reconnect backoff
		retryWait := time.Duration(rm.cfg.RetryIntervalSeconds) * time.Second
		log.Printf("[RECORDER][%s] Reconnecting camera in %v...", dev.ID, retryWait)

		select {
		case <-ctx.Done():
			log.Printf("[RECORDER][%s] Recorder terminated during reconnect backoff", dev.ID)
			return
		case <-time.After(retryWait):
		}
	}
}
