package service

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2/log"

	"github.com/KinMiu/worker-record/config"
	"github.com/KinMiu/worker-record/pkg/model"
)

type deviceRunner struct {
	device model.Device
	cancel context.CancelFunc
}

// RecorderManagerService coordinates concurrent Direct Chunk FFmpeg recording processes for active cameras.
type RecorderManagerService struct {
	uploadPool *UploadPoolService
	mu         sync.RWMutex
	runners    map[string]*deviceRunner
	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewRecorderManagerService initializes a new RecorderManagerService wired to the async upload pool.
func NewRecorderManagerService(uploadPool *UploadPoolService) *RecorderManagerService {
	ctx, cancel := context.WithCancel(context.Background())
	return &RecorderManagerService{
		uploadPool: uploadPool,
		runners:    make(map[string]*deviceRunner),
		rootCtx:    ctx,
		rootCancel: cancel,
	}
}

// UpsertDevice starts or restarts a recording runner for the specified device.
func (rm *RecorderManagerService) UpsertDevice(dev model.Device) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rtspURL := dev.GetEffectiveRTSPURL()
	isActive := dev.IsDeviceActive()

	existing, exists := rm.runners[dev.ID]

	// If device is inactive or RTSP URL is missing, stop runner if active
	if !isActive || rtspURL == "" {
		if exists {
			log.Infof("[RECORDER] Device %s (%s) is inactive/missing URL. Stopping recorder...", dev.ID, dev.Name)
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

		log.Infof("[RECORDER] Device %s (%s) configuration changed. Restarting recorder...", dev.ID, dev.Name)
		existing.cancel()
		delete(rm.runners, dev.ID)
	}

	recordStoragePath := config.RECORD_STORAGE_PATH.GetValueOrDefault("/opt/recordings/queue")
	deviceStorageDir := filepath.Join(recordStoragePath, dev.ID)
	if err := os.MkdirAll(deviceStorageDir, 0755); err != nil {
		log.Errorf("[RECORDER] Failed to create storage directory %s: %v", deviceStorageDir, err)
		return
	}

	devCtx, devCancel := context.WithCancel(rm.rootCtx)
	rm.runners[dev.ID] = &deviceRunner{
		device: dev,
		cancel: devCancel,
	}

	rm.wg.Add(1)
	go rm.runDeviceLoop(devCtx, dev)
}

// RemoveDevice halts and cleans up a device's recording process.
func (rm *RecorderManagerService) RemoveDevice(deviceID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if runner, exists := rm.runners[deviceID]; exists {
		log.Infof("[RECORDER] Removing recorder for device %s (%s)...", deviceID, runner.device.Name)
		runner.cancel()
		delete(rm.runners, deviceID)
	}
}

// ReconcileDevices reconciles running recording sessions with the latest device list.
func (rm *RecorderManagerService) ReconcileDevices(devices []model.Device) {
	newMap := make(map[string]model.Device)
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

	log.Infof("[RECORDER] Reconciled devices. Currently recording %d active camera(s)", rm.ActiveRecorderCount())
}

// ActiveRecorderCount returns the number of currently active recording runners.
func (rm *RecorderManagerService) ActiveRecorderCount() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.runners)
}

// StopAll stops all active recorders and waits for child processes to exit.
func (rm *RecorderManagerService) StopAll() {
	log.Info("[RECORDER] Stopping all camera recorders...")
	rm.rootCancel()

	rm.mu.Lock()
	for id, runner := range rm.runners {
		runner.cancel()
		delete(rm.runners, id)
	}
	rm.mu.Unlock()

	rm.wg.Wait()
	log.Info("[RECORDER] All camera recorders stopped cleanly.")
}

func (rm *RecorderManagerService) runDeviceLoop(ctx context.Context, dev model.Device) {
	defer rm.wg.Done()

	rtspURL := dev.GetEffectiveRTSPURL()
	recordStoragePath := config.RECORD_STORAGE_PATH.GetValueOrDefault("/opt/recordings/queue")
	deviceStorageDir := filepath.Join(recordStoragePath, dev.ID)
	segmentDuration := config.SEGMENT_DURATION_SECONDS.GetValueInt(300)
	ffmpegPath := config.FFMPEG_PATH.GetValueOrDefault("ffmpeg")
	retrySecs := config.RETRY_INTERVAL_SECONDS.GetValueInt(5)

	log.Infof("[RECORDER][%s] Direct Chunk Recorder initialized for '%s' (Chunk: %ds, Source: %s)",
		dev.ID, dev.Name, segmentDuration, rtspURL)

	for {
		select {
		case <-ctx.Done():
			log.Infof("[RECORDER][%s] Recorder loop stopped for '%s'", dev.ID, dev.Name)
			return
		default:
		}

		nowUTC := time.Now().UTC()
		fileName := fmt.Sprintf("%s.mp4", nowUTC.Format("2006-01-02_15-04-05"))
		filePath := filepath.Join(deviceStorageDir, fileName)

		log.Infof("[RECORDER][%s] Recording chunk '%s' for '%s' (%ds target)...",
			dev.ID, fileName, dev.Name, segmentDuration)

		args := []string{
			"-y",
			"-hide_banner",
			"-loglevel", "warning",
			"-rtsp_transport", "tcp",
			"-timeout", "15000000",
			"-buffer_size", "1024000",
			"-fflags", "+genpts+discardcorrupt",
			"-i", rtspURL,
			"-t", fmt.Sprintf("%d", segmentDuration),
			"-map", "0:v:0",
			"-map", "0:a?",
			"-c:v", "copy",
			"-c:a", "aac",
			"-b:a", "64k",
			"-bsf:v", "dump_extra",
			"-tag:v", "avc1",
			"-avoid_negative_ts", "make_zero",
			"-movflags", "+frag_keyframe+empty_moov+default_base_moof",
			filePath,
		}

		cmd := exec.CommandContext(ctx, ffmpegPath, args...)
		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf

		startTime := time.Now()
		err := cmd.Run()
		recordedDuration := time.Since(startTime)
		recordedSec := int(recordedDuration.Seconds())
		if recordedSec < 1 {
			recordedSec = 1
		}

		minValidDuration := 15 * time.Second
		minValidSize := int64(512 * 1024) // 512KB minimum

		fileInfo, statErr := os.Stat(filePath)
		if statErr == nil && fileInfo.Size() >= minValidSize && recordedDuration >= minValidDuration {
			task := UploadTask{
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
			if statErr == nil {
				_ = os.Remove(filePath)
				if fileInfo.Size() > 0 {
					log.Infof("[RECORDER][%s] Discarded micro-fragment '%s' (Size: %.2f KB, Duration: %v)",
						dev.ID, fileName, float64(fileInfo.Size())/1024.0, recordedDuration.Round(time.Second))
				}
			}
		}

		if ctx.Err() != nil {
			log.Infof("[RECORDER][%s] Recorder context cancelled during '%s'", dev.ID, fileName)
			return
		}

		if err != nil || recordedDuration < 30*time.Second {
			stderrMsg := strings.TrimSpace(stderrBuf.String())
			if stderrMsg != "" {
				log.Warnf("[RECORDER][%s] FFmpeg error on '%s': %s", dev.ID, dev.Name, stderrMsg)
			}

			retryWait := time.Duration(retrySecs) * time.Second
			log.Infof("[RECORDER][%s] Camera '%s' disconnected/errored after %v. Reconnecting in %v...",
				dev.ID, dev.Name, recordedDuration.Round(time.Second), retryWait)

			select {
			case <-ctx.Done():
				return
			case <-time.After(retryWait):
			}
		} else {
			log.Infof("[RECORDER][%s] Chunk '%s' completed successfully (%v). Rollover to next chunk...",
				dev.ID, fileName, recordedDuration.Round(time.Second))
			time.Sleep(500 * time.Millisecond)
		}
	}
}
