package consumer

import (
	"encoding/json"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gofiber/fiber/v2/log"

	"github.com/KinMiu/worker-record/pkg/dto"
	"github.com/KinMiu/worker-record/pkg/enum"
	"github.com/KinMiu/worker-record/pkg/service"
)

// CameraConsumer handles real-time MQTT synchronization events dispatched by the backend.
type CameraConsumer struct {
	deviceClient    *service.DeviceClientService
	recorderManager *service.RecorderManagerService
}

// NewCameraConsumer creates an instance of CameraConsumer with injected dependencies.
func NewCameraConsumer(
	deviceClient *service.DeviceClientService,
	recorderManager *service.RecorderManagerService,
) *CameraConsumer {
	return &CameraConsumer{
		deviceClient:    deviceClient,
		recorderManager: recorderManager,
	}
}

// HandleEventMessage unmarshals incoming MQTT event payloads and triggers corresponding recorder manager actions.
func (c *CameraConsumer) HandleEventMessage(_ mqtt.Client, msg mqtt.Message) {
	log.Infof("[CONSUMER][MQTT] Event received on topic [%s]: %s", msg.Topic(), string(msg.Payload()))

	var event dto.MQTTEventPayloadDTO
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		log.Errorf("[CONSUMER][MQTT] Failed to decode event JSON: %v", err)
		return
	}

	switch event.Action {
	case enum.ActionSyncAll:
		log.Info("[CONSUMER][MQTT] Action SYNC_ALL -> Re-fetching camera list from backend API...")
		go c.handleSyncAll()

	case enum.ActionUpsertCamera:
		if event.Camera == nil {
			log.Warn("[CONSUMER][MQTT] Action UPSERT_CAMERA received without camera payload")
			return
		}
		log.Infof("[CONSUMER][MQTT] Action UPSERT_CAMERA -> Upserting camera %s (%s)", event.Camera.ID, event.Camera.Name)
		c.recorderManager.UpsertDevice(*event.Camera)

	case enum.ActionRemoveCamera:
		if event.CameraID == "" {
			log.Warn("[CONSUMER][MQTT] Action REMOVE_CAMERA received without camera_id")
			return
		}
		log.Infof("[CONSUMER][MQTT] Action REMOVE_CAMERA -> Removing camera %s", event.CameraID)
		c.recorderManager.RemoveDevice(event.CameraID)

	default:
		log.Warnf("[CONSUMER][MQTT] Unknown action '%s' received, ignoring", event.Action)
	}
}

func (c *CameraConsumer) handleSyncAll() {
	devices, err := c.deviceClient.FetchDevices()
	if err != nil {
		log.Errorf("[CONSUMER][MQTT] SYNC_ALL device fetch failed: %v", err)
		return
	}
	c.recorderManager.ReconcileDevices(devices)
}
