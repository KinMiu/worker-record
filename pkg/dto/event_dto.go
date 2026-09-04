package dto

import (
	"github.com/KinMiu/worker-record/pkg/enum"
	"github.com/KinMiu/worker-record/pkg/model"
)

// MQTTEventPayloadDTO represents the real-time event message received over MQTT topics.
type MQTTEventPayloadDTO struct {
	Action   enum.MQTTEventAction `json:"action" validate:"required"`
	Camera   *model.Device        `json:"camera,omitempty"`
	CameraID string               `json:"camera_id,omitempty"`
}
