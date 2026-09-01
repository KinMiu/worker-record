package models

import "strings"

// User represents the associated user data from the backend schema.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Device represents the camera / CCTV device model from the central backend.
type Device struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	MacAddress       string   `json:"macAddress"`
	RTSPEndpoint     string   `json:"rtspEndpoint"`
	MediamtxEndpoint *string  `json:"mediamtxEndpoint,omitempty"`
	SourceURL        string   `json:"source_url,omitempty"`
	IsActive         *bool    `json:"is_active,omitempty"`
	Latitude         *float64 `json:"latitude,omitempty"`
	Longitude        *float64 `json:"longitude,omitempty"`
	CreatedAt        string   `json:"createdAt,omitempty"`
	UpdatedAt        string   `json:"updatedAt,omitempty"`
	UserID           string   `json:"userId,omitempty"`
	User             *User    `json:"user,omitempty"`
}

// GetEffectiveRTSPURL returns the RTSP streaming endpoint.
// Prioritizes rtspEndpoint, falling back to source_url.
func (d *Device) GetEffectiveRTSPURL() string {
	if strings.TrimSpace(d.RTSPEndpoint) != "" {
		return strings.TrimSpace(d.RTSPEndpoint)
	}
	return strings.TrimSpace(d.SourceURL)
}

// IsDeviceActive checks if the device is active.
// Defaults to true if isActive is not explicitly provided.
func (d *Device) IsDeviceActive() bool {
	if d.IsActive != nil {
		return *d.IsActive
	}
	return true
}

// APIResponse represents the standard REST API envelope from the backend.
type APIResponse struct {
	Status  string   `json:"status,omitempty"`
	Message string   `json:"message,omitempty"`
	Data    []Device `json:"data"`
}

// RecordingCompletedEvent represents the RabbitMQ message payload published
// when a 5-minute segmented MP4 file completes recording.
// Schema aligns 1:1 with Prisma backend Recording model.
type RecordingCompletedEvent struct {
	Event      string `json:"event"`
	DeviceID   string `json:"deviceId"`
	DeviceName string `json:"deviceName"`
	MacAddress string `json:"macAddress"`
	FileName   string `json:"fileName"`
	Path       string `json:"path"`
	URL        string `json:"url"`
	Size       int64  `json:"size"`
	Duration   int    `json:"duration"`
	CreatedAt  string `json:"createdAt"`
}
