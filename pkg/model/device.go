package model

import "strings"

// User represents the associated user metadata from the backend schema.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Device represents the camera/CCTV device entity.
type Device struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	MacAddress       string   `json:"macAddress"`
	RTSPEndpoint     string   `json:"rtspEndpoint"`
	MediamtxEndpoint *string  `json:"mediamtxEndpoint,omitempty"`
	SourceURL        string   `json:"source_url,omitempty"`
	TargetURL        string   `json:"target_url,omitempty"`
	IsActive         *bool    `json:"is_active,omitempty"`
	Latitude         *float64 `json:"latitude,omitempty"`
	Longitude        *float64 `json:"longitude,omitempty"`
	CreatedAt        string   `json:"createdAt,omitempty"`
	UpdatedAt        string   `json:"updatedAt,omitempty"`
	UserID           string   `json:"userId,omitempty"`
	User             *User    `json:"user,omitempty"`
}

// GetEffectiveRTSPURL returns the RTSP streaming endpoint.
func (d *Device) GetEffectiveRTSPURL() string {
	if strings.TrimSpace(d.RTSPEndpoint) != "" {
		return strings.TrimSpace(d.RTSPEndpoint)
	}
	return strings.TrimSpace(d.SourceURL)
}

// IsDeviceActive checks if the device is active (defaults to true if omitted).
func (d *Device) IsDeviceActive() bool {
	if d.IsActive != nil {
		return *d.IsActive
	}
	return true
}

// HealthStatus represents the payload returned by the health check endpoint.
type HealthStatus struct {
	Status          string `json:"status"`
	WorkerID        string `json:"worker_id"`
	ActiveRecorders int    `json:"active_recorders"`
	RMQConnected    bool   `json:"rmq_connected"`
	S3UploadEnabled bool   `json:"s3_upload_enabled"`
	Uptime          string `json:"uptime"`
}

// HealthStatusResponse provides a concrete envelope for Swagger documentation.
type HealthStatusResponse struct {
	ResponseEntity[HealthStatus]
}
