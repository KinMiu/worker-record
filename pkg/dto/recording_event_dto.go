package dto

// RecordingCompletedEventDTO represents the RabbitMQ message payload published
// when a 5-minute segmented MP4 chunk completes recording.
// Schema aligns 1:1 with Prisma backend Recording model.
type RecordingCompletedEventDTO struct {
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
