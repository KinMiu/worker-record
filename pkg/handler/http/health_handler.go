package http

import (
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/KinMiu/worker-record/config"
	"github.com/KinMiu/worker-record/pkg/handler/message_broker"
	"github.com/KinMiu/worker-record/pkg/model"
	"github.com/KinMiu/worker-record/pkg/service"
	"github.com/KinMiu/worker-record/pkg/utils"
)

// HealthHandler provides telemetry and operational health check endpoints.
type HealthHandler struct {
	startTime       time.Time
	recorderManager *service.RecorderManagerService
}

// NewHealthHandler creates an instance of HealthHandler.
func NewHealthHandler(recorderManager *service.RecorderManagerService) *HealthHandler {
	return &HealthHandler{
		startTime:       time.Now(),
		recorderManager: recorderManager,
	}
}

// CheckHealth returns the operational health status and active recorder count.
// @Summary Check worker recorder health status
// @Description Returns operational metrics, active recorder count, and RabbitMQ connectivity status
// @Tags Health
// @Produce json
// @Success 200 {object} model.HealthStatusResponse "Worker recorder is healthy"
// @Failure 500 {object} model.ResponseError[string] "Internal Server Error"
// @Router /api/v1/health [get]
func (h *HealthHandler) CheckHealth(c *fiber.Ctx) error {
	rmqConnected := false
	if message_broker.DefaultRabbitMQBroker != nil {
		rmqConnected = message_broker.DefaultRabbitMQBroker.IsConnected()
	}

	status := model.HealthStatus{
		Status:          "OK",
		WorkerID:        config.WORKER_ID.GetValue(),
		ActiveRecorders: h.recorderManager.ActiveRecorderCount(),
		RMQConnected:    rmqConnected,
		S3UploadEnabled: config.ENABLE_S3_UPLOAD.GetValueBool(false),
		Uptime:          time.Since(h.startTime).Round(time.Second).String(),
	}

	return utils.SuccessResponse(c, fiber.StatusOK, "Worker recorder service is running normally", status, nil)
}
