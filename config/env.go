package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

// EnvKey defines typed environment variable keys according to PT. LSKK Standard Stack v1.0.
type EnvKey string

const (
	PORT                         EnvKey = "PORT"
	WORKER_ID                    EnvKey = "WORKER_ID"
	API_BASE_URL                 EnvKey = "API_BASE_URL"
	API_KEY_HEADER               EnvKey = "API_KEY_HEADER"
	API_KEY                      EnvKey = "API_KEY"
	API_AUTH_TOKEN               EnvKey = "API_AUTH_TOKEN"
	RABBITMQ_URL                 EnvKey = "RABBITMQ_URL"
	RMQ_HOST                     EnvKey = "RMQ_HOST"
	RMQ_PORT                     EnvKey = "RMQ_PORT"
	RMQ_USER                     EnvKey = "RMQ_USER"
	RMQ_PASS                     EnvKey = "RMQ_PASS"
	RMQ_VHOST                    EnvKey = "RMQ_VHOST"
	RABBITMQ_QUEUE_NAME          EnvKey = "RABBITMQ_QUEUE_NAME"
	RECORD_STORAGE_PATH          EnvKey = "RECORD_STORAGE_PATH"
	RECORDING_BASE_URL           EnvKey = "RECORDING_BASE_URL"
	SEGMENT_DURATION_SECONDS     EnvKey = "SEGMENT_DURATION_SECONDS"
	SCAN_INTERVAL_SECONDS        EnvKey = "SCAN_INTERVAL_SECONDS"
	RETRY_INTERVAL_SECONDS       EnvKey = "RETRY_INTERVAL_SECONDS"
	ENABLE_S3_UPLOAD             EnvKey = "ENABLE_S3_UPLOAD"
	S3_ENDPOINT                  EnvKey = "S3_ENDPOINT"
	S3_ACCESS_KEY                EnvKey = "S3_ACCESS_KEY"
	S3_SECRET_KEY                EnvKey = "S3_SECRET_KEY"
	S3_BUCKET                    EnvKey = "S3_BUCKET"
	S3_REGION                    EnvKey = "S3_REGION"
	S3_USE_SSL                   EnvKey = "S3_USE_SSL"
	S3_DELETE_LOCAL_AFTER_UPLOAD EnvKey = "S3_DELETE_LOCAL_AFTER_UPLOAD"
	FFMPEG_PATH                  EnvKey = "FFMPEG_PATH"
	MQTT_BROKER                  EnvKey = "MQTT_BROKER"
	MQTT_CLIENT_ID               EnvKey = "MQTT_CLIENT_ID"
	MQTT_USERNAME                EnvKey = "MQTT_USERNAME"
	MQTT_PASSWORD                EnvKey = "MQTT_PASSWORD"
	MQTT_CAMERA_TOPIC            EnvKey = "MQTT_CAMERA_TOPIC"
)

// LoadEnv loads environment variables from .env if present.
func LoadEnv() error {
	return godotenv.Load()
}

// GetValue returns the string value of the environment variable.
func (e EnvKey) GetValue() string {
	return os.Getenv(string(e))
}

// GetValueOrDefault returns the environment variable value or fallback if not set.
func (e EnvKey) GetValueOrDefault(defaultVal string) string {
	val := os.Getenv(string(e))
	if val == "" {
		return defaultVal
	}
	return val
}

// GetValueInt returns integer value or fallback if invalid / empty.
func (e EnvKey) GetValueInt(defaultVal int) int {
	valStr := os.Getenv(string(e))
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(strings.TrimSpace(valStr))
	if err != nil {
		return defaultVal
	}
	return val
}

// GetValueBool returns boolean value or fallback if invalid / empty.
func (e EnvKey) GetValueBool(defaultVal bool) bool {
	valStr := os.Getenv(string(e))
	if valStr == "" {
		return defaultVal
	}
	valLower := strings.ToLower(strings.TrimSpace(valStr))
	if valLower == "true" || valLower == "1" || valLower == "yes" {
		return true
	}
	if valLower == "false" || valLower == "0" || valLower == "no" {
		return false
	}
	return defaultVal
}

// GetRabbitMQURL constructs or retrieves the effective AMQP URL.
func GetRabbitMQURL() string {
	url := RABBITMQ_URL.GetValue()
	if url != "" && !hasCustomRMQParams() {
		return url
	}

	host := RMQ_HOST.GetValueOrDefault("127.0.0.1")
	port := RMQ_PORT.GetValueInt(5672)
	user := RMQ_USER.GetValueOrDefault("guest")
	pass := RMQ_PASS.GetValueOrDefault("guest")
	vhost := RMQ_VHOST.GetValueOrDefault("/")

	uri := amqp.URI{
		Scheme:   "amqp",
		Host:     host,
		Port:     port,
		Username: user,
		Password: pass,
		Vhost:    vhost,
	}
	return uri.String()
}

func hasCustomRMQParams() bool {
	keys := []string{"RMQ_HOST", "RMQ_USER", "RMQ_PASS", "RMQ_PORT", "RMQ_VHOST"}
	for _, k := range keys {
		if val := os.Getenv(k); strings.TrimSpace(val) != "" {
			return true
		}
	}
	return false
}
