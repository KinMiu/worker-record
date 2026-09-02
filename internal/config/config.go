package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds all configuration properties for the worker.
type Config struct {
	WorkerID               string
	APIBaseURL             string
	APIKeyHeader           string
	APIKey                 string
	APIAuthToken           string
	RabbitMQURL            string
	RMQHost                string
	RMQPort                int
	RMQUser                string
	RMQPass                string
	RMQVHost               string
	RabbitMQQueueName      string
	RecordStoragePath      string
	RecordingBaseURL       string
	SegmentDurationSeconds int
	ScanIntervalSeconds    int
	RetryIntervalSeconds   int
	EnableS3Upload         bool
	FFmpegPath             string
}

// Load reads settings from the .env file (if present) and system environment variables.
func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("[CONFIG] No .env file found or unable to load, falling back to system environment variables")
	}

	workerID := getEnv("WORKER_ID", "worker_recorder_01")

	apiBaseURL := getEnv("API_BASE_URL", "")
	if apiBaseURL == "" {
		return nil, fmt.Errorf("API_BASE_URL environment variable is required")
	}

	apiKeyHeader := getEnv("API_KEY_HEADER", "x-api-key")
	apiKey := getEnv("API_KEY", "")
	apiAuthToken := getEnv("API_AUTH_TOKEN", "")

	// RabbitMQ parameters
	rmqHost := getEnv("RMQ_HOST", getEnv("RABBITMQ_HOST", "195.35.23.135"))
	rmqPort := getEnvInt("RMQ_PORT", getEnvInt("RABBITMQ_PORT", 5672))
	rmqUser := getEnv("RMQ_USER", getEnv("RABBITMQ_USER", "smk2iot"))
	rmqPass := getEnv("RMQ_PASS", getEnv("RABBITMQ_PASS", getEnv("RMQ_PASSWORD", getEnv("RABBITMQ_PASSWORD", "smk2iot"))))
	rmqVHost := getEnv("RMQ_VHOST", getEnv("RABBITMQ_VHOST", "/smk2pkl"))

	rabbitmqQueue := getEnv("RABBITMQ_QUEUE_NAME", getEnv("RMQ_QUEUE_NAME", "cctv.recordings"))

	// If explicit full URL is set and no RMQ_* override is provided, use it; otherwise construct from parameters
	rabbitmqURL := getEnv("RABBITMQ_URL", getEnv("RMQ_URL", ""))
	if rabbitmqURL == "" || hasCustomRMQParams() {
		uri := amqp.URI{
			Scheme:   "amqp",
			Host:     rmqHost,
			Port:     rmqPort,
			Username: rmqUser,
			Password: rmqPass,
			Vhost:    rmqVHost,
		}
		rabbitmqURL = uri.String()
	}

	recordStoragePath := getEnv("RECORD_STORAGE_PATH", "/opt/recordings/queue")
	recordingBaseURL := getEnv("RECORDING_BASE_URL", "http://195.35.23.135:9000/recordings")

	segmentDuration := getEnvInt("SEGMENT_DURATION_SECONDS", 300)
	if segmentDuration <= 0 {
		segmentDuration = 300
	}

	scanInterval := getEnvInt("SCAN_INTERVAL_SECONDS", 5)
	if scanInterval <= 0 {
		scanInterval = 5
	}

	retryInterval := getEnvInt("RETRY_INTERVAL_SECONDS", 5)
	if retryInterval <= 0 {
		retryInterval = 5
	}

	enableS3Upload := getEnvBool("ENABLE_S3_UPLOAD", false)
	ffmpegPath := getEnv("FFMPEG_PATH", "ffmpeg")

	return &Config{
		WorkerID:               workerID,
		APIBaseURL:             apiBaseURL,
		APIKeyHeader:           apiKeyHeader,
		APIKey:                 apiKey,
		APIAuthToken:           apiAuthToken,
		RabbitMQURL:            rabbitmqURL,
		RMQHost:                rmqHost,
		RMQPort:                rmqPort,
		RMQUser:                rmqUser,
		RMQPass:                rmqPass,
		RMQVHost:               rmqVHost,
		RabbitMQQueueName:      rabbitmqQueue,
		RecordStoragePath:      recordStoragePath,
		RecordingBaseURL:       recordingBaseURL,
		SegmentDurationSeconds: segmentDuration,
		ScanIntervalSeconds:    scanInterval,
		RetryIntervalSeconds:   retryInterval,
		EnableS3Upload:         enableS3Upload,
		FFmpegPath:             ffmpegPath,
	}, nil
}

func hasCustomRMQParams() bool {
	keys := []string{"RMQ_HOST", "RMQ_USER", "RMQ_PASS", "RMQ_PORT", "RMQ_VHOST", "RABBITMQ_HOST", "RABBITMQ_USER", "RABBITMQ_PASS", "RABBITMQ_VHOST"}
	for _, k := range keys {
		if val, exists := os.LookupEnv(k); exists && strings.TrimSpace(val) != "" {
			return true
		}
	}
	return false
}

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if valStr, exists := os.LookupEnv(key); exists && strings.TrimSpace(valStr) != "" {
		if val, err := strconv.Atoi(strings.TrimSpace(valStr)); err == nil {
			return val
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if valStr, exists := os.LookupEnv(key); exists && strings.TrimSpace(valStr) != "" {
		valLower := strings.ToLower(strings.TrimSpace(valStr))
		if valLower == "true" || valLower == "1" || valLower == "yes" {
			return true
		}
		if valLower == "false" || valLower == "0" || valLower == "no" {
			return false
		}
	}
	return fallback
}
