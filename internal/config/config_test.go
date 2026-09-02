package config

import (
	"os"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAMQPURI(t *testing.T) {
	uri := amqp.URI{
		Scheme:   "amqp",
		Host:     "195.35.23.135",
		Port:     5672,
		Username: "smk2iot",
		Password: "smk2iot",
		Vhost:    "/smk2pkl",
	}
	s := uri.String()
	t.Logf("amqp.URI String() = %s", s)

	parsed, err := amqp.ParseURI(s)
	if err != nil {
		t.Fatalf("ParseURI error: %v", err)
	}
	t.Logf("Parsed Vhost: %q", parsed.Vhost)
	if parsed.Vhost != "/smk2pkl" {
		t.Errorf("Expected vhost '/smk2pkl', got '%s'", parsed.Vhost)
	}
	if parsed.Host != "195.35.23.135" {
		t.Errorf("Expected host '195.35.23.135', got '%s'", parsed.Host)
	}
	if parsed.Username != "smk2iot" || parsed.Password != "smk2iot" {
		t.Errorf("Expected user/pass 'smk2iot', got '%s'/'%s'", parsed.Username, parsed.Password)
	}
}

func TestConfigLoad(t *testing.T) {
	os.Setenv("API_BASE_URL", "https://api-kamera.psti-ubl.id/devices")
	os.Setenv("RMQ_HOST", "195.35.23.135")
	os.Setenv("RMQ_USER", "smk2iot")
	os.Setenv("RMQ_PASS", "smk2iot")
	os.Setenv("RMQ_PORT", "5672")
	os.Setenv("RMQ_VHOST", "/smk2pkl")
	os.Setenv("RABBITMQ_QUEUE_NAME", "cctv.recordings")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.RMQHost != "195.35.23.135" {
		t.Errorf("Expected RMQHost '195.35.23.135', got '%s'", cfg.RMQHost)
	}
	if cfg.RMQUser != "smk2iot" {
		t.Errorf("Expected RMQUser 'smk2iot', got '%s'", cfg.RMQUser)
	}
	if cfg.RMQPass != "smk2iot" {
		t.Errorf("Expected RMQPass 'smk2iot', got '%s'", cfg.RMQPass)
	}
	if cfg.RMQPort != 5672 {
		t.Errorf("Expected RMQPort 5672, got %d", cfg.RMQPort)
	}
	if cfg.RMQVHost != "/smk2pkl" {
		t.Errorf("Expected RMQVHost '/smk2pkl', got '%s'", cfg.RMQVHost)
	}

	parsed, err := amqp.ParseURI(cfg.RabbitMQURL)
	if err != nil {
		t.Fatalf("Failed to parse generated RabbitMQURL: %v", err)
	}
	if parsed.Vhost != "/smk2pkl" {
		t.Errorf("Expected parsed vhost '/smk2pkl', got '%s'", parsed.Vhost)
	}
}
