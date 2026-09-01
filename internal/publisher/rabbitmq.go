package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/KinMiu/worker-record/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQPublisher manages reliable, persistent event publishing to RabbitMQ.
type RabbitMQPublisher struct {
	cfg      *config.Config
	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	isClosed bool
}

// NewRabbitMQPublisher creates a new RabbitMQ publisher instance.
func NewRabbitMQPublisher(cfg *config.Config) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		cfg: cfg,
	}
}

// Connect establishes the AMQP connection and declares the target queue.
func (p *RabbitMQPublisher) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed {
		return fmt.Errorf("publisher is already marked as closed")
	}

	return p.connectInternal()
}

func (p *RabbitMQPublisher) connectInternal() error {
	log.Printf("[RABBITMQ] Connecting to broker at %s...", p.cfg.RabbitMQURL)

	conn, err := amqp.Dial(p.cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ broker: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open AMQP channel: %w", err)
	}

	// Declare durable queue to ensure messages persist through broker restarts
	q, err := ch.QueueDeclare(
		p.cfg.RabbitMQQueueName, // queue name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare queue '%s': %w", p.cfg.RabbitMQQueueName, err)
	}

	p.conn = conn
	p.channel = ch

	log.Printf("[RABBITMQ] Connected successfully. Target queue declared: '%s' (%d messages, %d consumers)",
		q.Name, q.Messages, q.Consumers)

	// Listen for connection closures and initiate auto-reconnection
	go p.handleReconnect(conn.NotifyClose(make(chan *amqp.Error, 1)))

	return nil
}

func (p *RabbitMQPublisher) handleReconnect(closeErrChan chan *amqp.Error) {
	err, ok := <-closeErrChan
	if !ok {
		return // Channel closed normally
	}

	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	log.Printf("[RABBITMQ][WARNING] Connection lost: %v. Initiating reconnect loop...", err)

	for {
		p.mu.Lock()
		if p.isClosed {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		time.Sleep(time.Duration(p.cfg.RetryIntervalSeconds) * time.Second)

		p.mu.Lock()
		err := p.connectInternal()
		if err == nil {
			p.mu.Unlock()
			log.Println("[RABBITMQ] Reconnection succeeded.")
			return
		}
		p.mu.Unlock()

		log.Printf("[RABBITMQ][WARNING] Reconnect attempt failed: %v. Retrying in %ds...", err, p.cfg.RetryIntervalSeconds)
	}
}

// PublishRecordingCompleted sends a persistent RECORDING_COMPLETED JSON event to RabbitMQ.
func (p *RabbitMQPublisher) PublishRecordingCompleted(ctx context.Context, event models.RecordingCompletedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event payload to JSON: %w", err)
	}

	if p.channel == nil || p.conn == nil || p.conn.IsClosed() {
		// Attempt one reconnect try if channel is down
		if err := p.connectInternal(); err != nil {
			return fmt.Errorf("cannot publish message, connection unavailable: %w", err)
		}
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent, // Persistent delivery ensures disk storage
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Body:         payloadBytes,
	}

	err = p.channel.PublishWithContext(
		ctx,
		"",                      // default direct exchange
		p.cfg.RabbitMQQueueName, // routing key (queue name)
		false,                   // mandatory
		false,                   // immediate
		msg,
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to queue '%s': %w", p.cfg.RabbitMQQueueName, err)
	}

	log.Printf("[RABBITMQ] Published RECORDING_COMPLETED event for device %s (%s) -> %s (Size: %d bytes, Duration: %ds)",
		event.DeviceID, event.DeviceName, event.FileName, event.Size, event.Duration)

	return nil
}

// Close gracefully closes the AMQP channel and connection.
func (p *RabbitMQPublisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isClosed = true

	if p.channel != nil {
		_ = p.channel.Close()
		p.channel = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}

	log.Println("[RABBITMQ] AMQP connection closed cleanly.")
}
