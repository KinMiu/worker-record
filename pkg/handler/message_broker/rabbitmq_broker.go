package message_broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2/log"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/KinMiu/worker-record/config"
	"github.com/KinMiu/worker-record/pkg/dto"
)

// RabbitMQBroker manages AMQP connection lifecycle and publishing operations.
type RabbitMQBroker struct {
	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	isClosed bool
}

// DefaultRabbitMQBroker holds the singleton active broker instance.
var DefaultRabbitMQBroker *RabbitMQBroker

// InitRabbitMQBroker initializes and connects the default RabbitMQ broker instance.
func InitRabbitMQBroker() (*RabbitMQBroker, error) {
	broker := &RabbitMQBroker{}
	if err := broker.connectInternal(); err != nil {
		return nil, err
	}
	DefaultRabbitMQBroker = broker
	return broker, nil
}

func (p *RabbitMQBroker) connectInternal() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rmqURL := config.GetRabbitMQURL()
	queueName := config.RABBITMQ_QUEUE_NAME.GetValueOrDefault("cctv.recordings")

	log.Infof("[RABBITMQ] Connecting to broker at %s...", rmqURL)

	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ broker: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open AMQP channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare queue '%s': %w", queueName, err)
	}

	p.conn = conn
	p.channel = ch

	log.Infof("[RABBITMQ] Connected successfully. Target queue declared: '%s' (%d messages, %d consumers)",
		q.Name, q.Messages, q.Consumers)

	go p.handleReconnect(conn.NotifyClose(make(chan *amqp.Error, 1)))
	return nil
}

func (p *RabbitMQBroker) handleReconnect(closeErrChan chan *amqp.Error) {
	err, ok := <-closeErrChan
	if !ok {
		return
	}

	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	retrySecs := config.RETRY_INTERVAL_SECONDS.GetValueInt(5)
	log.Warnf("[RABBITMQ] Connection lost: %v. Initiating reconnect loop...", err)

	for {
		p.mu.Lock()
		if p.isClosed {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		time.Sleep(time.Duration(retrySecs) * time.Second)

		err := p.connectInternal()
		if err == nil {
			log.Info("[RABBITMQ] Reconnection succeeded.")
			return
		}

		log.Warnf("[RABBITMQ] Reconnect attempt failed: %v. Retrying in %ds...", err, retrySecs)
	}
}

// PublishToRmq sends raw message bytes to RabbitMQ using standard exchange/queue parameters.
func (p *RabbitMQBroker) PublishToRmq(ctx context.Context, message []byte, queueName, exchange string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel == nil || p.conn == nil || p.conn.IsClosed() {
		return fmt.Errorf("RabbitMQ connection is unavailable")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Body:         message,
	}

	return p.channel.PublishWithContext(
		ctx,
		exchange,
		queueName,
		false,
		false,
		msg,
	)
}

// PublishRecordingCompleted serializes and dispatches a RecordingCompletedEventDTO to the configured queue.
func (p *RabbitMQBroker) PublishRecordingCompleted(ctx context.Context, event dto.RecordingCompletedEventDTO) error {
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event payload to JSON: %w", err)
	}

	queueName := config.RABBITMQ_QUEUE_NAME.GetValueOrDefault("cctv.recordings")
	err = p.PublishToRmq(ctx, payloadBytes, queueName, "")
	if err != nil {
		return fmt.Errorf("failed to publish recording completed event: %w", err)
	}

	log.Infof("[RABBITMQ] Published RECORDING_COMPLETED event for device %s (%s) -> %s (Size: %d bytes, Duration: %ds)",
		event.DeviceID, event.DeviceName, event.FileName, event.Size, event.Duration)
	return nil
}

// IsConnected returns whether the RabbitMQ connection is active.
func (p *RabbitMQBroker) IsConnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn != nil && !p.conn.IsClosed()
}

// Close gracefully closes the AMQP channel and connection.
func (p *RabbitMQBroker) Close() {
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
	log.Info("[RABBITMQ] AMQP connection closed cleanly.")
}
