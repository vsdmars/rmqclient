package rmqclient

import (
	"errors"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

var errNoConnection = errors.New("no rabbitmq connection")
var errChannelClosed = errors.New("channel closed")
var errPublishFailed = errors.New("publish failed")

// Publish send message to the rabbitmq server.
func (p *Publish) Publish(
	exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {

	if p.closed {
		return errChannelClosed
	}

	if err := p.c.Publish(exchange, key, mandatory, immediate, msg); err != nil {
		return err
	}

	if p.confirm {
		c := <-p.pubConfirm

		logger.Debug(
			"publish confirm message",
			zap.String("service", serviceName),
			zap.String("uuid", p.rmq.uuid),
			zap.Uint64("DeliveryTag", c.DeliveryTag),
			zap.Bool("Ack", c.Ack),
		)

		if c.Ack {
			return nil
		}
		return errPublishFailed
	}

	return nil
}

// Confirm returns current publish's confirm mode.
func (p *Publish) Confirm() bool {
	return p.confirm
}

// Close closes publish channel.
func (p *Publish) Close() {
	p.closed = true
	p.c.Close()
}
