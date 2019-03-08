package rmqclient

import (
	"errors"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

var errNoConnection = errors.New("no rabbitmq connection")

func (p *publish) Publish(
	exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {

	pfunc := func() error {
		if err := p.c.Publish(exchange, key, mandatory, immediate, msg); err != nil {
			return err
		}
	}

	select {
	case <-p.r.cctx.Done():
		logger.Error(
			"publish failed",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", "no rabbitmq connection"),
		)

		return errNoConnection
	default:
		return pfunc()
	}
}
