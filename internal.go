package rmqclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// defaultHandle is a template for handler writers
func defaultHandle(ctx context.Context, channel *amqp.Channel) error {
	logger.Info(
		"default rabbitmq consume handler",
		zap.String("service", serviceName),
	)

	<-ctx.Done()
	return errors.New("service ends")
}

func (rmq *RmqStruct) start() <-chan string {
	status := make(chan string)

	go func() {
		var reconnect = true
		sctx, cancel := context.WithCancel(rmq.ctx)

		defer func() {
			// cleanup consumer goroutine
			cancel()

			if reconnect {
				logger.Info(
					"re-establish connection",
					zap.String("service", serviceName),
					zap.String("uuid", rmq.uuid),
					zap.String(
						"wait_time",
						(rmq.config.ReconnectWait*time.Second).String(),
					),
				)

				time.Sleep(rmq.config.ReconnectWait * time.Second)
			}

			// cleanup status
			close(status)
		}()

		// create rabbitmq connection
		if err := rmq.createConnect(); err != nil {
			return
		}
		status <- "rabbitmq connection established"

		// create rabbitmq channel
		if err := rmq.createChannel(); err != nil {
			return
		}
		status <- "rabbitmq channel established"

		go rmq.consume(sctx)
		status <- "rabbitmq consumer established"

		err := rmq.catchEvent().(retryError)
		reconnect = err.reconnect
		status <- fmt.Sprintf("amqp event occurred: %s", err.Error())
	}()

	return status
}

func (rmq *RmqStruct) catchEvent() error {
	select {
	case <-rmq.ctx.Done():
		logger.Info(
			"service ends",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
		)

		return retryError{
			errors.New("service ends, cleanup connection loop"),
			false,
		}
	case err := <-rmq.connCloseError:
		logger.Warn(
			"lost connection",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", err.Error()),
		)

		return retryError{
			err,
			true,
		}
	case val := <-rmq.channelCancelError:
		// interestingly, the amqp library won't trigger
		// this event iff we are not using amqp.Channel
		// to declare the queue.
		// However, if the queue is autodeleted, this event will be triggered.
		logger.Warn(
			"lost channel",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", val),
		)

		return retryError{
			errors.New(val),
			true,
		}
	}
}

// createConnect creates amqp connection
func (rmq *RmqStruct) createConnect() error {
	amqpURL := amqp.URI{
		Scheme:   "amqp",
		Host:     rmq.config.Host,
		Username: rmq.config.Username,
		Password: "XXXXX",
		Port:     rmq.config.Port,
		Vhost:    rmq.config.Vhost,
	}

	logger.Info(
		"connect URL",
		zap.String("service", serviceName),
		zap.String("uuid", rmq.uuid),
		zap.String("url", amqpURL.String()),
	)

	amqpURL.Password = rmq.config.Password

	// tcp connection timeout in 3 seconds
	myconn, err := amqp.DialConfig(
		amqpURL.String(),
		amqp.Config{
			Vhost: rmq.config.Vhost,
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, 3*time.Second)
			},
			Heartbeat: 10 * time.Second,
			Locale:    "en_US"},
	)
	if err != nil {
		logger.Warn(
			"open connection failed",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", err.Error()),
		)

		return err
	}

	rmq.connection = myconn
	rmq.connCloseError = make(chan *amqp.Error)
	// amqp library is resposible for closing the error channel
	rmq.connection.NotifyClose(rmq.connCloseError)
	return nil
}

// createChannel creates amqp channel
func (rmq *RmqStruct) createChannel() error {
	myChannel, err := rmq.connection.Channel()
	if err != nil {
		logger.Warn(
			"create channel failed",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", err.Error()),
		)

		return err
	}

	rmq.channel = myChannel

	// These can be sent from the server when a queue is deleted or
	// when consuming from a mirrored queue where the master has just failed
	// (and was moved to another node)
	rmq.channelCancelError = make(chan string)
	// amqp library is resposible for closing the error channel
	rmq.channel.NotifyCancel(rmq.channelCancelError)
	return nil
}

func (rmq *RmqStruct) consume(ctx context.Context) {
	if err := rmq.consumeHandle(ctx, rmq.channel); err != nil {
		logger.Warn(
			"queue consume error",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
			zap.String("error", err.Error()),
		)
	}
}

// TODO: fill the guts~
func (rmq *RmqStruct) validate() error {
	return nil
}
