package rmqclient

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

var (
	errNoHandle  = errors.New("handle not registered")
	errDupHandle = errors.New("handle already registered")
)

// NewRmq creates new rabbitmq connection instance
func NewRmq(ctx context.Context, cfg RmqConfig) (*RmqStruct, error) {
	// TODO:
	// validate value by calling RmqStruct.validate()
	// return error if validates fails
	rs := RmqStruct{
		ctx:            ctx,
		uuid:           uuid.New().String(),
		config:         cfg,
		consumeHandles: make(map[string]*handle),
	}

	return &rs, nil
}

// RegisterHandle register consumer's handle
func (rmq *RmqStruct) RegisterHandle(
	name string, // queue's name
	h ConsumeHandle,
	autoAck bool, // autoack
	exclusive bool, // exclusive
	noWait bool, // nowait
) (err error) {
	defer rmq.rwlock.Unlock()
	rmq.rwlock.Lock()

	if _, ok := rmq.consumeHandles[name]; ok {
		err = errDupHandle
	} else {
		running := atomic.Value{}
		running.Store(false)
		rmq.consumeHandles[name] = &handle{
			h,
			nil,
			running,
			autoAck,
			exclusive,
			noWait,
		}
	}

	return
}

// UnregisterHandle unregister consumer's handle
func (rmq *RmqStruct) UnregisterHandle(name string) (err error) {
	defer rmq.rwlock.Unlock()
	rmq.rwlock.Lock()

	if h, ok := rmq.consumeHandles[name]; ok {
		delete(rmq.consumeHandles, name)

		if h.cancel != nil {
			h.cancel()
		}
	} else {
		err = errNoHandle
	}

	return
}

// GetPublish retrieve publish from pool
func (rmq *RmqStruct) GetPublish(confirm bool) *publish {
	if rmq.channelPool.New == nil {
		rmq.channelPool.New = func() interface{} {
			if rmq.cctx.Done() == nil || rmq.connection == nil {
				return nil
			}

			select {
			// connection context.
			case <-rmq.cctx.Done():
				logger.Error(
					"no rabbitmq connection",
					zap.String("service", serviceName),
					zap.String("uuid", rmq.uuid),
				)

				return nil
			default:
				if ch, err := rmq.connection.Channel(); err != nil {
					logger.Error(
						"create publish channel failed",
						zap.String("service", serviceName),
						zap.String("uuid", rmq.uuid),
						zap.String("error", err.Error()),
					)

					return nil
				} else {
					var pubC chan amqp.Confirmation = nil

					if confirm {
						if err := ch.Confirm(false); err != nil {
							logger.Error(
								"set publish confirm failed",
								zap.String("service", serviceName),
								zap.String("uuid", rmq.uuid),
								zap.String("error", err.Error()),
							)

							return nil
						}

						pubC = ch.NotifyPublish(make(chan amqp.Confirmation, 10))
					}

					return &publish{ch, rmq, confirm, pubC}
				}
			}
		}
	}

	return rmq.channelPool.Get().(*publish)
}

// ReleasePublish recycle 'publish' instance.
func (rmq *RmqStruct) ReleasePublish(p *publish) {
	rmq.channelPool.Put(p)
}

// Run starts rabbitmq service
//
// Non-block call
//
// Runs as daemon, exit on caller's context cancel()
func (rmq *RmqStruct) Run() {
	// sync logger
	defer Sync()

	go func() {
		logger.Info(
			"service starts",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
		)

		for {
			select {
			case <-rmq.ctx.Done():
				logger.Info(
					"service ends",
					zap.String("service", serviceName),
					zap.String("uuid", rmq.uuid),
				)
				return
			default:
				for s := range rmq.start() {
					logger.Info(
						"status",
						zap.String("service", serviceName),
						zap.String("uuid", rmq.uuid),
						zap.String("status", s),
					)
				}
			}
		}
	}()
}
