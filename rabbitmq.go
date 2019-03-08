package rmqclient

import (
	"context"
	"errors"

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
	name string, // queue name
	h ConsumeHandle, // ConsumeHandle
	autoAck bool, // autoack
	exclusive bool, // exclusive
	noWait bool, // nowait
) (err error) {
	defer rmq.rwlock.Unlock()
	rmq.rwlock.Lock()

	if _, ok := rmq.consumeHandles[name]; ok {
		err = errDupHandle
	} else {
		rmq.consumeHandles[name] = &handle{
			h:         h,         // ConsumeHandle
			cancel:    nil,       // context.CancelFunc
			autoAck:   autoAck,   // bool
			exclusive: exclusive, // bool
			noWait:    noWait,    // bool
		}
		rmq.consumeHandles[name].running.Store(false)
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
func (rmq *RmqStruct) GetPublish(confirm bool) (*Publish, error) {
	if rmq.cctx.Load() == nil ||
		rmq.cctx.Load().(context.Context).Done() == nil ||
		rmq.connection.Load() == nil {
		return nil, errNoConnection
	}

	select {
	// connection context.
	case <-rmq.cctx.Load().(context.Context).Done():
		logger.Error(
			"no rabbitmq connection",
			zap.String("service", serviceName),
			zap.String("uuid", rmq.uuid),
		)

		return nil, errNoConnection
	default:
		if ch, err := rmq.connection.Load().(*amqp.Connection).Channel(); err != nil {
			logger.Error(
				"create publish channel failed",
				zap.String("service", serviceName),
				zap.String("uuid", rmq.uuid),
				zap.String("error", err.Error()),
			)

			return nil, errNoConnection
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

					return nil, errNoConnection
				}

				pubC = ch.NotifyPublish(make(chan amqp.Confirmation, 10))
			}

			return &Publish{ch, rmq, false, confirm, pubC}, nil
		}
	}
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

				if rmq.connection.Load() != nil {
					rmq.connection.Load().(*amqp.Connection).Close()
				}
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
