package rmqclient

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	noHandleErr  = errors.New("handle not registered")
	dupHandleErr = errors.New("handle already registered")
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
		err = dupHandleErr
	} else {
		rmq.consumeHandles[name] = &handle{
			h,
			nil,
			false, // init. false as not running
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
		if h.cancel != nil {
			h.cancel()
		}

		delete(rmq.consumeHandles, name)
	} else {
		err = noHandleErr
	}

	return
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
