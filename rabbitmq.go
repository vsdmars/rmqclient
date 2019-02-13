package rmqclient

import (
	"context"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// NewRmq creates new rabbitmq connection instance
func NewRmq(ctx context.Context, cfg RmqConfig) (*RmqStruct, error) {
	// TODO:
	// validate value by calling RmqStruct.validate()
	// return error if validates fails
	rs := RmqStruct{
		ctx:           ctx,
		uuid:          uuid.New().String(),
		config:        cfg,
		consumeHandle: defaultHandle,
	}

	return &rs, nil
}

// RegisterConsumeHandle register consumer's handle
func (rmq *RmqStruct) RegisterConsumeHandle(handle ConsumeHandle) {
	rmq.consumeHandle = handle
}

// Run starts rabbitmq service
//
// Non-block call
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
