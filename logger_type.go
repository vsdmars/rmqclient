package rmqclient

import (
	"go.uber.org/zap"
)

type appLogger struct {
	*zap.Logger
	atom     *zap.AtomicLevel
	provided bool
}
