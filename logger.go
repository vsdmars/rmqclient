package rmqclient

import (
	"fmt"
	"os"
	gosync "sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger appLogger
var once gosync.Once

func init() {
	initLogger()

	// default debug level
	setLevel(zapcore.DebugLevel)
}

func sync() {
	logger.Sync()
}

func setLevel(l zapcore.Level) {
	logger.atom.SetLevel(l)
}

func initLogger() {
	initLogger := func() {
		// default log level set to 'info'
		atom := zap.NewAtomicLevelAt(zap.InfoLevel)

		config := zap.Config{
			Level:       atom,
			Development: false,
			Sampling: &zap.SamplingConfig{
				Initial:    100,
				Thereafter: 100,
			},
			Encoding:         "json", // console, json, toml
			EncoderConfig:    zap.NewProductionEncoderConfig(),
			OutputPaths:      []string{"stderr"},
			ErrorOutputPaths: []string{"stderr"},
		}

		mylogger, err := config.Build()
		if err != nil {
			fmt.Printf("Initialize zap logger error: %v", err)
			os.Exit(1)
		}

		logger = appLogger{mylogger, &atom}
	}

	once.Do(initLogger)
}
