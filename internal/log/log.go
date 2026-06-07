package log

import (
	"context"
	"fmt"
	"os"

	logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
)

var (
	contextKeyLogging = contextKey("logging")
)

type contextKey string

func (c contextKey) String() string {
	return "logger" + string(c)
}

func StdLogger() *zap.Logger {
	logger, _ := zap.NewProduction(zap.AddCallerSkip(1))
	return logger
}

func LoggerForContext(ctx context.Context) *zap.Logger {
	existingLogger := ctx.Value(contextKeyLogging)
	if existingLogger == nil {
		return StdLogger()
	}
	if retLogger, ok := existingLogger.(*zap.Logger); ok {
		return retLogger
	}
	return StdLogger()
}

func ContextWithLogger(ctx context.Context, l *zap.Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogging, l)
}

func ErrorfCtx(ctx context.Context, err error, format string, args ...interface{}) {
	LoggerForContext(ctx).Error(fmt.Sprintf(format, args...), zap.Error(err))
}

func WarningfCtx(ctx context.Context, format string, args ...interface{}) {
	LoggerForContext(ctx).Warn(fmt.Sprintf(format, args...))
}

func InfofCtx(ctx context.Context, format string, args ...interface{}) {
	LoggerForContext(ctx).Info(fmt.Sprintf(format, args...))
}

func FatalfCtx(ctx context.Context, format string, args ...interface{}) {
	LoggerForContext(ctx).Fatal(fmt.Sprintf(format, args...))
	os.Exit(1)
}

// InterceptorLogger adapts zap logger to interceptor logger.
// This code is simple enough to be copied and not imported.
func InterceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)

		for i := 0; i < len(fields); i += 2 {
			key := fields[i]
			value := fields[i+1]

			switch v := value.(type) {
			case string:
				f = append(f, zap.String(key.(string), v))
			case int:
				f = append(f, zap.Int(key.(string), v))
			case bool:
				f = append(f, zap.Bool(key.(string), v))
			default:
				f = append(f, zap.Any(key.(string), v))
			}
		}

		logger := l.WithOptions(zap.AddCallerSkip(1)).With(f...)

		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg)
		case logging.LevelInfo:
			logger.Info(msg)
		case logging.LevelWarn:
			logger.Warn(msg)
		case logging.LevelError:
			logger.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}
