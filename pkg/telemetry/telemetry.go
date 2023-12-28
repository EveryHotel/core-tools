package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	slogotel "github.com/DmitryKolbin/slog-otel"
	"github.com/getsentry/sentry-go"
	"github.com/getsentry/sentry-go/otel"
	slogsentry "github.com/samber/slog-sentry/v2"
	"github.com/uptrace/uptrace-go/uptrace"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type TelemetryParams struct {
	AppName    string
	LogEngine  string
	UptraceDSN string
	SentryDSN  string
	AppEnv     string
	LogFile    string
}

var isUptrace bool
var isSentry bool
var appName string

// Shutdown flushes all telemetry data and stops all background processes.
func Shutdown(ctx context.Context) error {
	if isSentry {
		sentry.Flush(2 * time.Second)
	}
	if isUptrace {
		if err := uptrace.Shutdown(ctx); err != nil {
			return fmt.Errorf("uptrace.Shutdown: %s", err)
		}
	}

	return nil
}

func GetTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(appName)
}

// Start initializes telemetry and logging.
func Start(params TelemetryParams) error {
	appName = params.AppName
	if len(params.UptraceDSN) > 0 {
		isUptrace = true
		uptrace.ConfigureOpentelemetry(
			uptrace.WithDeploymentEnvironment(params.AppEnv),
			uptrace.WithServiceName(params.AppName),
		)
	}
	if len(params.SentryDSN) > 0 {
		isSentry = true
		err := sentry.Init(sentry.ClientOptions{
			Dsn: params.SentryDSN,
			// Enable printing of SDK debug messages.
			// Useful when getting started or trying to figure something out.
			Debug:            false,
			EnableTracing:    true,
			TracesSampleRate: 1.0,
		})
		if err != nil {
			return fmt.Errorf("sentry.Init: %s", err)
		}

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSpanProcessor(sentryotel.NewSentrySpanProcessor()),
		)
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(sentryotel.NewSentryPropagator())
	}

	switch params.LogEngine {
	case "sentry":
		logger := slog.New(slogotel.OtelHandler{
			Next: slogsentry.Option{
				Level:     slog.LevelError,
				AddSource: true,
			}.NewSentryHandler(),
			AddSource: true,
		})
		logger = logger.
			With("environment", params.AppEnv)
		slog.SetDefault(logger)
	case "file":
		logfile, err := os.OpenFile(params.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("can't open log file: %w", err)
		}
		slog.SetDefault(slog.New(slogotel.OtelHandler{
			Next:      slog.NewTextHandler(logfile, nil),
			AddSource: true,
		}))
	default:
		slog.SetDefault(slog.New(slogotel.OtelHandler{
			Next:      slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}),
			AddSource: true,
		}))
	}

	return nil
}
