package otel_kafka_go

import (
	"context"
	"errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Tracer глобальный провайдер трассировки с помощью OpenTelemetry
type Tracer struct {
	exporter sdktrace.SpanExporter
	provider *sdktrace.TracerProvider
}

// NewProvider инициализация grpc экспортера и глобального провайдера OpenTelemetry трассировки
func NewProvider(ctx context.Context, servicename string, ednpoint string) (*Tracer, error) {

	exporter, err := otlptracegrpc.New( // grpc экспортер
		ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(ednpoint),
	)
	if err != nil {
		return nil, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(servicename),
			),
		),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(
			sdktrace.AlwaysSample(),
			//sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.2)), // 20% сэмплируем
			//sdktrace.NeverSample(),
		),
	)

	otel.SetTracerProvider(provider)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return &Tracer{
		exporter: exporter,
		provider: provider,
	}, nil
}

// Shutdown shuts down the trace exporter and trace provider.
func (t *Tracer) Shutdown(ctx context.Context) error {

	// Shutdown the trace provider.
	err := t.provider.Shutdown(ctx)

	// Shutdown the trace exporter.
	if err1 := t.exporter.Shutdown(ctx); err1 != nil {
		err = errors.Join(err, err1)
	}

	if err != nil {
		return err
	}
	return nil
}
