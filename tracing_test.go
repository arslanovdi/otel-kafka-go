package otel_kafka_go

import (
	"bytes"
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

var tracer = otel.GetTracerProvider().Tracer(
	"test service",
	trace.WithSchemaURL(semconv.SchemaURL),
)

var buf bytes.Buffer // буфер для вывода трэйсинга

var duration = 1 * time.Second // время ожидания отправки сообщения в буфер провайдером OpenTelemetry

func TestMain(m *testing.M) {

	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(&buf), //os.Stdout), //
	)

	if err != nil {
		os.Exit(1)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("test service"),
			),
		),
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(500*time.Millisecond)),
		sdktrace.WithSampler(
			sdktrace.AlwaysSample(),
		),
	)

	otel.SetTracerProvider(provider)

	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown trace provider", slog.String("error", err.Error()))
		}
	}()

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	code := m.Run()
	os.Exit(code)
}

func Test_otelProvider_Context(t *testing.T) {
	t.Run("msg == nil", func(t *testing.T) {
		t.Parallel()
		ctx := Context(nil)
		assert.Equal(t, ctx, context.Background())
	})
	t.Run("msg.TopicPartition.Topic == nil", func(t *testing.T) {
		t.Parallel()
		ctx := Context(&kafka.Message{})
		assert.Equal(t, ctx, context.Background())
	})
	t.Run("msg == kafka.Message without span Headers", func(t *testing.T) {
		t.Parallel()
		topic := "topic"
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}
		ctx := Context(msg)
		assert.Equal(t, ctx, context.Background())
	})
	t.Run("msg == kafka.Message with span Headers", func(t *testing.T) {
		t.Parallel()
		_, span := tracer.Start(context.Background(), "span name in test") // сгенерировали span
		defer span.End()

		traceId := span.SpanContext().TraceID().String()
		spanId := span.SpanContext().SpanID().String()
		topic := "topic"
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}
		SetSpanAttributes(span.SpanContext(), msg) // привязали span к msg

		ctx := Context(msg) // вытаскиваем span из msg
		assert.Equal(t, traceId, trace.SpanFromContext(ctx).SpanContext().TraceID().String())
		assert.Equal(t, spanId, trace.SpanFromContext(ctx).SpanContext().SpanID().String())
	})
}

func Test_otelProvider_OnCommit(t *testing.T) {
	t.Run("msg nil pointer dereference", func(t *testing.T) {
		op := NewOtelConsumer("test")
		op.OnCommit(nil, "group")
	})

	t.Run("msg.TopicPartition.Topic == nil", func(t *testing.T) {
		op := New("test")
		op.OnCommit(&kafka.Message{}, "group")
	})

	t.Run("OnCommit with span", func(t *testing.T) {
		op := NewOtelConsumer("test")
		// create root span
		_, span := tracer.Start(context.Background(), "root span name in test") // сгенерировали span
		traceId := span.SpanContext().TraceID().String()
		spanId := span.SpanContext().SpanID().String()
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		SetSpanAttributes(span.SpanContext(), &msg) // привязали span к msg

		buf.Reset()

		op.OnCommit(&msg, "group")

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		foundspanid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, traceId) { // нашли root traceid
				foundrootid++
			}
			if strings.Contains(line, spanId) { // нашли root traceid
				foundspanid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing
		assert.Equal(t, 1, foundspanid) // должно быть 1 вхождение spanid в tracing, т.к. OnCommit формирует новый span

		for _, h := range msg.Headers {
			switch h.Key {
			case TraceHeaderName:
				assert.Equal(t, traceId, string(h.Value)) // после выполнения OnProcess в msg.Headers должен быть root traceid
			case SpanHeaderName:
				assert.NotEqual(t, spanId, string(h.Value)) // spanid должен быть новым
			}
		}

	})

	t.Run("OnCommit without span", func(t *testing.T) {
		op := NewOtelConsumer("test")
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		buf.Reset()

		op.OnCommit(&msg, "group")

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, "TraceID") { // нашли root traceid
				foundrootid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing. Просто проверяем что информация отправлена.

		traceid, spanid := false, false
		for _, h := range msg.Headers {
			switch h.Key {
			case TraceHeaderName:
				traceid = true
			case SpanHeaderName:
				spanid = true
			}
		}

		assert.True(t, traceid) // после выполнения OnProcess в msg.Headers должен быть root traceid
		assert.True(t, spanid)  // после выполнения OnProcess в msg.Headers должен быть spanid
	})
}

func Test_otelProvider_OnPoll(t *testing.T) {
	t.Run("msg nil pointer dereference", func(t *testing.T) {
		op := NewOtelConsumer("test")
		op.OnPoll(nil, "group")
	})

	t.Run("msg.TopicPartition.Topic == nil", func(t *testing.T) {
		op := NewOtelConsumer("test")
		op.OnPoll(&kafka.Message{}, "group")
	})

	t.Run("OnPoll with span", func(t *testing.T) {
		op := NewOtelConsumer("test")
		// create root span
		_, span := tracer.Start(context.Background(), "root span name in test") // сгенерировали span
		traceId := span.SpanContext().TraceID().String()
		spanId := span.SpanContext().SpanID().String()
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		SetSpanAttributes(span.SpanContext(), &msg) // привязали span к msg

		buf.Reset()

		op.OnPoll(&msg, "group")

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		foundspanid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, traceId) { // нашли root traceid
				foundrootid++
			}
			if strings.Contains(line, spanId) { // нашли root traceid
				foundspanid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing
		assert.Equal(t, 1, foundspanid) // должно быть 1 вхождение spanid в tracing, т.к. OnPoll формирует новый span

	})

	t.Run("OnPoll without span", func(t *testing.T) {
		op := NewOtelConsumer("test")
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		buf.Reset()

		op.OnPoll(&msg, "group")

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, "TraceID") { // нашли root traceid
				foundrootid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing. Просто проверяем что информация отправлена.
	})
}

func Test_otelProvider_OnProcess(t *testing.T) {
	t.Run("msg nil pointer dereference", func(t *testing.T) {
		op := NewOtelConsumer("test")
		op.OnProcess(nil, "group")
	})

	t.Run("msg.TopicPartition.Topic == nil", func(t *testing.T) {
		op := New("test")
		op.OnProcess(&kafka.Message{}, "group")
	})

	t.Run("OnProcess with span", func(t *testing.T) {
		op := NewOtelConsumer("test")
		// create root span
		_, span := tracer.Start(context.Background(), "root span name in test") // сгенерировали span
		traceId := span.SpanContext().TraceID().String()
		spanId := span.SpanContext().SpanID().String()
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		SetSpanAttributes(span.SpanContext(), &msg) // привязали span к msg

		buf.Reset()

		op.OnProcess(&msg, "group")

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		foundspanid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, traceId) { // нашли root traceid
				foundrootid++
			}
			if strings.Contains(line, spanId) { // нашли root traceid
				foundspanid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing
		assert.Equal(t, 1, foundspanid) // должно быть 1 вхождение spanid в tracing, т.к. OnProcess формирует новый span

		for _, h := range msg.Headers {
			switch h.Key {
			case TraceHeaderName:
				assert.Equal(t, traceId, string(h.Value)) // после выполнения OnProcess в msg.Headers должен быть root traceid
			case SpanHeaderName:
				assert.NotEqual(t, spanId, string(h.Value)) // spanid должен быть новым
			}
		}

	})

	t.Run("OnPoll without span", func(t *testing.T) {
		op := NewOtelConsumer("test")
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		buf.Reset()

		op.OnProcess(&msg, "group")

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, "TraceID") { // нашли root traceid
				foundrootid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing. Просто проверяем что информация отправлена.

		traceid, spanid := false, false
		for _, h := range msg.Headers {
			switch h.Key {
			case TraceHeaderName:
				traceid = true
			case SpanHeaderName:
				spanid = true
			}
		}

		assert.True(t, traceid) // после выполнения OnProcess в msg.Headers должен быть root traceid
		assert.True(t, spanid)  // после выполнения OnProcess в msg.Headers должен быть spanid
	})

}

func Test_otelProvider_OnSend(t *testing.T) {
	t.Run("msg nil pointer dereference", func(t *testing.T) {
		op := New("test")
		op.OnSend(context.Background(), nil)
	})

	t.Run("msg.TopicPartition.Topic == nil", func(t *testing.T) {
		op := New("test")
		op.OnSend(context.Background(), &kafka.Message{})
	})

	t.Run("ctx nil pointer dereference", func(t *testing.T) {
		op := New("test")
		topic := "topic"
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}
		op.OnSend(nil, msg)
	})

	t.Run("OnSend", func(t *testing.T) {
		op := NewOtelProducer("test")
		// create root span
		ctx, span := tracer.Start(context.Background(), "root span name in test") // сгенерировали span
		traceId := span.SpanContext().TraceID().String()
		spanId := span.SpanContext().SpanID().String()
		topic := "topic"
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte("key"),
			Value: []byte("value"),
		}
		buf.Reset()
		op.OnSend(ctx, &msg)

		time.Sleep(duration) // waiting for OpenTelemetry provider send tracing
		// check tracing
		foundrootid := 0
		foundspanid := 0
		// Читаем лог отправки трейсинга через OpenTelemetry provider
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.Fail(t, "Failed to read line", err)
				break
			}

			if strings.Contains(line, traceId) { // нашли root traceid
				foundrootid++
			}
			if strings.Contains(line, spanId) { // нашли root traceid
				foundspanid++
			}
		}
		assert.Equal(t, 2, foundrootid) // должно быть 2 вхождения rootid в tracing
		assert.Equal(t, 1, foundspanid) // должно быть 1 вхождения spanid в tracing, т.к. OnSend формирует новый span

		for _, h := range msg.Headers {
			switch h.Key {
			case TraceHeaderName:
				assert.Equal(t, traceId, string(h.Value)) // после выполнения OnSend в msg.Headers должен быть root traceid
			case SpanHeaderName:
				assert.NotEqual(t, spanId, string(h.Value)) // spanid должен быть новым
			}
		}

	})

}

func Test_setSpanAttributes(t *testing.T) {
	t.Run("msg nil pointer dereference", func(t *testing.T) { // Тут нет asserts, достаточно того что не вылетел nil pointer dereference
		t.Parallel()
		SetSpanAttributes(trace.SpanContext{}, nil)
	})
	t.Run("SetSpanAttributes", func(t *testing.T) {
		t.Parallel()
		_, span := tracer.Start(context.Background(), "span name in test") // сгенерировали span
		defer span.End()
		traceId := span.SpanContext().TraceID().String()
		spanId := span.SpanContext().SpanID().String()

		msg := &kafka.Message{}

		SetSpanAttributes(span.SpanContext(), msg)

		sampled := false
		traceid2 := ""
		spanid2 := ""
		for _, h := range msg.Headers {
			switch string(h.Key) {
			case TraceHeaderName:
				traceid2 = string(h.Value)
			case SpanHeaderName:
				spanid2 = string(h.Value)
			case SampledHeaderName:
				sampled = true
			}
		}
		assert.Equal(t, traceId, traceid2)
		assert.Equal(t, spanId, spanid2)
		assert.Equal(t, sampled, span.SpanContext().IsSampled())

	})
}
