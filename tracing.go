// Package otel_kafka_go
// OpenTelemetry instrumentation for confluent-kafka-go
package otel_kafka_go

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"strconv"
)

// These based on OpenTelemetry API & SDKs for go
// https://opentelemetry.io/docs/languages/go/

// Semantic Conventions for Kafka 21.11.2024
// https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md

// Semantic Conventions for Messaging Spans 21.11.2024
// https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/messaging-spans.md

// General Attributes 21.11.2024
// https://github.com/open-telemetry/semantic-conventions/blob/main/docs/general/attributes.md#general-remote-service-attributes

const (
	otelLibraryName = "github.com/arslanovdi/otel-kafka-go"
	otelLibraryVer  = "v0.0.1"

	TraceHeaderName   = "trace_id"
	SpanHeaderName    = "span_id"
	SampledHeaderName = "sampled"
)

// OtelProducer
// Интерфейс обработчика трассировки kafka продюсера
type OtelProducer interface {
	OnSend(ctx context.Context, msg *kafka.Message) // tracing produce message. integrate tracing into msg headers
}

// OtelConsumer
// Интерфейс обработчика трассировки kafka консюмера
type OtelConsumer interface {
	OnPoll(msg *kafka.Message, group string)    // tracing poll(read) message.
	OnProcess(msg *kafka.Message, group string) // tracing process message. update tracing info in msg headers.
	OnCommit(msg *kafka.Message, group string)  // tracing commit message. update tracing info in msg headers.
	Context(message *kafka.Message) context.Context
}

type OtelProvider interface {
	OtelProducer
	OtelConsumer
}

type otelProvider struct {
	tracer     trace.Tracer
	fixedAttrs []attribute.KeyValue
}

func (op *otelProvider) new(instance string) {
	op.tracer = otel.GetTracerProvider().Tracer(otelLibraryName, trace.WithInstrumentationVersion(otelLibraryVer))
	op.fixedAttrs = []attribute.KeyValue{
		attribute.String("messaging.client.id", instance),
		attribute.String("messaging.system", "kafka"),
	}
}

// New
// Инициализация обработчика трассировки kafka
// global trace provider must be initialized
func New(instance string) OtelProvider {
	op := otelProvider{}
	op.new(instance)
	return &op
}

// NewOtelProducer
// Инициализация обработчика трассировки kafka продюсера
// global trace provider must be initialized
func NewOtelProducer(instance string) OtelProducer {
	op := otelProvider{}
	op.new(instance)
	return &op
}

// NewOtelConsumer
// Инициализация обработчика трассировки kafka консюмера
// global trace provider must be initialized
func NewOtelConsumer(instance string) OtelConsumer {
	op := otelProvider{}
	op.new(instance)
	return &op
}

// OnSend
// tracing produce message.
// integrate tracing into msg headers
func (op *otelProvider) OnSend(ctx context.Context, msg *kafka.Message) {
	if msg == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	attWithTopic := append(
		op.fixedAttrs,
		attribute.String("messaging.destination.name", *msg.TopicPartition.Topic),
		attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)),
		attribute.String("messaging.operation.name", "send"),
		attribute.String("messaging.operation.type", "send"),
	)

	if len(msg.Key) > 0 { //  If the key is null, the attribute MUST NOT be set
		attWithTopic = append(attWithTopic, attribute.String("messaging.kafka.message.key", string(msg.Key)))
	}

	_, span := op.tracer.Start(
		ctx,
		*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))

	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(attribute.String("messaging.message.id", spanContext.SpanID().String()))

	setSpanAttributes(spanContext, msg)
}

// OnPoll
// tracing poll(read) message.
func (op *otelProvider) OnPoll(msg *kafka.Message, group string) {
	if msg == nil {
		return
	}

	attWithTopic := append(
		op.fixedAttrs,
		attribute.String("messaging.destination.name", *msg.TopicPartition.Topic),
		attribute.String("messaging.consumer.group.name", group),
		attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)),
		attribute.String("messaging.operation.name", "poll"),
		attribute.String("messaging.operation.type", "receive"),
		attribute.String("messaging.kafka.message.offset", strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)),
	)

	if len(msg.Key) > 0 { //  If the key is null, the attribute MUST NOT be set
		attWithTopic = append(attWithTopic, attribute.String("messaging.kafka.message.key", string(msg.Key)))
	}

	_, span := op.tracer.Start(
		op.Context(msg),
		*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))
	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(attribute.String("messaging.message.id", spanContext.SpanID().String()))
}

// OnProcess
// tracing processing message.
// update tracing info in msg headers.
func (op *otelProvider) OnProcess(msg *kafka.Message, group string) {
	if msg == nil {
		return
	}

	attWithTopic := append(
		op.fixedAttrs,
		attribute.String("messaging.destination.name", *msg.TopicPartition.Topic),
		attribute.String("messaging.consumer.group.name", group),
		attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.kafka.message.offset", strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)),
	)

	if len(msg.Key) > 0 { //  If the key is null, the attribute MUST NOT be set
		attWithTopic = append(attWithTopic, attribute.String("messaging.kafka.message.key", string(msg.Key)))
	}

	_, span := op.tracer.Start(
		op.Context(msg),
		*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))
	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(attribute.String("messaging.message.id", spanContext.SpanID().String()))

	setSpanAttributes(spanContext, msg)
}

// OnCommit
// tracing commit message.
// update tracing info in msg headers.
func (op *otelProvider) OnCommit(msg *kafka.Message, group string) {
	if msg == nil {
		return
	}

	attWithTopic := append(
		op.fixedAttrs,
		attribute.String("messaging.destination.name", *msg.TopicPartition.Topic),
		attribute.String("messaging.consumer.group.name", group),
		attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)),
		attribute.String("messaging.operation.name", "commit"),
		attribute.String("messaging.operation.type", "settle"),
		attribute.String("messaging.kafka.message.offset", strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)),
	)

	_, span := op.tracer.Start(
		op.Context(msg),
		*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))
	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(attribute.String("messaging.message.id", spanContext.SpanID().String()))

	setSpanAttributes(spanContext, msg)
}

// Context
// get span context from kafka message headers
func (op *otelProvider) Context(msg *kafka.Message) context.Context {
	ctx := context.Background()
	if msg == nil {
		return ctx
	}
	roottraceid := ""
	rootspanid := ""
	issampled := trace.TraceFlags(0)

	for _, h := range msg.Headers {
		switch h.Key {
		case TraceHeaderName:
			roottraceid = string(h.Value)
		case SpanHeaderName:
			rootspanid = string(h.Value)
		case SampledHeaderName:
			issampled = trace.FlagsSampled
		}
	}

	if len(roottraceid) == 0 || len(rootspanid) == 0 {
		return ctx
	}

	traceid, err := trace.TraceIDFromHex(roottraceid)
	if err != nil {
		return ctx
	}
	spanid, err := trace.SpanIDFromHex(rootspanid)
	if err != nil {
		return ctx
	}

	ctx = trace.ContextWithRemoteSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceid,
			SpanID:     spanid,
			TraceFlags: issampled,
		}),
	)

	return ctx
}

// setSpanAttributes
// setting partial tracing headers to create child span.
func setSpanAttributes(spanContext trace.SpanContext, msg *kafka.Message) {
	if msg == nil {
		return
	}

	// remove existing partial tracing headers if exists
	noTraceHeaders := msg.Headers[:0]
	for _, h := range msg.Headers {
		if h.Key != TraceHeaderName && h.Key != SpanHeaderName && h.Key != SampledHeaderName {
			noTraceHeaders = append(noTraceHeaders, h)
		}
	}
	traceHeaders := []kafka.Header{
		{Key: TraceHeaderName, Value: []byte(spanContext.TraceID().String())},
		{Key: SpanHeaderName, Value: []byte(spanContext.SpanID().String())},
	}
	if spanContext.IsSampled() {
		traceHeaders = append(traceHeaders, kafka.Header{Key: SampledHeaderName, Value: []byte("true")})
	}
	msg.Headers = append(noTraceHeaders, traceHeaders...)
	return
}
