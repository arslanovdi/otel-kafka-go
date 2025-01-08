// Package otelkafkago
// OpenTelemetry instrumentation for confluent-kafka-go
package otelkafkago

import (
	"context"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
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
// - kafka producer instrumentation for tracing
type OtelProducer interface {
	// OnSend - message sending trace. Integrate tracing into msg headers
	OnSend(ctx context.Context, msg *kafka.Message)
}

// OtelConsumer
// - kafka consumer instrumentation for tracing
type OtelConsumer interface {
	// OnPoll - trace poll/read message
	OnPoll(msg *kafka.Message, group string)
	// OnProcess - trace message processing. Updates trace information in message headers.
	OnProcess(msg *kafka.Message, group string)
	// OnCommit - trace commit message. Updates trace information in message headers.
	OnCommit(msg *kafka.Message, group string)
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
		semconv.MessagingClientID(instance), // messaging.client.id
		semconv.MessagingSystemKafka,        // messaging.system set kafka
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
// - initialization OtelProducer.
// Global trace provider must be initialized.
func NewOtelProducer(instance string) OtelProducer {
	op := otelProvider{}
	op.new(instance)
	return &op
}

// NewOtelConsumer
// - initialization OtelConsumer.
// Global trace provider must be initialized.
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
	if msg.TopicPartition.Topic == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var attWithTopic []attribute.KeyValue
	copy(attWithTopic, op.fixedAttrs)

	attWithTopic = append(
		attWithTopic,
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),                                         // messaging.destination.name
		semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)), // messaging.destination.partition.id
		semconv.MessagingOperationName("send"),                                                              // messaging.operation.name
		semconv.MessagingOperationTypeKey.String("send"),                                                    // messaging.operation.type = settle
	)

	if len(msg.Key) > 0 { //  If the key is null, the attribute MUST NOT be set
		attWithTopic = append(attWithTopic, semconv.MessagingKafkaMessageKey(string(msg.Key))) // messaging.kafka.message.key
	}

	_, span := op.tracer.Start(
		ctx,
		"send "+*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))

	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(semconv.MessagingMessageID(spanContext.SpanID().String())) // messaging.message.id

	SetSpanAttributes(spanContext, msg) // update tracing info in msg headers.
}

// OnPoll
// tracing poll(read) message.
func (op *otelProvider) OnPoll(msg *kafka.Message, group string) {
	if msg == nil {
		return
	}
	if msg.TopicPartition.Topic == nil {
		return
	}

	var attWithTopic []attribute.KeyValue
	copy(attWithTopic, op.fixedAttrs)

	attWithTopic = append(
		attWithTopic,
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),                                         // messaging.destination.name
		semconv.MessagingConsumerGroupName(group),                                                           // messaging.consumer.group.name
		semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)), // messaging.destination.partition.id
		semconv.MessagingOperationName("poll"),                                                              // messaging.operation.name
		semconv.MessagingOperationTypeReceive,                                                               // // messaging.operation.type = receive
		semconv.MessagingKafkaOffset(int(msg.TopicPartition.Offset)),                                        // messaging.kafka.offset
	)

	if len(msg.Key) > 0 { //  If the key is null, the attribute MUST NOT be set
		attWithTopic = append(attWithTopic, semconv.MessagingKafkaMessageKey(string(msg.Key))) // messaging.kafka.message.key
	}

	_, span := op.tracer.Start(
		Context(msg),
		"poll "+*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))
	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(semconv.MessagingMessageID(spanContext.SpanID().String())) // messaging.message.id
}

// OnProcess
// tracing processing message.
// update tracing info in msg headers.
func (op *otelProvider) OnProcess(msg *kafka.Message, group string) {
	if msg == nil {
		return
	}
	if msg.TopicPartition.Topic == nil {
		return
	}

	var attWithTopic []attribute.KeyValue
	copy(attWithTopic, op.fixedAttrs)

	attWithTopic = append(
		attWithTopic,
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),                                         // messaging.destination.name
		semconv.MessagingConsumerGroupName(group),                                                           // messaging.consumer.group.name
		semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)), // messaging.destination.partition.id
		semconv.MessagingOperationName("process"),                                                           // messaging.operation.name
		semconv.MessagingOperationTypeProcess,                                                               // messaging.operation.type = process
		semconv.MessagingKafkaOffset(int(msg.TopicPartition.Offset)),                                        // messaging.kafka.offset
	)

	if len(msg.Key) > 0 { //  If the key is null, the attribute MUST NOT be set
		attWithTopic = append(attWithTopic, semconv.MessagingKafkaMessageKey(string(msg.Key))) // messaging.kafka.message.key
	}

	_, span := op.tracer.Start(
		Context(msg),
		"process "+*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))
	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(semconv.MessagingMessageID(spanContext.SpanID().String())) // messaging.message.id

	SetSpanAttributes(spanContext, msg)
}

// OnCommit
// tracing commit message.
// update tracing info in msg headers.
func (op *otelProvider) OnCommit(msg *kafka.Message, group string) {
	if msg == nil {
		return
	}
	if msg.TopicPartition.Topic == nil {
		return
	}

	var attWithTopic []attribute.KeyValue
	copy(attWithTopic, op.fixedAttrs)

	attWithTopic = append(
		attWithTopic,
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),                                         // messaging.destination.name
		semconv.MessagingConsumerGroupName(group),                                                           // messaging.consumer.group.name
		semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(msg.TopicPartition.Partition), 10)), // messaging.destination.partition.id
		semconv.MessagingOperationName("commit"),                                                            // messaging.operation.name
		semconv.MessagingOperationTypeSettle,                                                                // messaging.operation.type = settle
		semconv.MessagingKafkaOffset(int(msg.TopicPartition.Offset)),                                        // messaging.kafka.offset
	)

	_, span := op.tracer.Start(
		Context(msg),
		"commit "+*msg.TopicPartition.Topic,
		trace.WithAttributes(attWithTopic...))
	defer span.End()
	spanContext := span.SpanContext()

	span.SetAttributes(semconv.MessagingMessageID(spanContext.SpanID().String())) // messaging.message.id

	SetSpanAttributes(spanContext, msg)
}

// Context
// get span context from kafka message headers
func Context(msg *kafka.Message) context.Context {
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

	if roottraceid == "" || rootspanid == "" {
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

// SetSpanAttributes
// setting partial tracing headers to create child span.
func SetSpanAttributes(spanContext trace.SpanContext, msg *kafka.Message) {
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
}
