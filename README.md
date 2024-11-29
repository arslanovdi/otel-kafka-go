## OpenTelemetry instrumentation for [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

Библиотека реализует трассировку сообщений в/из Kafka с сохранением контекста.

### Install
Manual install:
```bash
go get -u github.com/arslanovdi/otel-kafka-go
```

Golang import:

```go
import otelkafka "github.com/arslanovdi/otel-kafka-go"
```

### Usage

В приложении должен быть зарегистрирован глобальный провайдер трассировки. 
Например экспортер в jaeger.
```go
import (
    otelkafka "github.com/arslanovdi/otel-kafka-go"
)
const jaeger_address = "127.0.0.1:4317"
jaeger, err := otelkafka.NewProvider(context.Background(), "service_unique_id", jaeger_address)
```

Вся информация о контексте трассировки сохраняется в заголовки kafka сообщения. Соответственно контекст не прерывается от продюсера к консюмеру.

Интерфейс `OtelProvider` обединяет методы Producer и Consumer, использовать если трассировка требуется в обоих направлениях, чтобы не множить сущности.

#### Producer
Инициализация провайдера трассировки
```go
import (
    otelkafka "github.com/arslanovdi/otel-kafka-go"
)
trace := otelkafka.NewOtelProducer("service_unique_id")
```

OtelProducer представлен методом:

```go
OnSend(ctx context.Context, msg *kafka.Message)
```
, где ctx - контекст с родительским trace span.
msg - сообщение, которое отправляется в кафку.

Метод `OnSend` вызывать до отправки сообщения в кафку, т.к. он создает новый trace span и интегрирует его в сообщение.

#### Consumer
Инициализация провайдера трассировки
```go
import (
    otelkafka "github.com/arslanovdi/otel-kafka-go"
)

trace := otelkafka.NewOtelConsumer("service_unique_id")
```
Методы OtelConsumer:
```go
OnPoll(msg *kafka.Message, group string)
OnProcess(msg *kafka.Message, group string)
OnCommit(msg *kafka.Message, group string)
```

В методы дополнительно передается consumer group, т.к. в сообщении оно не хранится, а по стандарту OpenTelemetry это поле рекомендуется передавать.

Метод `OnPoll` вызывать после получения сообщения через Poll или ReadMessage. Не меняет родительский span в сообщении.

Метод `OnProcess` вызывать перед началом обработки сообщения. Меняет родительский span в сообщении.

Метод `OnCommit` вызывать после коммита сообщения. Span является дочерним по отношению к OnProcess.

#### Общие методы
Метод `Context` возвращает контекст с trace span, извлеченный из kafka сообщения. Для трассировки контекста выполнения в следующих сервисах.
Метод `SetSpanAttributes` интегрирует trace span в kafka сообщение.

### Если мы получаем сообщение из kafka без root span.
Например, сообщение было отправлено в kafka неизвестной библиотекой.

Это приведет к тому что у OnPoll и OnProcess будут разные родители.

Как этого избежать:

До вызова метода OnPoll нужно создать новый root span и интегрировать его в msg.
```go
import (
    "go.opentelemetry.io/otel"
    oteltrace "go.opentelemetry.io/otel/trace"
    otelkafka "github.com/arslanovdi/otel-kafka-go"
)

trace := otelkafka.NewOtelConsumer("service_unique_id")

msg, _ := consumer.ReadMessage(-1)                                  // получаем сообщение

if otelkafka.Context(msg)==context.Background() {                   // если нет root span, функция вернет context.Background()
    tracer := otel.GetTracerProvider().Tracer("service name",       // получили глобальный провайдер трассировки
        oteltrace.WithSchemaURL(semconv.SchemaURL),
    )
    
    _, span := tracer.Start(context.Background(), "root span name") // генерирует span
    
    otelkafka.SetSpanAttributes(span.SpanContext(), &msg)            // интегрирует span в msg
}

trace.OnPoll(&msg, "group")

trace.OnProcess(&msg, "group")

trace.OnCommit(&msg, "group")
```

### Example

[пример использования библиотеки](https://github.com/arslanovdi/otel-kafka-go/tree/master/example)

## Documentation
[OpenTelemetry API & SDKs for go](https://opentelemetry.io/docs/languages/go/)

[Semantic Conventions for Kafka](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md)