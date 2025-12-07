**EventFlux.RedisFlow**

Lightweight helper library for publishing and consuming EventFlux events via Redis Streams.
This library provides a small opinionated host integration: a Redis stream publisher, a background worker
that reads stream entries, resolves event types, publishes to EventFlux's IEventBus and invokes local handlers.

**Features**
- **Publish to Redis streams**: `IRedisStreamPublisher.PublishAsync(eventType, payload)` helper.
- **Background consumer worker**: `RedisStreamWorker` reads Redis streams, resolves event types, and publishes to EventFlux.
- **Automatic handler registration**: `AddRedisEventQueue(...)` scans assemblies you pass and registers `IEventHandler<T>` implementations.
- **Flexible type resolution**: resolves registered types by name and falls back to a `GenericEvent` when the concrete type isn't available.
- **Consumer group per instance option**: append machine-name or GUID to `ConsumerGroup` to allow multiple independent consumers to process the same stream.
- **Ack + optional delete**: worker acknowledges processed entries and (optionally) deletes them to avoid reprocessing on restarts.
- **Startup diagnostics**: logs discovered event types and handler registrations to help debugging.

**Quick Start**

1. Add a project reference to the library (or package):

   - In your API project the NuGet package is EvenFlux.RedisFlow.

2. Configure Redis stream settings in `appsettings.json`:

```json
{
  "RedisStream": {
    "ConnectionString": "localhost:6379",
    "StreamName": "event-stream",
    "ConsumerGroup": "event-group",
    "ConsumerName": "consumer-1",
    "BatchSize": 10
  }
}
```

3. Register the integration in `Program.cs`:

```csharp
// Register and scan your assembly for event types and handlers
builder.Services.AddRedisEventQueue(
    builder.Configuration,
    assemblies: new[] { typeof(Program).Assembly },
    appendGuidToConsumerGroup: true // optional: make group unique per instance
);
```

4. Publish from any project that references the library by using `IRedisStreamPublisher`:

```csharp
app.MapPost("/publish", async (MyEvent req, IRedisStreamPublisher publisher) =>
{
    var payload = JsonConvert.SerializeObject(req);
    await publisher.PublishAsync(nameof(MyEvent), payload);
    return Results.Ok();
});
```

5. In consumer projects define your event and handler types locally:

```csharp
public class MyEvent : IEventRequest { public string Data { get; set; } }

public class MyEventHandler : IEventHandler<MyEvent>
{
    public async Task Handle(MyEvent request)
    {
        // handle
    }
}
```

The library's `AddRedisEventQueue(...)` will scan the assembly you pass and register `IEventHandler<T>` implementations automatically.

**Behavior and important details**

- **Type resolution**: the worker reads `type` and `data` fields from Redis stream entries. It attempts to resolve the `type` name to a CLR type using registered event types (from assemblies you passed). If not found, it publishes a `GenericEvent` (with `EventName` and `RawPayload`).

- **Handler dispatch**: EventFlux's `IEventBus` dispatches the event object. The handler must be registered in DI as `IEventHandler<T>` for the concrete `T` used by the publisher; `AddRedisEventQueue` auto-registers handlers found in scanned assemblies.

- **Consumer groups**: Redis consumer groups distribute messages across group members. If multiple services use the same `ConsumerGroup`, messages will be load-balanced (each message delivered to only one member). If you want multiple independent services to receive the same messages, give each service a distinct `ConsumerGroup` (use `appendGuidToConsumerGroup` or set unique `ConsumerGroup` values per app).

- **Acknowledgement and deletion**: the worker acknowledges processed entries with `XACK`. The library also deletes entries from the stream after ack to avoid reprocessing on restart. If you prefer to keep entries for audit, you can modify the worker to skip deletion and rely on XACK only.

**Advanced options**

- `AddRedisEventQueue(..., appendMachineNameToConsumerGroup: true)` — append the machine name to `ConsumerGroup` so each instance has a per-host group.
- `AddRedisEventQueue(..., appendGuidToConsumerGroup: true)` — append a GUID to `ConsumerGroup` so each instance is independent (useful for local testing).
- Handler lifetime: by default handlers are registered as `Transient`. If handlers need scoped services, change registration to `AddScoped` in `ServiceCollectionExtensions`.