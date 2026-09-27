## NuGet Package Information

| Package | Downloads | License |
|---------|-----------|---------|
| [![NuGet](https://img.shields.io/nuget/v/EventFlux.RedisFlow)](https://www.nuget.org/packages/EventFlux.RedisFlow) | [![Downloads](https://img.shields.io/nuget/dt/EventFlux.RedisFlow)](https://www.nuget.org/packages/EventFlux.RedisFlow) | [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/kadirdemirkaya/EventFlux.RedisFlow/blob/main/LICENSE.txt) |


**EventFlux.RedisFlow**

Lightweight helper library for publishing and consuming [EventFlux](https://www.nuget.org/packages/EventFlux) events via Redis Streams.
It provides a small opinionated host integration: a Redis stream publisher, and a background worker that reads stream
entries, resolves event types, publishes them to EventFlux's `IEventBus` and so invokes your local handlers.

**Features**
- **Publish to Redis streams**: `IRedisStreamPublisher.PublishAsync(@event)` for typed events, or `PublishAsync(eventType, payload)` for raw JSON.
- **Background consumer worker**: `RedisStreamWorker` reads the stream through a consumer group, resolves event types and publishes them to EventFlux.
- **Automatic handler registration**: `AddRedisEventQueue(...)` forwards the assemblies you pass to EventFlux's `AddEventBus` and registers their `IEventHandler<T>` implementations.
- **Retry and dead-letter**: a failed entry is not acknowledged; it is retried and, after a configurable number of deliveries, moved to a dead-letter stream.
- **Survives Redis outages**: the worker keeps running while Redis is unavailable, retries with a backoff and recreates a deleted consumer group; the application also starts while Redis is down.
- **Safe fan-out**: processed entries are deleted only after every consumer group of the stream has processed them, so each group receives every entry.
- **Flexible type resolution**: resolves registered types by name and falls back to a `GenericEvent` when the concrete type isn't available.
- **Consumer group per instance option**: append machine-name or GUID to `ConsumerGroup` to allow multiple independent consumers to process the same stream.
- **Startup diagnostics**: logs discovered event types and handler registrations to help debugging.

**Requirements**

- .NET 8.0, 9.0 or 10.0
- EventFlux 2.1.1 or later (installed as a dependency)
- Redis 5.0 or later (Redis Streams and consumer groups)

**Quick Start**

1. Install the package:

```powershell
dotnet add package EventFlux.RedisFlow --version 1.1.0
```

Or via `<PackageReference>` in your `.csproj`:

```xml
<PackageReference Include="EventFlux.RedisFlow" Version="1.1.0" />
```

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
app.MapPost("/publish", async (MyEvent req, IRedisStreamPublisher publisher, CancellationToken ct) =>
{
    await publisher.PublishAsync(req, ct);
    return Results.Ok();
});
```

`PublishAsync(@event)` writes the event's type name and JSON payload exactly as the worker reads them, so the event
reaches handlers as the same type. The string overloads are still available when you already have a serialized payload:

```csharp
await publisher.PublishAsync(nameof(MyEvent), payloadJson);
```

5. In consumer projects define your event and handler types locally. Handlers use the EventFlux 2.x signature with a
`CancellationToken`:

```csharp
public class MyEvent : IEventRequest { public string Data { get; set; } }

public class MyEventHandler : IEventHandler<MyEvent>
{
    public Task Handle(MyEvent request, CancellationToken cancellationToken = default)
    {
        // handle
        return Task.CompletedTask;
    }
}
```

6. Access event context inside your event handlers:

- If your producer sends an event context (version, configs, tenant info, etc.), you can access it inside your handler using `IEventContextAccessor`.

```csharp
await publisher.PublishAsync(nameof(PublishEventRequest), payload, new EventContext
{
    Version = "1.2.3",
    Configs = new Dictionary<string, string> { ["TenantId"] = "1001" }
});
```

```csharp
public class PublishEventHandler : IEventHandler<PublishEventRequest>
{
    private readonly IEventContextAccessor _contextAccessor;

    public PublishEventHandler(IEventContextAccessor contextAccessor)
    {
        _contextAccessor = contextAccessor;
    }

    public Task Handle(PublishEventRequest request, CancellationToken cancellationToken = default)
    {
        var context = _contextAccessor.EventContext;

        if (context != null)
        {
            Console.WriteLine($"Context Version: {context.Version}");

            if (context.Configs != null)
            {
                foreach (var cfg in context.Configs)
                {
                    Console.WriteLine($"{cfg.Key}: {cfg.Value}");
                }
            }
        }

        // handle event logic
        return Task.CompletedTask;
    }
}
```

**Registration with EventFlux**

- `AddRedisEventQueue(configuration, assemblies)` calls EventFlux's `AddEventBus` with the assemblies you pass, so you
  don't need a separate `AddEventBus` call for them. Handlers are registered once, even if you also call `AddEventBus`
  yourself before or after.
- `AddRedisEventQueue(configuration)` without assemblies registers `IEventBus` but no handlers from your application.
  Register them with your own `builder.Services.AddEventBus<Program>()` call, before or after `AddRedisEventQueue`.
- If you configure `EventFluxOptions` (for example `HandlerLifetime`), call `AddEventBus(options => ..., assemblies)`
  **before** `AddRedisEventQueue`, so that your handler lifetime is the one registered.

**Error handling**

- A handler that completes without an exception acknowledges the entry (`XACK`). The entry is then deleted from the
  stream (`XDEL`) once every consumer group of the stream has read and acknowledged it; see "Acknowledgement and deletion".
- A handler that throws does **not** acknowledge the entry. It stays pending in the consumer group.
- Once an entry has been pending for `RetryIdleTime`, the worker claims it (`XPENDING` + `XCLAIM`) and runs the handler
  again. This also picks up entries left pending by a consumer that crashed or was stopped.
- When an entry has been delivered `MaxDeliveryAttempts` times, it is copied to the dead-letter stream and acknowledged.
  The dead-letter entry keeps all original fields (`type`, `data`, `version`, `configs`) and adds
  `dead-letter-source-stream`, `dead-letter-source-id`, `dead-letter-consumer-group`, `dead-letter-delivery-count` and
  `dead-letter-at`. To replay it, add its `type`/`data` (and optional `version`/`configs`) fields back to the source stream.
- If writing to the dead-letter stream fails, the original entry is **not** acknowledged and stays pending, so it is not lost.
- Entries whose type cannot be resolved or whose payload cannot be deserialized are dispatched as `GenericEvent`
  (`EventName`, `RawPayload`) and acknowledged, as in 1.0.x.
- Delivery is at-least-once: a handler can run more than once for the same entry (after a failure, or after a crash
  before the acknowledgement), so make handlers idempotent.
- On shutdown, the handler that is running is allowed to finish and its entry is acknowledged. The handler's
  `CancellationToken` is cancelled only when the host's shutdown timeout (`HostOptions.ShutdownTimeout`) expires; the
  entry then stays pending and is retried after the next start.

**Redis outages and connection**

- The application starts even when Redis is not reachable yet. Unless your connection string sets `abortConnect`
  explicitly, the library connects with `abortConnect=false`, so the connection is retried in the background. Set
  `abortConnect=true` in `ConnectionString` if you prefer the application to fail at startup when Redis is down.
- While Redis is unavailable, the worker logs the error and retries after 1, 2, 4, 8, 16 and then every 30 seconds. It
  does not stop and it does not stop the host. Consumption resumes when Redis is back.
- If the stream or the consumer group is deleted while the worker runs, the worker recreates the group (reading from the
  start of the stream) and continues.
- If an acknowledgement fails because the connection dropped after the handler finished, the entry stays pending and is
  handled again after `RetryIdleTime` (at-least-once delivery).
- `IRedisStreamPublisher.PublishAsync` throws `RedisConnectionException` / `RedisTimeoutException` while Redis is
  unavailable; retry or handle it in the calling code.

**Options (`RedisStream` section)**

| Option | Default | Description |
|---|---|---|
| `ConnectionString` | `""` | Redis connection string. `abortConnect=false` is added unless you set `abortConnect` yourself. |
| `StreamName` | `event-stream` | Stream to publish to and consume from. |
| `ConsumerGroup` | `event-group` | Consumer group of this service. |
| `ConsumerName` | machine name | Consumer name inside the group. Use a distinct name per instance. |
| `BatchSize` | `10` | Entries read per call. The worker reads the next batch right away while entries are queued and pauses 100 ms only when the stream is empty. |
| `EnableRetry` | `true` | Retry failed and abandoned pending entries. `false` leaves a failed entry pending without retrying it. |
| `MaxDeliveryAttempts` | `5` | Deliveries after which an entry is moved to the dead-letter stream. `0` or less retries without limit. |
| `RetryIdleTime` | `00:00:30` | How long an entry must be pending before it is retried. Keep it longer than your slowest handler. |
| `DeadLetterStreamName` | `{StreamName}-dead-letter` | Stream that receives entries over `MaxDeliveryAttempts`. |
| `DeleteProcessedEntries` | `true` | Delete entries that every consumer group has acknowledged. `false` keeps all entries in the stream (for example for audit); trim the stream yourself then. |

**Message format**

Each event is one stream entry with these fields. The format is the same in 1.0.x and 1.1.0, so both versions can read
each other's entries.

| Field | Content |
|---|---|
| `type` | Event type name (`Type.Name`, e.g. `MyEvent`). |
| `data` | Event serialized as JSON. |
| `version` | Optional, `EventContext.Version`. |
| `configs` | Optional, `EventContext.Configs` as a JSON object. |

**Behavior and important details**

- **Type resolution**: the worker reads `type` and `data` fields from Redis stream entries. It attempts to resolve the `type` name to a CLR type using registered event types (from assemblies you passed). If not found, it publishes a `GenericEvent` (with `EventName` and `RawPayload`).

- **Handler dispatch**: EventFlux's `IEventBus` dispatches the event object. The handler must be registered in DI as `IEventHandler<T>` for the concrete `T` used by the publisher; `AddRedisEventQueue` registers handlers found in the assemblies you pass.

- **Consumer groups**: Redis consumer groups distribute messages across group members. If multiple services use the same `ConsumerGroup`, messages will be load-balanced (each message delivered to only one member). If you want multiple independent services to receive the same messages, give each service a distinct `ConsumerGroup` (use `appendGuidToConsumerGroup` or set unique `ConsumerGroup` values per app).

- **Acknowledgement and deletion**: the worker acknowledges processed entries with `XACK`. After each batch it checks the
  stream's consumer groups (`XINFO GROUPS`, `XPENDING`) and deletes (`XDEL`) the entries that every other group has
  already read and acknowledged. With a single consumer group this happens right after the acknowledgement, so the stream
  does not grow. With several groups the last group to acknowledge an entry deletes it. An entry is kept while any group
  has not read it yet or still has it pending, so a consumer group that no longer runs keeps its unread entries in the
  stream; remove such groups with `XGROUP DESTROY` (groups created with `appendGuidToConsumerGroup` are removed
  automatically). Every 30 seconds the worker also re-checks the oldest entries, so entries that were only kept for a
  group that has since been deleted are removed too. Entries that stay pending after a failure do not hold back the
  deletion of other entries.

**Advanced options**

- `AddRedisEventQueue(..., appendMachineNameToConsumerGroup: true)` — append the machine name to `ConsumerGroup` so each instance has a per-host group.
- `AddRedisEventQueue(..., appendGuidToConsumerGroup: true)` — append a GUID to `ConsumerGroup` so each instance is independent and receives every entry (useful for local testing and for per-instance notifications). The group belongs to that one instance:
  - When the instance stops, it deletes its consumer group. Entries that were still pending in it (for example failed ones waiting for a retry) are dropped with the group.
  - When an instance starts, it deletes groups named `{ConsumerGroup}-{32 hex digits}` whose consumers have all been idle for more than an hour, which is what an instance that crashed leaves behind.
  - A new instance reads the entries that are still in the stream, as in 1.0.x: entries published while no instance was running are handled by the next one.
- Handler lifetime follows EventFlux's `EventFluxOptions.HandlerLifetime` (default `Transient`); see "Registration with EventFlux".

**Upgrading from 1.0.x**

| Change | What to do |
|---|---|
| Requires EventFlux 2.1.1 or later | Upgrade `EventFlux` if you reference it directly. |
| Handler signature is `Handle(TEvent, CancellationToken)` (EventFlux 2.x) | Add `CancellationToken cancellationToken = default` to your handlers. |
| `net6.0` and `net7.0` are no longer targeted | Move to `net8.0` or later. |
| `Microsoft.Extensions.*` dependencies follow your target framework (8.x on `net8.0`) | Nothing, unless you relied on 9.x packages flowing in transitively on `net8.0`; then reference them directly. |
| A failing handler no longer loses the entry | 1.0.x acknowledged and deleted the entry even when its handler failed. 1.1.0 retries it and moves it to `{StreamName}-dead-letter` after 5 deliveries. Entries that were left pending by 1.0.x are retried after the upgrade. Set `EnableRetry` to `false` to keep failed entries pending without retrying. |
| Handlers may run more than once | Retries make delivery at-least-once; make handlers idempotent. |
| The application starts while Redis is down | 1.0.x failed at startup when Redis was not reachable. 1.1.0 starts and connects in the background. Add `abortConnect=true` to `ConnectionString` to keep the old behavior. |
| Entries are no longer deleted before other consumer groups read them | 1.0.x deleted each entry as soon as one group processed it, so other groups could miss it. 1.1.0 deletes it after every group has acknowledged it. A 1.0.x consumer still running on the same stream keeps deleting immediately, so upgrade all consumers of a fan-out stream. Remove consumer groups you no longer use, otherwise their unread entries stay in the stream. Groups left by 1.0.x instances with `appendGuidToConsumerGroup` are removed by 1.1.0 instances with the same `ConsumerGroup` once they have been idle for an hour. |
| New typed publish API | Optional: replace `PublishAsync(nameof(T), serializedJson)` with `PublishAsync(e)`; the stream entry is identical. |

The stream entry format did not change: 1.0.x and 1.1.0 publishers and consumers can run side by side on the same stream.
