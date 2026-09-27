using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.RedisFlow.Abstractions;
using EventFlux.RedisFlow.Events;

namespace EventFlux.RedisFlow.Tests.Support
{
    public static class Probe
    {
        public static ConcurrentQueue<object> Received { get; } = new ConcurrentQueue<object>();

        public static ConcurrentQueue<CancellationToken> Tokens { get; } = new ConcurrentQueue<CancellationToken>();

        public static ConcurrentQueue<EventContext?> Contexts { get; } = new ConcurrentQueue<EventContext?>();

        public static int Calls;

        public static int FailuresLeft;

        public static bool AlwaysFail;

        public static void Reset(int failures = 0, bool alwaysFail = false)
        {
            AlwaysFail = alwaysFail;
            Received.Clear();
            Tokens.Clear();
            Contexts.Clear();
            Calls = 0;
            FailuresLeft = failures;
        }
    }

    public class OrderPlaced : IEventRequest
    {
        public int OrderId { get; set; }

        public string Customer { get; set; } = string.Empty;

        public decimal Total { get; set; }

        public List<string> Lines { get; set; } = new List<string>();
    }

    public class OrderPlacedHandler : IEventHandler<OrderPlaced>
    {
        private readonly IEventContextAccessor _accessor;

        public OrderPlacedHandler(IEventContextAccessor accessor)
        {
            _accessor = accessor;
        }

        public Task Handle(OrderPlaced request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Probe.Calls);
            Probe.Received.Enqueue(request);
            Probe.Tokens.Enqueue(cancellationToken);
            Probe.Contexts.Enqueue(_accessor.EventContext);
            return Task.CompletedTask;
        }
    }

    public class PaymentFailed : IEventRequest
    {
        public string Reason { get; set; } = string.Empty;
    }

    public class PaymentFailedHandler : IEventHandler<PaymentFailed>
    {
        public Task Handle(PaymentFailed request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Probe.Calls);
            if (Probe.AlwaysFail || Interlocked.Decrement(ref Probe.FailuresLeft) >= 0)
                throw new InvalidOperationException("handler failure " + request.Reason);
            Probe.Received.Enqueue(request);
            return Task.CompletedTask;
        }
    }

    public class SlowJob : IEventRequest
    {
        public int DelayMs { get; set; } = Timeout.Infinite;
    }

    public class SlowJobHandler : IEventHandler<SlowJob>
    {
        public async Task Handle(SlowJob request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Probe.Calls);
            Probe.Tokens.Enqueue(cancellationToken);
            await Task.Delay(request.DelayMs, cancellationToken);
            Probe.Received.Enqueue(request);
        }
    }

    public class GenericEventHandler : IEventHandler<GenericEvent>
    {
        public Task Handle(GenericEvent request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Probe.Calls);
            Probe.Received.Enqueue(request);
            return Task.CompletedTask;
        }
    }
}
