using EventFlux.RedisFlow.Abstractions;
using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Tests.Fakes;
using EventFlux.RedisFlow.Tests.Support;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventFlux.RedisFlow.Tests.NewApi
{
    public class TypedPublishTests
    {
        private sealed class StringOnlyPublisher : IRedisStreamPublisher
        {
            public List<(string Type, string Payload)> Published { get; } = new List<(string, string)>();

            public Task PublishAsync(string eventType, string payload)
            {
                Published.Add((eventType, payload));
                return Task.CompletedTask;
            }

            public Task PublishAsync(string eventType, string payload, EventContext context)
            {
                Published.Add((eventType, payload));
                return Task.CompletedTask;
            }
        }

        private static OrderPlaced Sample() => new OrderPlaced { OrderId = 9, Customer = "linus", Total = 99.95m, Lines = new List<string> { "x", "y" } };

        [Fact]
        public async Task TypedPublish_IsHandledAsSameType()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            var publisher = new RedisStreamPublisher(h.Fake.Multiplexer, Microsoft.Extensions.Options.Options.Create(h.Options));

            await publisher.PublishAsync(Sample());
            await h.RunUntilAsync(() => Probe.Calls == 1);

            var order = Assert.IsType<OrderPlaced>(Assert.Single(Probe.Received));
            Assert.Equal(9, order.OrderId);
            Assert.Equal("linus", order.Customer);
            Assert.Equal(99.95m, order.Total);
            Assert.Equal(new[] { "x", "y" }, order.Lines);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
        }

        [Fact]
        public async Task TypedPublish_WritesTypeNameResolvedByWorker()
        {
            var fake = FakeStreamDatabase.Create();
            IRedisStreamPublisher publisher = new RedisStreamPublisher(fake.Multiplexer, Microsoft.Extensions.Options.Options.Create(new RedisStreamOptions { StreamName = "orders" }));

            await publisher.PublishAsync(Sample());

            var entry = Assert.Single(fake.Entries("orders"));
            Assert.Equal(new[] { "type", "data" }, entry.Values.Select(v => v.Name.ToString()));
            var type = entry.Values[0].Value.ToString();
            Assert.Equal("OrderPlaced", type);
            Assert.Equal(typeof(OrderPlaced), EventTypeResolver.Resolve(type));
        }

        [Fact]
        public async Task InterfaceDefault_WritesSameTypeAndPayloadAsPublisher()
        {
            var fake = FakeStreamDatabase.Create();
            var publisher = new RedisStreamPublisher(fake.Multiplexer, Microsoft.Extensions.Options.Options.Create(new RedisStreamOptions { StreamName = "orders" }));
            var custom = new StringOnlyPublisher();

            await publisher.PublishAsync(Sample());
            await ((IRedisStreamPublisher)custom).PublishAsync(Sample());

            var entry = Assert.Single(fake.Entries("orders"));
            var (type, payload) = Assert.Single(custom.Published);
            Assert.Equal(entry.Values[0].Value.ToString(), type);
            Assert.Equal(entry.Values[1].Value.ToString(), payload);
        }

        [Fact]
        public async Task TypedPublish_UsesRuntimeTypeOfEvent()
        {
            var custom = new StringOnlyPublisher();
            EventFlux.Abstractions.IEventRequest request = new PaymentFailed { Reason = "r" };

            await ((IRedisStreamPublisher)custom).PublishAsync(request);

            Assert.Equal("PaymentFailed", Assert.Single(custom.Published).Type);
        }

        [Fact]
        public async Task TypedPublish_Null_Throws()
        {
            var fake = FakeStreamDatabase.Create();
            var publisher = new RedisStreamPublisher(fake.Multiplexer, Microsoft.Extensions.Options.Options.Create(new RedisStreamOptions()));

            await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync<OrderPlaced>(null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => ((IRedisStreamPublisher)new StringOnlyPublisher()).PublishAsync<OrderPlaced>(null!));
        }

        [Fact]
        public async Task TypedPublish_CancelledToken_ThrowsAndWritesNothing()
        {
            var fake = FakeStreamDatabase.Create();
            var publisher = new RedisStreamPublisher(fake.Multiplexer, Microsoft.Extensions.Options.Options.Create(new RedisStreamOptions { StreamName = "orders" }));
            var custom = new StringOnlyPublisher();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publisher.PublishAsync(Sample(), new CancellationToken(true)));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ((IRedisStreamPublisher)custom).PublishAsync(Sample(), new CancellationToken(true)));

            Assert.Empty(fake.Entries("orders"));
            Assert.Empty(custom.Published);
        }
    }
}
