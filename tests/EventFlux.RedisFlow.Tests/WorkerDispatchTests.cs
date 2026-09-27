using EventFlux.RedisFlow.Events;
using EventFlux.RedisFlow.Tests.Support;
using Newtonsoft.Json;
using StackExchange.Redis;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class WorkerDispatchTests
    {
        [Fact]
        public async Task PublishedEntry_IsHandledOnce_ThenAcknowledgedAndDeleted()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Fake.CreateGroup(WorkerHarness.Stream, WorkerHarness.Group);
            h.Publish(nameof(OrderPlaced), JsonConvert.SerializeObject(new OrderPlaced { OrderId = 7, Customer = "ada" }));

            await h.RunUntilAsync(() => h.Fake.Entries(WorkerHarness.Stream).Count == 0);

            var order = Assert.IsType<OrderPlaced>(Assert.Single(Probe.Received));
            Assert.Equal(7, order.OrderId);
            Assert.Equal("ada", order.Customer);
            Assert.Equal(1, Probe.Calls);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
            Assert.Empty(h.Fake.Entries(WorkerHarness.Stream));
        }

        [Fact]
        public async Task Handler_ReceivesCancellableToken_NotCancelledByGracefulStop()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Publish(nameof(OrderPlaced), JsonConvert.SerializeObject(new OrderPlaced { OrderId = 1 }));

            await h.RunUntilAsync(() => Probe.Calls == 1);

            var token = Assert.Single(Probe.Tokens);
            Assert.True(token.CanBeCanceled);
            Assert.False(token.IsCancellationRequested);
        }

        [Fact]
        public async Task Handler_SeesVersionAndConfigsFromEntry()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            var id = h.Publish(
                nameof(OrderPlaced),
                JsonConvert.SerializeObject(new OrderPlaced { OrderId = 2 }),
                new NameValueEntry("version", "1.2.3"),
                new NameValueEntry("configs", "{\"TenantId\":\"1001\"}"));

            await h.RunUntilAsync(() => Probe.Calls == 1);

            var ctx = Assert.Single(Probe.Contexts);
            Assert.NotNull(ctx);
            Assert.Equal(id, ctx!.StreamId);
            Assert.Equal("1.2.3", ctx.Version);
            Assert.Equal("1001", ctx.Configs["TenantId"]);
        }

        [Fact]
        public async Task UnknownEventType_IsDispatchedAsGenericEvent_AndAcknowledged()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Publish("ShipmentDelayedSomewhereElse", "{\"x\":1}");

            await h.RunUntilAsync(() => h.Fake.Entries(WorkerHarness.Stream).Count == 0);

            var generic = Assert.IsType<GenericEvent>(Assert.Single(Probe.Received));
            Assert.Equal("ShipmentDelayedSomewhereElse", generic.EventName);
            Assert.Equal("{\"x\":1}", generic.RawPayload);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
        }
    }
}
