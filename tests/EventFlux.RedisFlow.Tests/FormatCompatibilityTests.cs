using EventFlux.RedisFlow.Abstractions;
using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Tests.Fakes;
using EventFlux.RedisFlow.Tests.Support;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class FormatCompatibilityTests
    {
        private static string[] Flatten(StreamEntry entry) =>
            entry.Values.SelectMany(v => new[] { v.Name.ToString(), v.Value.ToString() }).ToArray();

        [Fact]
        public async Task EntryWrittenByVersion102_IsHandledWithContext()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Publish(
                "OrderPlaced",
                "{\"OrderId\":42,\"Customer\":\"grace\",\"Total\":12.5,\"Lines\":[\"a\",\"b\"]}",
                new NameValueEntry("version", "1.2.3"),
                new NameValueEntry("configs", "{\"TenantId\":\"1001\",\"Source\":\"ContextAPI\"}"));

            await h.RunUntilAsync(() => Probe.Calls == 1);

            var order = Assert.IsType<OrderPlaced>(Assert.Single(Probe.Received));
            Assert.Equal(42, order.OrderId);
            Assert.Equal("grace", order.Customer);
            Assert.Equal(12.5m, order.Total);
            Assert.Equal(new[] { "a", "b" }, order.Lines);
            var ctx = Assert.Single(Probe.Contexts)!;
            Assert.Equal("1.2.3", ctx.Version);
            Assert.Equal("ContextAPI", ctx.Configs["Source"]);
        }

        [Fact]
        public async Task StringPublish_WritesSameFieldsAsVersion102()
        {
            var fake = FakeStreamDatabase.Create();
            var publisher = new RedisStreamPublisher(fake.Multiplexer, Microsoft.Extensions.Options.Options.Create(new RedisStreamOptions { StreamName = "orders" }));

            await publisher.PublishAsync("OrderPlaced", "{\"OrderId\":1}");
            await publisher.PublishAsync("OrderPlaced", "{\"OrderId\":2}", new EventContext
            {
                Version = "1.2.3",
                Configs = new Dictionary<string, string> { ["TenantId"] = "1001", ["Source"] = "ContextAPI" }
            });
            await publisher.PublishAsync("OrderPlaced", "{\"OrderId\":3}", new EventContext());

            var entries = fake.Entries("orders");
            Assert.Equal(new[] { "type", "OrderPlaced", "data", "{\"OrderId\":1}" }, Flatten(entries[0]));
            Assert.Equal(
                new[] { "type", "OrderPlaced", "data", "{\"OrderId\":2}", "version", "1.2.3", "configs", "{\"TenantId\":\"1001\",\"Source\":\"ContextAPI\"}" },
                Flatten(entries[1]));
            Assert.Equal(new[] { "type", "OrderPlaced", "data", "{\"OrderId\":3}" }, Flatten(entries[2]));
        }
    }
}
