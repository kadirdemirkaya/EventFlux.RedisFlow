using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Tests.Fakes;
using EventFlux.RedisFlow.Tests.Support;
using EventFlux.RedisFlow.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using StackExchange.Redis;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class ConsumerGroupLifecycleTests
    {
        private const string Stream = WorkerHarness.Stream;
        private const long StaleMs = 60 * 60 * 1000 + 1000;

        private sealed class Instance : IAsyncDisposable
        {
            private readonly ServiceProvider _provider;

            public Instance(FakeStreamDatabase fake, bool appendGuid, string group = "svc")
            {
                var configuration = new ConfigurationBuilder()
                    .AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        ["RedisStream:ConnectionString"] = "fake:6379",
                        ["RedisStream:StreamName"] = Stream,
                        ["RedisStream:ConsumerGroup"] = group,
                        ["RedisStream:ConsumerName"] = "worker-" + Guid.NewGuid().ToString("N"),
                    })
                    .Build();
                var services = new ServiceCollection();
                services.AddLogging();
                services.AddRedisEventQueue(configuration, new[] { typeof(OrderPlaced).Assembly }, appendGuidToConsumerGroup: appendGuid);
                services.AddSingleton(fake.Multiplexer);
                _provider = services.BuildServiceProvider();
                Worker = _provider.GetServices<IHostedService>().OfType<RedisStreamWorker>().Single();
                Group = _provider.GetRequiredService<IOptions<RedisStreamOptions>>().Value.ConsumerGroup;
            }

            public RedisStreamWorker Worker { get; }

            public string Group { get; }

            public async Task RunUntilAsync(Func<bool> condition, int timeoutMs = 5000)
            {
                await Worker.StartAsync(CancellationToken.None);
                await WorkerHarness.WaitUntilAsync(condition, timeoutMs);
                await Worker.StopAsync(CancellationToken.None);
            }

            public async ValueTask DisposeAsync()
            {
                await _provider.DisposeAsync();
            }
        }

        private static void Add(FakeStreamDatabase fake, int id) =>
            fake.Add(Stream, new NameValueEntry("type", nameof(OrderPlaced)), new NameValueEntry("data", JsonConvert.SerializeObject(new OrderPlaced { OrderId = id })));

        [Fact]
        public async Task AppendGuidToConsumerGroup_InstanceStop_DeletesItsGroup()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            await using var instance = new Instance(fake, appendGuid: true);
            Add(fake, 1);

            await instance.RunUntilAsync(() => Probe.Calls == 1);

            Assert.StartsWith("svc-", instance.Group);
            Assert.DoesNotContain(instance.Group, fake.Groups(Stream));
        }

        [Fact]
        public async Task FixedConsumerGroup_InstanceStop_KeepsGroup()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            await using var instance = new Instance(fake, appendGuid: false);
            Add(fake, 1);

            await instance.RunUntilAsync(() => Probe.Calls == 1);

            Assert.Contains("svc", fake.Groups(Stream));
        }

        [Fact]
        public async Task AppendGuidToConsumerGroup_Restarts_DoNotReplayOrGrowTheStream()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            var handledPerRun = new List<int>();

            for (var run = 0; run < 3; run++)
            {
                var before = Probe.Calls;
                await using (var instance = new Instance(fake, appendGuid: true))
                {
                    Add(fake, run * 10);
                    await instance.RunUntilAsync(() => fake.Entries(Stream).Count == 0 && Probe.Calls > before, 3000);
                }
                handledPerRun.Add(Probe.Calls - before);
                Add(fake, run * 10 + 1);
            }

            Assert.Equal(new[] { 1, 2, 2 }, handledPerRun);
            Assert.Empty(fake.Groups(Stream));
            Assert.Single(fake.Entries(Stream));
        }

        [Fact]
        public async Task AppendGuidToConsumerGroup_Start_DeletesStaleSiblingGroups_KeepsOthers()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            var stale = "svc-" + Guid.NewGuid().ToString("N");
            var active = "svc-" + Guid.NewGuid().ToString("N");
            fake.Touch(Stream, stale, "crashed");
            fake.Touch(Stream, "svc-reports", "idle");
            fake.Touch(Stream, "svc", "idle");
            fake.NowMs += StaleMs;
            fake.Touch(Stream, active, "alive");
            await using var instance = new Instance(fake, appendGuid: true);
            Add(fake, 1);

            await instance.RunUntilAsync(() => Probe.Calls == 1);

            var groups = fake.Groups(Stream);
            Assert.DoesNotContain(stale, groups);
            Assert.Contains(active, groups);
            Assert.Contains("svc-reports", groups);
            Assert.Contains("svc", groups);
        }

        [Fact]
        public async Task EntriesBlockedByAGroupThatWasDeleted_AreRemovedByTheNextSweep()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            fake.CreateGroup(Stream, "billing");
            fake.CreateGroup(Stream, "retired");
            Add(fake, 1);
            Add(fake, 2);
            await using (var first = new Instance(fake, appendGuid: false, group: "billing"))
            {
                await first.RunUntilAsync(() => Probe.Calls == 2);
            }
            Assert.Equal(2, fake.Entries(Stream).Count);

            fake.DeleteGroup(Stream, "retired");
            await using (var second = new Instance(fake, appendGuid: false, group: "billing"))
            {
                await second.RunUntilAsync(() => fake.Entries(Stream).Count == 0, 3000);
            }

            Assert.Empty(fake.Entries(Stream));
        }
    }
}
