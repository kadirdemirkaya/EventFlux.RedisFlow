using System.Diagnostics;
using EventFlux.RedisFlow.Tests.Support;
using Newtonsoft.Json;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class ThroughputTests
    {
        [Fact]
        public async Task QueuedEntries_AreDrainedWithoutWaitingBetweenFullBatches()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create(("BatchSize", "10"));
            h.Fake.CreateGroup(WorkerHarness.Stream, WorkerHarness.Group);
            for (var i = 0; i < 200; i++)
                h.Publish(nameof(OrderPlaced), JsonConvert.SerializeObject(new OrderPlaced { OrderId = i }));

            var sw = Stopwatch.StartNew();
            await h.RunUntilAsync(() => Probe.Calls == 200, 10000);
            sw.Stop();

            Assert.Equal(200, Probe.Calls);
            Assert.True(sw.ElapsedMilliseconds < 1500, $"200 entries took {sw.ElapsedMilliseconds} ms");
        }

        [Fact]
        public async Task IdleWorker_StillPausesBetweenEmptyReads()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Fake.CreateGroup(WorkerHarness.Stream, WorkerHarness.Group);

            await h.RunForAsync(1000);

            Assert.InRange(h.Fake.ReadCalls, 1, 15);
        }
    }
}
