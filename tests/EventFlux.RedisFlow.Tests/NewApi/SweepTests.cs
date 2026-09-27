using EventFlux.RedisFlow.Tests.Support;
using Newtonsoft.Json;
using StackExchange.Redis;
using Xunit;

namespace EventFlux.RedisFlow.Tests.NewApi
{
    public class SweepTests
    {
        [Fact]
        public async Task Sweep_KeepsEntriesUnreadOrPendingInOwnGroup()
        {
            await using var h = WorkerHarness.Create();
            var fake = h.Fake;
            fake.CreateGroup(WorkerHarness.Stream, WorkerHarness.Group);
            for (var i = 0; i < 3; i++)
                fake.Add(WorkerHarness.Stream, new NameValueEntry("type", nameof(OrderPlaced)), new NameValueEntry("data", JsonConvert.SerializeObject(new OrderPlaced { OrderId = i })));
            var ids = fake.Entries(WorkerHarness.Stream).Select(e => e.Id.ToString()).ToArray();
            fake.Deliver(WorkerHarness.Stream, WorkerHarness.Group, "worker", ids[0]);
            fake.Ack(WorkerHarness.Stream, WorkerHarness.Group, ids[0]);
            fake.Deliver(WorkerHarness.Stream, WorkerHarness.Group, "worker", ids[1]);

            await h.Worker.SweepProcessedEntriesAsync();

            Assert.Equal(new[] { ids[1], ids[2] }, fake.Entries(WorkerHarness.Stream).Select(e => e.Id.ToString()));
        }

        [Fact]
        public async Task Sweep_StreamWithoutGroups_KeepsEntries()
        {
            await using var h = WorkerHarness.Create();
            h.Fake.Add(WorkerHarness.Stream, new NameValueEntry("type", "X"), new NameValueEntry("data", "{}"));

            await h.Worker.SweepProcessedEntriesAsync();

            Assert.Single(h.Fake.Entries(WorkerHarness.Stream));
        }
    }
}
