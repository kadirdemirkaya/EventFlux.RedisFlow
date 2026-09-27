using EventFlux.RedisFlow.Tests.Fakes;
using EventFlux.RedisFlow.Tests.Support;
using Newtonsoft.Json;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class RetentionTests
    {
        private const string Stream = WorkerHarness.Stream;

        private static string Order(int id) => JsonConvert.SerializeObject(new OrderPlaced { OrderId = id });

        [Fact]
        public async Task TwoGroups_GroupStartedLater_StillReceivesEntryProcessedByFirstGroup()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            fake.CreateGroup(Stream, "billing");
            fake.CreateGroup(Stream, "shipping");
            await using var billing = WorkerHarness.Create(fake, "billing");
            await using var shipping = WorkerHarness.Create(fake, "shipping");
            fake.Add(Stream, new StackExchange.Redis.NameValueEntry("type", nameof(OrderPlaced)), new StackExchange.Redis.NameValueEntry("data", Order(1)));

            await billing.RunForAsync(1500);
            Assert.Equal(1, Probe.Calls);

            await shipping.RunUntilAsync(() => Probe.Calls == 2);

            Assert.Equal(2, Probe.Calls);
        }

        [Fact]
        public async Task TwoGroups_EntryIsRemovedOnceBothGroupsProcessedIt()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            fake.CreateGroup(Stream, "billing");
            fake.CreateGroup(Stream, "shipping");
            await using var billing = WorkerHarness.Create(fake, "billing");
            await using var shipping = WorkerHarness.Create(fake, "shipping");
            for (var i = 0; i < 5; i++)
                fake.Add(Stream, new StackExchange.Redis.NameValueEntry("type", nameof(OrderPlaced)), new StackExchange.Redis.NameValueEntry("data", Order(i)));

            await billing.RunForAsync(1500);
            Assert.Equal(5, fake.Entries(Stream).Count);

            await shipping.RunUntilAsync(() => fake.Entries(Stream).Count == 0);

            Assert.Equal(10, Probe.Calls);
            Assert.Empty(fake.Entries(Stream));
        }

        [Fact]
        public async Task SingleGroup_ProcessedEntries_AreRemoved()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Fake.CreateGroup(Stream, WorkerHarness.Group);
            for (var i = 0; i < 3; i++)
                h.Publish(nameof(OrderPlaced), Order(i));

            await h.RunUntilAsync(() => h.Fake.Entries(Stream).Count == 0);

            Assert.Equal(3, Probe.Calls);
            Assert.Empty(h.Fake.Entries(Stream));
        }

        [Fact]
        public async Task DeleteProcessedEntriesFalse_KeepsAcknowledgedEntries()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create(("DeleteProcessedEntries", "false"));
            h.Fake.CreateGroup(Stream, WorkerHarness.Group);
            h.Publish(nameof(OrderPlaced), Order(1));
            h.Publish(nameof(OrderPlaced), Order(2));

            await h.RunForAsync(1500);

            Assert.Equal(2, Probe.Calls);
            Assert.Empty(h.Fake.Pending(Stream, WorkerHarness.Group));
            Assert.Equal(2, h.Fake.Entries(Stream).Count);
        }

        [Fact]
        public async Task PendingEntry_IsKept_WhileEntriesAroundItAreRemoved()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:10:00"));
            h.Fake.CreateGroup(Stream, WorkerHarness.Group);
            h.Publish(nameof(OrderPlaced), Order(1));
            var failing = h.Publish(nameof(PaymentFailed), JsonConvert.SerializeObject(new PaymentFailed { Reason = "x" }));
            h.Publish(nameof(OrderPlaced), Order(2));

            await h.RunUntilAsync(() => h.Fake.Entries(Stream).Count == 1);

            Assert.Equal(failing, Assert.Single(h.Fake.Entries(Stream)).Id.ToString());
        }

        [Fact]
        public async Task EntryStillPendingInOtherGroup_IsKept()
        {
            Probe.Reset();
            var fake = FakeStreamDatabase.Create();
            fake.CreateGroup(Stream, "billing");
            fake.CreateGroup(Stream, "shipping");
            await using var billing = WorkerHarness.Create(fake, "billing");
            var first = fake.Add(Stream, new StackExchange.Redis.NameValueEntry("type", nameof(OrderPlaced)), new StackExchange.Redis.NameValueEntry("data", Order(1)));
            var second = fake.Add(Stream, new StackExchange.Redis.NameValueEntry("type", nameof(OrderPlaced)), new StackExchange.Redis.NameValueEntry("data", Order(2)));
            fake.Deliver(Stream, "shipping", "shipping-worker", first);
            fake.Deliver(Stream, "shipping", "shipping-worker", second);
            fake.Ack(Stream, "shipping", first);

            await billing.RunForAsync(1500);

            Assert.Equal(2, Probe.Calls);
            Assert.Equal(new[] { second }, fake.Entries(Stream).Select(e => e.Id.ToString()));
        }

        [Fact]
        public async Task GroupInfoFailure_KeepsEntriesAndWorkerKeepsConsuming()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Fake.FailGroupInfo = true;
            h.Fake.CreateGroup(Stream, WorkerHarness.Group);
            h.Publish(nameof(OrderPlaced), Order(1));
            await h.RunForAsync(800);

            h.Publish(nameof(OrderPlaced), Order(2));
            await h.RunUntilAsync(() => Probe.Calls == 2);

            Assert.Equal(2, Probe.Calls);
            Assert.Empty(h.Fake.Pending(Stream, WorkerHarness.Group));
            Assert.Equal(2, h.Fake.Entries(Stream).Count);
        }
    }
}
