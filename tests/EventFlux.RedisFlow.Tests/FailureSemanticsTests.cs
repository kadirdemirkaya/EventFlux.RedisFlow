using EventFlux.RedisFlow.Tests.Support;
using Newtonsoft.Json;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class FailureSemanticsTests
    {
        private static string Payload(string reason) => JsonConvert.SerializeObject(new PaymentFailed { Reason = reason });

        private static string Field(StackExchange.Redis.StreamEntry entry, string name) =>
            entry.Values.Single(v => v.Name == name).Value.ToString();

        [Fact]
        public async Task FailedEntry_IsNotAcknowledgedOrDeleted()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:10:00"));
            var id = h.Publish(nameof(PaymentFailed), Payload("card"));

            await h.RunForAsync(400);

            Assert.Equal(1, Probe.Calls);
            Assert.True(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group).ContainsKey(id));
            Assert.Single(h.Fake.Entries(WorkerHarness.Stream));
        }

        [Fact]
        public async Task FailedEntry_IsRetried_AndAcknowledgedOnceHandlerSucceeds()
        {
            Probe.Reset(failures: 2);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"));
            h.Publish(nameof(PaymentFailed), Payload("timeout"));

            await h.RunUntilAsync(() => h.Fake.Entries(WorkerHarness.Stream).Count == 0);

            Assert.Equal(3, Probe.Calls);
            Assert.Single(Probe.Received);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
            Assert.Empty(h.Fake.Entries("orders-dead-letter"));
        }

        [Fact]
        public async Task FailedEntry_IsNotRetried_BeforeRetryIdleTime()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:10:00"));
            h.Publish(nameof(PaymentFailed), Payload("card"));
            await h.RunForAsync(300);
            h.Fake.NowMs += 9 * 60 * 1000;

            await h.RunForAsync(1300);

            Assert.Equal(1, Probe.Calls);
        }

        [Fact]
        public async Task FailedEntry_IsRetried_AfterRetryIdleTime()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:10:00"));
            h.Publish(nameof(PaymentFailed), Payload("card"));
            await h.RunForAsync(300);
            h.Fake.NowMs += 10 * 60 * 1000;

            await h.RunUntilAsync(() => Probe.Calls >= 2);

            Assert.Equal(2, Probe.Calls);
        }

        [Fact]
        public async Task EntryExceedingMaxDeliveryAttempts_IsMovedToDeadLetterStream()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"), ("MaxDeliveryAttempts", "3"));
            var id = h.Publish(nameof(PaymentFailed), Payload("fraud"), new StackExchange.Redis.NameValueEntry("version", "2"));

            await h.RunUntilAsync(() => h.Fake.Entries("orders-dead-letter").Count == 1 && h.Fake.Entries(WorkerHarness.Stream).Count == 0);

            Assert.Equal(3, Probe.Calls);
            var dead = Assert.Single(h.Fake.Entries("orders-dead-letter"));
            Assert.Equal(nameof(PaymentFailed), Field(dead, "type"));
            Assert.Equal(Payload("fraud"), Field(dead, "data"));
            Assert.Equal("2", Field(dead, "version"));
            Assert.Equal("orders", Field(dead, "dead-letter-source-stream"));
            Assert.Equal(id, Field(dead, "dead-letter-source-id"));
            Assert.Equal("billing", Field(dead, "dead-letter-consumer-group"));
            Assert.Equal("3", Field(dead, "dead-letter-delivery-count"));
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
            Assert.Empty(h.Fake.Entries(WorkerHarness.Stream));
        }

        [Fact]
        public async Task DeadLetterStreamName_CanBeConfigured()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"), ("MaxDeliveryAttempts", "1"), ("DeadLetterStreamName", "orders.failed"));
            h.Publish(nameof(PaymentFailed), Payload("x"));

            await h.RunUntilAsync(() => h.Fake.Entries("orders.failed").Count == 1);

            Assert.Equal(1, Probe.Calls);
            Assert.Single(h.Fake.Entries("orders.failed"));
            Assert.Empty(h.Fake.Entries("orders-dead-letter"));
        }

        [Fact]
        public async Task DeadLetterWriteFailure_KeepsOriginalEntryPending()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"), ("MaxDeliveryAttempts", "2"));
            h.Fake.FailingAddStreams.Add("orders-dead-letter");
            var id = h.Publish(nameof(PaymentFailed), Payload("x"));

            await h.RunForAsync(800);

            Assert.Equal(2, Probe.Calls);
            Assert.True(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group).ContainsKey(id));
            Assert.Single(h.Fake.Entries(WorkerHarness.Stream));

            h.Fake.FailingAddStreams.Clear();
            await h.RunUntilAsync(() => h.Fake.Entries("orders-dead-letter").Count == 1);

            Assert.Equal(2, Probe.Calls);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
        }

        [Fact]
        public async Task EnableRetryFalse_LeavesFailedEntryPendingWithoutRetry()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"), ("EnableRetry", "false"));
            var id = h.Publish(nameof(PaymentFailed), Payload("x"));

            await h.RunForAsync(800);

            Assert.Equal(1, Probe.Calls);
            Assert.True(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group).ContainsKey(id));
            Assert.Empty(h.Fake.Entries("orders-dead-letter"));
        }

        [Fact]
        public async Task MaxDeliveryAttemptsZero_RetriesWithoutDeadLetter()
        {
            Probe.Reset(alwaysFail: true);
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"), ("MaxDeliveryAttempts", "0"));
            h.Publish(nameof(PaymentFailed), Payload("x"));

            await h.RunUntilAsync(() => Probe.Calls >= 8);

            Assert.True(Probe.Calls >= 8);
            Assert.Empty(h.Fake.Entries("orders-dead-letter"));
            Assert.Single(h.Fake.Entries(WorkerHarness.Stream));
        }

        [Fact]
        public async Task EntryLeftPendingByStoppedConsumer_IsClaimedAndProcessed()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:30"));
            var id = h.Publish(nameof(OrderPlaced), JsonConvert.SerializeObject(new OrderPlaced { OrderId = 5 }));
            h.Fake.Deliver(WorkerHarness.Stream, WorkerHarness.Group, "crashed-worker", id);
            h.Fake.NowMs += 31_000;

            await h.RunUntilAsync(() => Probe.Calls == 1);

            Assert.Equal(1, Probe.Calls);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
        }

        [Fact]
        public async Task ShutdownWhileHandlerRuns_CompletesAndAcknowledgesEntry()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:10:00"));
            h.Publish(nameof(SlowJob), "{\"DelayMs\":500}");
            await h.StartAsync();
            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 1));

            await h.StopAsync();

            Assert.Single(Probe.Received);
            Assert.Empty(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group));
            Assert.Empty(h.Fake.Entries(WorkerHarness.Stream));
        }

        [Fact]
        public async Task ShutdownTimeoutExpires_CancelsHandlerAndLeavesEntryPending()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:10:00"));
            var id = h.Publish(nameof(SlowJob), "{}");
            await h.StartAsync();
            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 1));

            using var timeout = new CancellationTokenSource(200);
            await h.StopAsync(timeout.Token);
            Assert.True(await WorkerHarness.WaitUntilAsync(() => h.Worker.ExecuteTask!.IsCompleted));

            Assert.True(Assert.Single(Probe.Tokens).IsCancellationRequested);
            Assert.Empty(Probe.Received);
            Assert.True(h.Fake.Pending(WorkerHarness.Stream, WorkerHarness.Group).ContainsKey(id));
            Assert.Single(h.Fake.Entries(WorkerHarness.Stream));
        }
    }
}
