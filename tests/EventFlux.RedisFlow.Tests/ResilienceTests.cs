using EventFlux.RedisFlow.Tests.Support;
using Newtonsoft.Json;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class ResilienceTests
    {
        private const string Stream = WorkerHarness.Stream;

        private static string Order(int id) => JsonConvert.SerializeObject(new OrderPlaced { OrderId = id });

        [Fact]
        public async Task RedisUnavailableAtStart_WorkerKeepsRunning_AndConsumesAfterRecovery()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Fake.Unavailable = true;
            await h.StartAsync();
            await Task.Delay(500);
            Assert.False(h.Worker.ExecuteTask!.IsCompleted);

            h.Fake.Unavailable = false;
            h.Publish(nameof(OrderPlaced), Order(1));
            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 1, 8000));

            await h.StopAsync();
        }

        [Fact]
        public async Task RedisUnavailableWhileRunning_WorkerKeepsRunning_AndConsumesAfterRecovery()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Publish(nameof(OrderPlaced), Order(1));
            await h.StartAsync();
            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 1));

            h.Fake.Unavailable = true;
            await Task.Delay(700);
            Assert.False(h.Worker.ExecuteTask!.IsCompleted);
            h.Fake.Unavailable = false;
            h.Publish(nameof(OrderPlaced), Order(2));

            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 2, 8000));
            await h.StopAsync();
        }

        [Fact]
        public async Task StreamDeletedWhileRunning_GroupIsRecreated_AndConsumptionContinues()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create();
            h.Publish(nameof(OrderPlaced), Order(1));
            await h.StartAsync();
            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 1));

            h.Fake.DeleteStream(Stream);
            h.Publish(nameof(OrderPlaced), Order(2));

            Assert.True(await WorkerHarness.WaitUntilAsync(() => Probe.Calls == 2, 8000));
            Assert.False(h.Worker.ExecuteTask!.IsCompleted);
            await h.StopAsync();
        }

        [Fact]
        public async Task AcknowledgementFails_WorkerKeepsRunning_AndEntryIsRetriedLater()
        {
            Probe.Reset();
            await using var h = WorkerHarness.Create(("RetryIdleTime", "00:00:00"));
            h.Fake.FailingAcknowledgements = 1;
            h.Publish(nameof(OrderPlaced), Order(1));

            await h.RunUntilAsync(() => Probe.Calls == 2 && h.Fake.Pending(Stream, WorkerHarness.Group).Count == 0, 8000);

            Assert.Equal(2, Probe.Calls);
            Assert.Empty(h.Fake.Pending(Stream, WorkerHarness.Group));
        }
    }
}
