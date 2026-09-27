using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Workers;
using Xunit;

namespace EventFlux.RedisFlow.Tests.NewApi
{
    public class RetryInternalsTests
    {
        [Theory]
        [InlineData("1700000000000-0", "1700000000000-1")]
        [InlineData("5-41", "5-42")]
        [InlineData("5-18446744073709551615", "6-0")]
        public void NextStreamId_ReturnsFollowingId(string id, string expected)
        {
            Assert.Equal(expected, RedisStreamWorker.NextStreamId(id).ToString());
        }

        [Fact]
        public void NextStreamId_InvalidId_ReturnsNull()
        {
            Assert.True(RedisStreamWorker.NextStreamId("not-an-id").IsNull);
        }

        [Theory]
        [InlineData("5-1", "5-2", -1)]
        [InlineData("10-0", "9-99", 1)]
        [InlineData("1700000000000-3", "1700000000000-3", 0)]
        [InlineData("2", "1-5", 1)]
        public void CompareStreamIds_OrdersNumerically(string x, string y, int expected)
        {
            Assert.Equal(expected, Math.Sign(RedisStreamWorker.CompareStreamIds(x, y)));
        }

        [Fact]
        public void Options_Defaults()
        {
            var options = new RedisStreamOptions { StreamName = "orders" };

            Assert.True(options.EnableRetry);
            Assert.Equal(5, options.MaxDeliveryAttempts);
            Assert.Equal(TimeSpan.FromSeconds(30), options.RetryIdleTime);
            Assert.Null(options.DeadLetterStreamName);
            Assert.True(options.DeleteProcessedEntries);
            Assert.Equal("orders-dead-letter", options.ResolveDeadLetterStreamName());
        }
    }
}
