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

        [Fact]
        public void Options_Defaults()
        {
            var options = new RedisStreamOptions { StreamName = "orders" };

            Assert.True(options.EnableRetry);
            Assert.Equal(5, options.MaxDeliveryAttempts);
            Assert.Equal(TimeSpan.FromSeconds(30), options.RetryIdleTime);
            Assert.Null(options.DeadLetterStreamName);
            Assert.Equal("orders-dead-letter", options.ResolveDeadLetterStreamName());
        }
    }
}
