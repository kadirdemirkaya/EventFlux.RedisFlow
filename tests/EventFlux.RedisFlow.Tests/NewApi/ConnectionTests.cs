using EventFlux.RedisFlow.Workers;
using Xunit;

namespace EventFlux.RedisFlow.Tests.NewApi
{
    public class ConnectionTests
    {
        [Theory]
        [InlineData("localhost:6379", false)]
        [InlineData("localhost:6379,password=secret,ssl=false", false)]
        [InlineData("localhost:6379,abortConnect=true", true)]
        [InlineData("localhost:6379, AbortConnect = true", true)]
        [InlineData("localhost:6379,abortConnect=false", false)]
        public void CreateConnectionOptions_DisablesAbortOnConnectFailUnlessSet(string connectionString, bool expected)
        {
            var options = ServiceCollectionExtensions.CreateConnectionOptions(connectionString);

            Assert.Equal(expected, options.AbortOnConnectFail);
        }

        [Fact]
        public void CreateConnectionOptions_KeepsOtherSettings()
        {
            var options = ServiceCollectionExtensions.CreateConnectionOptions("cache-1:6380,password=secret,connectTimeout=2500");

            Assert.EndsWith("cache-1:6380", options.EndPoints.Single().ToString());
            Assert.Equal("secret", options.Password);
            Assert.Equal(2500, options.ConnectTimeout);
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(2, 2)]
        [InlineData(3, 4)]
        [InlineData(5, 16)]
        [InlineData(6, 30)]
        [InlineData(100, 30)]
        public void GetRetryDelay_GrowsExponentiallyUpTo30Seconds(int failures, int expectedSeconds)
        {
            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), RedisStreamWorker.GetRetryDelay(failures));
        }
    }
}
