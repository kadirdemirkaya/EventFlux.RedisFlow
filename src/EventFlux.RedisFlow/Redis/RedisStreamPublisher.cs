using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventFlux.RedisFlow.Redis
{
    public class RedisStreamPublisher : IRedisStreamPublisher
    {
        private readonly IDatabase _db;
        private readonly RedisStreamOptions _options;

        public RedisStreamPublisher(IConnectionMultiplexer mux, IOptions<RedisStreamOptions> options)
        {
            _options = options.Value;
            _db = mux.GetDatabase();
        }

        public async Task PublishAsync(string eventType, string payload)
        {
            var entry = new NameValueEntry[]
            {
                new NameValueEntry("type", eventType),
                new NameValueEntry("data", payload)
            };

            await _db.StreamAddAsync(_options.StreamName, entry);
        }
    }
}
