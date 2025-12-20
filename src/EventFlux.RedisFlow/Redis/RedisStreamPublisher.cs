using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventFlux.RedisFlow.Redis
{
    using EventFlux.RedisFlow.Abstractions;
    using Newtonsoft.Json;
    using System.Collections.Generic;

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

        public async Task PublishAsync(string eventType, string payload, EventContext context)
        {
            var list = new List<NameValueEntry>
            {
                new NameValueEntry("type", eventType),
                new NameValueEntry("data", payload)
            };

            if (context != null)
            {
                if (!string.IsNullOrEmpty(context.Version))
                {
                    list.Add(new NameValueEntry("version", context.Version));
                }

                if (context.Configs != null && context.Configs.Count > 0)
                {
                    list.Add(new NameValueEntry("configs", JsonConvert.SerializeObject(context.Configs)));
                }
            }

            await _db.StreamAddAsync(_options.StreamName, list.ToArray());
        }
    }
}
