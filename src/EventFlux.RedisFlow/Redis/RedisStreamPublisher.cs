using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlux.Abstractions;
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
            await _db.StreamAddAsync(_options.StreamName, CreateEntries(eventType, payload, null)).ConfigureAwait(false);
        }

        public async Task PublishAsync(string eventType, string payload, EventContext context)
        {
            await _db.StreamAddAsync(_options.StreamName, CreateEntries(eventType, payload, context)).ConfigureAwait(false);
        }

        /// <summary>
        /// Publishes <paramref name="event"/> to the stream using the event type name and JSON payload that the
        /// consuming worker resolves and deserializes, so the event reaches handlers as the same type.
        /// </summary>
        /// <typeparam name="TEvent">The event type.</typeparam>
        /// <param name="event">The event to publish.</param>
        /// <param name="ct">Cancels the call before the entry is written.</param>
        /// <exception cref="ArgumentNullException"><paramref name="event"/> is <c>null</c>.</exception>
        public async Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEventRequest
        {
            if (@event == null)
                throw new ArgumentNullException(nameof(@event));

            ct.ThrowIfCancellationRequested();

            await PublishAsync(EventTypeResolver.GetEventTypeName(@event.GetType()), JsonConvert.SerializeObject(@event)).ConfigureAwait(false);
        }

        internal static NameValueEntry[] CreateEntries(string eventType, string payload, EventContext? context)
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

            return list.ToArray();
        }
    }
}
