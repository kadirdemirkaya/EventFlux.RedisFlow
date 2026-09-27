using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlux.Abstractions;
using Newtonsoft.Json;

namespace EventFlux.RedisFlow.Redis
{
    using EventFlux.RedisFlow.Abstractions;

    public interface IRedisStreamPublisher
    {
        Task PublishAsync(string eventType, string payload);
        Task PublishAsync(string eventType, string payload, EventContext context);

        /// <summary>
        /// Publishes <paramref name="event"/> to the stream using the event type name and JSON payload that the
        /// consuming worker resolves and deserializes, so the event reaches handlers as the same type.
        /// </summary>
        /// <typeparam name="TEvent">The event type.</typeparam>
        /// <param name="event">The event to publish.</param>
        /// <param name="ct">Cancels the call before the entry is written.</param>
        /// <exception cref="ArgumentNullException"><paramref name="event"/> is <c>null</c>.</exception>
        Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEventRequest
        {
            if (@event == null)
                throw new ArgumentNullException(nameof(@event));

            ct.ThrowIfCancellationRequested();

            return PublishAsync(EventTypeResolver.GetEventTypeName(@event.GetType()), JsonConvert.SerializeObject(@event));
        }
    }
}
