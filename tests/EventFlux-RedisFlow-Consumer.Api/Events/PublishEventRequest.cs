using EventFlux;
using EventFlux.Abstractions;

namespace EventFlux_RedisFlow_Consumer.Api.Events
{
    public class PublishEventRequest : IEventRequest
    {
        public string Data { get; set; } = string.Empty;
    }
}
