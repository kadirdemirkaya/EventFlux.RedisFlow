using EventFlux;
using EventFlux.Abstractions;

namespace EventFlux_RedisFlow_Consumer.Api2.Events
{
    public class SendEventRequest : IEventRequest
    {
        public string Data { get; set; } = string.Empty;
    }
}
