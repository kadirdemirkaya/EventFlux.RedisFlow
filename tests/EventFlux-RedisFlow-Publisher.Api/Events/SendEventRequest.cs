using EventFlux;
using EventFlux.Abstractions;

namespace EventFlux_RedisFlow_Publisher.Api.Events
{
    public class SendEventRequest : IEventRequest
    {
        public string Data { get; set; } = string.Empty;
    }
}
