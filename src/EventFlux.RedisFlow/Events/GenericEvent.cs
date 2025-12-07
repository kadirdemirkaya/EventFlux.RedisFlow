using EventFlux;
using EventFlux.Abstractions;

namespace EventFlux.RedisFlow.Events
{
    public class GenericEvent : IEventRequest
    {
        public string EventName { get; set; } = string.Empty;
        public string RawPayload { get; set; } = string.Empty;
    }
}
