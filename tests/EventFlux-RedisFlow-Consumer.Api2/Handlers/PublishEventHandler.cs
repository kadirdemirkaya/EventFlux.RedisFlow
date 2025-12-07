using EventFlux.Abstractions;
using EventFlux_RedisFlow_Consumer.Api2.Events;

namespace EventFlux_RedisFlow_Consumer.Api2.Handlers
{
    public class PublishEventHandler : IEventHandler<PublishEventRequest>
    {
        public async Task Handle(PublishEventRequest request)
        {
            Console.WriteLine(request.Data);
        }
    }
}
