using System;
using System.Threading.Tasks;
using EventFlux.Abstractions;
using EventFlux_RedisFlow_Consumer.Api.Events;

namespace EventFlux_RedisFlow_Consumer.Api.Handlers
{
    public class PublishEventHandler : IEventHandler<PublishEventRequest>
    {
        public async Task Handle(PublishEventRequest request)
        {
            Console.WriteLine(request.Data);
        }
    }
}
