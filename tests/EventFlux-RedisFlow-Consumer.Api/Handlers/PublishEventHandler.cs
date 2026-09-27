using System;
using System.Threading.Tasks;
using EventFlux.Abstractions;
using EventFlux_RedisFlow_Consumer.Api.Events;

namespace EventFlux_RedisFlow_Consumer.Api.Handlers
{
    public class PublishEventHandler : IEventHandler<PublishEventRequest>
    {
        public async Task Handle(PublishEventRequest request, CancellationToken cancellationToken = default)
        {
            Console.WriteLine(request.Data);
        }
    }
}
