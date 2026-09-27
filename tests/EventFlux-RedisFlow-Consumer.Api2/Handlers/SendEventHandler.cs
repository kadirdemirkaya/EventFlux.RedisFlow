using EventFlux.Abstractions;
using EventFlux_RedisFlow_Consumer.Api2.Events;

namespace EventFlux_RedisFlow_Consumer.Api2.Handlers
{
    public class SendEventHandler : IEventHandler<SendEventRequest>
    {
        public async Task Handle(SendEventRequest request, CancellationToken cancellationToken = default)
        {
            Console.WriteLine(request.Data);
        }
    }
}
