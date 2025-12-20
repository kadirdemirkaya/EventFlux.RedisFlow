using EventFlux.Abstractions;
using EventFlux.RedisFlow.Abstractions;
using EventFlux_RedisFlow_Consumer.Api2.Events;

namespace EventFlux_RedisFlow_Consumer.Api2.Handlers
{
    public class PublishEventHandler : IEventHandler<PublishEventRequest>
    {
        private readonly IEventContextAccessor _contextAccessor;

        public PublishEventHandler(IEventContextAccessor contextAccessor)
        {
            _contextAccessor = contextAccessor;
        }

        public async Task Handle(PublishEventRequest request)
        {
            if (_contextAccessor.EventContext != null)
            {
                Console.WriteLine($"Context Version: {_contextAccessor.EventContext.Version}");
                if (_contextAccessor.EventContext.Configs != null)
                {
                    foreach (var cfg in _contextAccessor.EventContext.Configs)
                    {
                        Console.WriteLine($"Config {cfg.Key}: {cfg.Value}");
                    }
                }
            }
        }
    }
}
