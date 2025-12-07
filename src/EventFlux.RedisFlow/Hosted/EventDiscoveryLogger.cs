using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventFlux.RedisFlow.Hosted
{
    using Redis;

    public class EventDiscoveryLogger : IHostedService
    {
        private readonly ILogger<EventDiscoveryLogger> _logger;
        private readonly RegisteredHandlers _handlers;

        public EventDiscoveryLogger(ILogger<EventDiscoveryLogger> logger, RegisteredHandlers handlers)
        {
            _logger = logger;
            _handlers = handlers;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                var names = EventTypeResolver.GetRegisteredEventTypes();
                _logger.LogInformation("EventFlux.RedisFlow: Registered event types: {Count}", names.Count);
                foreach (var kv in names)
                {
                    _logger.LogInformation(" - {Name} => {Type}", kv.Key, kv.Value.FullName);
                }

                _logger.LogInformation("EventFlux.RedisFlow: Discovered {Count} handler registrations", _handlers.Items.Length);
                foreach (var h in _handlers.Items)
                {
                    _logger.LogInformation(" - {Iface} -> {Impl}", h.iface.FullName, h.impl.FullName);
                }
            }
            catch (System.Exception ex)
            {
                _logger.LogWarning(ex, "Failed to log event discovery information");
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
