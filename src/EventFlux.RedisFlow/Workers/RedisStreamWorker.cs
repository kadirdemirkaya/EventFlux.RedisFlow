using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace EventFlux.RedisFlow.Workers
{
    using EventFlux.Abstractions;
    using EventFlux.RedisFlow.Redis;

    public class RedisStreamWorker : BackgroundService
    {
        private readonly IDatabase _db;
        private readonly RedisStreamOptions _options;
        private readonly IServiceProvider _services;
        private readonly Microsoft.Extensions.Logging.ILogger<RedisStreamWorker> _logger;

        public RedisStreamWorker(IConnectionMultiplexer mux, IOptions<RedisStreamOptions> options, IServiceProvider services, Microsoft.Extensions.Logging.ILogger<RedisStreamWorker> logger)
        {
            _options = options.Value;
            _db = mux.GetDatabase();
            _services = services;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Ensure consumer group exists (ignore errors)
            try
            {
                await _db.StreamCreateConsumerGroupAsync(_options.StreamName, _options.ConsumerGroup, "0-0", createStream: true);
            }
            catch { }

            while (!stoppingToken.IsCancellationRequested)
            {
                var entries = await _db.StreamReadGroupAsync(
                    _options.StreamName,
                    _options.ConsumerGroup,
                    _options.ConsumerName,
                    ">",
                    _options.BatchSize
                );

                foreach (var entry in entries)
                {
                    var typeEntry = entry.Values.FirstOrDefault(x => x.Name.HasValue && x.Name.ToString() == "type");
                    var dataEntry = entry.Values.FirstOrDefault(x => x.Name.HasValue && x.Name.ToString() == "data");

                    var eventType = typeEntry.Value.ToString();
                    var payload = dataEntry.Value.ToString();

                    _logger.LogDebug("Received stream entry {Id} type={Type}", entry.Id, eventType);

                    object? requestObj = null;
                    Type? requestType = null;

                    try
                    {
                        requestType = EventTypeResolver.Resolve(eventType);
                        requestObj = JsonConvert.DeserializeObject(payload, requestType);
                    }
                    catch
                    {
                        var gen = new Events.GenericEvent
                        {
                            EventName = eventType,
                            RawPayload = payload
                        };

                        using (var scope = _services.CreateScope())
                        {
                            var eventBus = scope.ServiceProvider.GetRequiredService<IEventBus>();
                            await eventBus.PublishAsync(gen);
                        }

                        await _db.StreamAcknowledgeAsync(_options.StreamName, _options.ConsumerGroup, entry.Id);
                        continue;
                    }

                    if (requestObj is IEventRequest req)
                    {
                        using (var scope = _services.CreateScope())
                        {
                            var eventBus = scope.ServiceProvider.GetRequiredService<IEventBus>();
                            await eventBus.PublishAsync(req);
                        }
                    }

                    await _db.StreamAcknowledgeAsync(_options.StreamName, _options.ConsumerGroup, entry.Id);
                }

                await Task.Delay(100, stoppingToken);
            }
        }
    }
}
