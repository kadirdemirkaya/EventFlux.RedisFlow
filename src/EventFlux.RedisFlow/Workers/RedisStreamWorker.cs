using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using StackExchange.Redis;
using EventFlux.Abstractions;
using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Abstractions;
using System.Collections.Generic;

namespace EventFlux.RedisFlow.Workers
{
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
                    using (var scope = _services.CreateScope())
                    {
                        var contextAccessor = scope.ServiceProvider.GetRequiredService<IEventContextAccessor>();

                        if (contextAccessor != null)
                        {
                            var ctx = new EventContext { StreamId = entry.Id.ToString(), Timestamp = DateTime.UtcNow };

                            var versionEntry = entry.Values.FirstOrDefault(x => x.Name.HasValue && x.Name.ToString() == "version");
                            var configsEntry = entry.Values.FirstOrDefault(x => x.Name.HasValue && x.Name.ToString() == "configs");

                            if (versionEntry.Name.HasValue) ctx.Version = versionEntry.Value.ToString();
                            if (configsEntry.Name.HasValue)
                            {
                                try
                                {
                                    ctx.Configs = JsonConvert.DeserializeObject<Dictionary<string, string>>(configsEntry.Value.ToString()) ?? new Dictionary<string, string>();
                                }
                                catch { }
                            }
                            contextAccessor.EventContext = ctx;
                        }

                        try
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
                                requestObj = new Events.GenericEvent
                                {
                                    EventName = eventType,
                                    RawPayload = payload
                                };
                            }

                            if (requestObj != null)
                            {
                                var eventBus = scope.ServiceProvider.GetRequiredService<IEventBus>();
                                if (requestObj is IEventRequest req)
                                {
                                    await eventBus.PublishAsync(req);
                                }
                                else if (requestObj is Events.GenericEvent gen)
                                {
                                    await eventBus.PublishAsync(gen);
                                }
                            }

                            await _db.StreamAcknowledgeAsync(_options.StreamName, _options.ConsumerGroup, entry.Id);
                            _logger.LogDebug("Acknowledged stream entry {Id} in group {Group}", entry.Id, _options.ConsumerGroup);

                            try
                            {
                                var deleted = await _db.StreamDeleteAsync(_options.StreamName, new StackExchange.Redis.RedisValue[] { entry.Id });
                                _logger.LogDebug("Deleted stream entry {Id} from stream {Stream} (deleted={Count})", entry.Id, _options.StreamName, deleted);
                            }
                            catch (System.Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to delete stream entry {Id}", entry.Id);
                            }
                        }
                        catch (Exception ex)
                        {
                            // Context is available here for logging!
                            var version = contextAccessor?.EventContext?.Version ?? "N/A";
                            _logger.LogError(ex, "Error processing stream entry {Id}. Version: {Version}", entry.Id, version);

                            // Optional: Decided whether to acknowledge or not on failure. 
                            // Usually we might want to ACK if it's a poison message to avoid infinite loop, 
                            // OR keep it to retry. For now, we just log and continue.
                        }
                    }
                }

                await Task.Delay(100, stoppingToken);
            }
        }
    }
}
