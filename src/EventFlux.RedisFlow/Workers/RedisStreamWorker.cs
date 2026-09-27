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
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace EventFlux.RedisFlow.Workers
{
    public class RedisStreamWorker : BackgroundService
    {
        private static readonly TimeSpan MaxRetryScanInterval = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan MaxReconnectDelay = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(30);
        private const int SweepBatchSize = 100;
        internal static readonly TimeSpan StaleEphemeralGroupIdleTime = TimeSpan.FromHours(1);

        private readonly IDatabase _db;
        private readonly RedisStreamOptions _options;
        private readonly IServiceProvider _services;
        private readonly Microsoft.Extensions.Logging.ILogger<RedisStreamWorker> _logger;
        private readonly CancellationTokenSource _handlerCancellation = new CancellationTokenSource();
        private readonly List<RedisValue> _acknowledged = new List<RedisValue>();
        private RedisValue? _retryCursor;
        private long _lastRetryScan;
        private long _lastSweep;
        private bool _staleGroupsChecked;

        public RedisStreamWorker(IConnectionMultiplexer mux, IOptions<RedisStreamOptions> options, IServiceProvider services, Microsoft.Extensions.Logging.ILogger<RedisStreamWorker> logger)
        {
            _options = options.Value;
            _db = mux.GetDatabase();
            _services = services;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumerGroupReady = false;
            var consecutiveFailures = 0;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (!consumerGroupReady)
                    {
                        await EnsureConsumerGroupAsync().ConfigureAwait(false);
                        consumerGroupReady = true;
                    }

                    var read = await PollAsync(stoppingToken).ConfigureAwait(false);
                    consecutiveFailures = 0;

                    if (read > 0)
                        continue;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (RedisServerException ex) when (ex.Message.StartsWith("NOGROUP", StringComparison.Ordinal))
                {
                    consumerGroupReady = false;
                    consecutiveFailures++;
                    _logger.LogWarning("Consumer group {Group} or stream {Stream} no longer exists; recreating it", _options.ConsumerGroup, _options.StreamName);
                }
                catch (Exception ex)
                {
                    consecutiveFailures++;
                    var delay = GetRetryDelay(consecutiveFailures);
                    _logger.LogError(ex, "Reading stream {Stream} failed ({Failures} consecutive failures); retrying in {Delay}", _options.StreamName, consecutiveFailures, delay);
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await Task.Delay(100, stoppingToken).ConfigureAwait(false);
            }
        }

        private async Task EnsureConsumerGroupAsync()
        {
            try
            {
                await _db.StreamCreateConsumerGroupAsync(_options.StreamName, _options.ConsumerGroup, "0-0", createStream: true).ConfigureAwait(false);
                _logger.LogInformation("Created consumer group {Group} on stream {Stream}", _options.ConsumerGroup, _options.StreamName);
            }
            catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
            {
            }

            if (!_staleGroupsChecked && _options.EphemeralGroupBaseName != null)
            {
                await RemoveStaleEphemeralGroupsAsync().ConfigureAwait(false);
                _staleGroupsChecked = true;
            }
        }

        internal async Task RemoveStaleEphemeralGroupsAsync()
        {
            var pattern = new Regex("^" + Regex.Escape(_options.EphemeralGroupBaseName!) + "-[0-9a-f]{32}$");
            var staleMs = (long)StaleEphemeralGroupIdleTime.TotalMilliseconds;

            foreach (var group in await _db.StreamGroupInfoAsync(_options.StreamName).ConfigureAwait(false))
            {
                if (group.Name == _options.ConsumerGroup || !pattern.IsMatch(group.Name))
                    continue;

                var consumers = await _db.StreamConsumerInfoAsync(_options.StreamName, group.Name).ConfigureAwait(false);
                if (consumers.Length == 0 || consumers.Any(c => c.IdleTimeInMilliseconds < staleMs))
                    continue;

                await _db.StreamDeleteConsumerGroupAsync(_options.StreamName, group.Name).ConfigureAwait(false);
                _logger.LogWarning("Deleted consumer group {Group} of a stopped instance; its consumers were idle for more than {Idle}", group.Name, StaleEphemeralGroupIdleTime);
            }
        }

        private async Task<int> PollAsync(CancellationToken stoppingToken)
        {
            if (_options.EnableRetry && IsRetryScanDue())
            {
                await RetryPendingAsync(stoppingToken).ConfigureAwait(false);
            }

            var entries = await _db.StreamReadGroupAsync(
                _options.StreamName,
                _options.ConsumerGroup,
                _options.ConsumerName,
                ">",
                _options.BatchSize
            ).ConfigureAwait(false);

            foreach (var entry in entries)
            {
                if (stoppingToken.IsCancellationRequested)
                    break;

                await ProcessEntryAsync(entry, _handlerCancellation.Token).ConfigureAwait(false);
            }

            await DeleteProcessedEntriesAsync().ConfigureAwait(false);

            if (_options.DeleteProcessedEntries && IsSweepDue())
                await SweepProcessedEntriesAsync().ConfigureAwait(false);

            return entries.Length;
        }

        internal static TimeSpan GetRetryDelay(int consecutiveFailures)
        {
            var seconds = Math.Pow(2, Math.Min(Math.Max(consecutiveFailures, 1) - 1, 5));
            return TimeSpan.FromSeconds(Math.Min(seconds, MaxReconnectDelay.TotalSeconds));
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            using (cancellationToken.Register(() => _handlerCancellation.Cancel()))
            {
                await base.StopAsync(cancellationToken).ConfigureAwait(false);
            }

            if (cancellationToken.IsCancellationRequested)
                _handlerCancellation.Cancel();

            if (_options.EphemeralGroupBaseName != null)
                await DeleteEphemeralGroupAsync().ConfigureAwait(false);
        }

        internal async Task DeleteEphemeralGroupAsync()
        {
            try
            {
                await _db.StreamDeleteConsumerGroupAsync(_options.StreamName, _options.ConsumerGroup).ConfigureAwait(false);
                _logger.LogInformation("Deleted consumer group {Group} of this instance on shutdown", _options.ConsumerGroup);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete consumer group {Group} on shutdown", _options.ConsumerGroup);
            }
        }

        public override void Dispose()
        {
            _handlerCancellation.Dispose();
            base.Dispose();
        }

        internal async Task<bool> ProcessEntryAsync(StreamEntry entry, CancellationToken handlerToken)
        {
            try
            {
                await DispatchAsync(entry, handlerToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (handlerToken.IsCancellationRequested)
            {
                _logger.LogWarning("Processing of stream entry {Id} was cancelled because the shutdown timeout expired; it stays pending and is retried", entry.Id);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing stream entry {Id}; it stays pending and is retried", entry.Id);
                return false;
            }

            await AcknowledgeAsync(entry.Id).ConfigureAwait(false);
            return true;
        }

        internal async Task DispatchAsync(StreamEntry entry, CancellationToken cancellationToken)
        {
            using (var scope = _services.CreateScope())
            {
                var contextAccessor = scope.ServiceProvider.GetRequiredService<IEventContextAccessor>();

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

                var typeEntry = entry.Values.FirstOrDefault(x => x.Name.HasValue && x.Name.ToString() == "type");
                var dataEntry = entry.Values.FirstOrDefault(x => x.Name.HasValue && x.Name.ToString() == "data");
                var eventType = typeEntry.Value.ToString();
                var payload = dataEntry.Value.ToString();

                _logger.LogDebug("Received stream entry {Id} type={Type}", entry.Id, eventType);

                object? requestObj;

                try
                {
                    var requestType = EventTypeResolver.Resolve(eventType);
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

                if (requestObj is IEventRequest req)
                {
                    var eventBus = scope.ServiceProvider.GetRequiredService<IEventBus>();
                    await eventBus.PublishAsync(req, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        internal async Task RetryPendingAsync(CancellationToken stoppingToken)
        {
            _lastRetryScan = Stopwatch.GetTimestamp();

            var pending = await _db.StreamPendingMessagesAsync(
                _options.StreamName,
                _options.ConsumerGroup,
                Math.Max(1, _options.BatchSize),
                RedisValue.Null,
                _retryCursor,
                null
            ).ConfigureAwait(false);

            var next = pending.Length >= Math.Max(1, _options.BatchSize)
                ? NextStreamId(pending[pending.Length - 1].MessageId)
                : RedisValue.Null;
            _retryCursor = next.IsNull ? (RedisValue?)null : next;

            var minIdleMs = (long)Math.Max(0, _options.RetryIdleTime.TotalMilliseconds);
            var idle = pending.Where(p => p.IdleTimeInMilliseconds >= minIdleMs).ToArray();
            if (idle.Length == 0)
                return;

            var deliveries = idle.ToDictionary(p => p.MessageId.ToString(), p => p.DeliveryCount);

            var claimed = await _db.StreamClaimAsync(
                _options.StreamName,
                _options.ConsumerGroup,
                _options.ConsumerName,
                minIdleMs,
                idle.Select(p => p.MessageId).ToArray()
            ).ConfigureAwait(false);

            foreach (var entry in claimed)
            {
                if (stoppingToken.IsCancellationRequested)
                    return;

                if (entry.IsNull)
                    continue;

                deliveries.TryGetValue(entry.Id.ToString(), out var delivered);

                if (_options.MaxDeliveryAttempts > 0 && delivered >= _options.MaxDeliveryAttempts)
                {
                    await DeadLetterAsync(entry, delivered).ConfigureAwait(false);
                    continue;
                }

                _logger.LogInformation("Retrying stream entry {Id} (delivery {Delivery})", entry.Id, delivered + 1);
                await ProcessEntryAsync(entry, _handlerCancellation.Token).ConfigureAwait(false);
            }

            await DeleteProcessedEntriesAsync().ConfigureAwait(false);
        }

        internal async Task<bool> DeadLetterAsync(StreamEntry entry, int deliveryCount)
        {
            var deadLetterStream = _options.ResolveDeadLetterStreamName();

            var fields = entry.Values
                .Concat(new[]
                {
                    new NameValueEntry("dead-letter-source-stream", _options.StreamName),
                    new NameValueEntry("dead-letter-source-id", entry.Id),
                    new NameValueEntry("dead-letter-consumer-group", _options.ConsumerGroup),
                    new NameValueEntry("dead-letter-delivery-count", deliveryCount),
                    new NameValueEntry("dead-letter-at", DateTime.UtcNow.ToString("O"))
                })
                .ToArray();

            try
            {
                await _db.StreamAddAsync(deadLetterStream, fields).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to move stream entry {Id} to dead-letter stream {DeadLetterStream}; it stays pending", entry.Id, deadLetterStream);
                return false;
            }

            _logger.LogWarning("Moved stream entry {Id} to dead-letter stream {DeadLetterStream} after {Deliveries} deliveries", entry.Id, deadLetterStream, deliveryCount);

            await AcknowledgeAsync(entry.Id).ConfigureAwait(false);
            return true;
        }

        private async Task AcknowledgeAsync(RedisValue id)
        {
            await _db.StreamAcknowledgeAsync(_options.StreamName, _options.ConsumerGroup, id).ConfigureAwait(false);
            _logger.LogDebug("Acknowledged stream entry {Id} in group {Group}", id, _options.ConsumerGroup);

            if (_options.DeleteProcessedEntries)
                _acknowledged.Add(id);
        }

        internal async Task DeleteProcessedEntriesAsync()
        {
            if (_acknowledged.Count == 0)
                return;

            var candidates = _acknowledged.ToArray();
            _acknowledged.Clear();

            await DeleteIfProcessedByAllGroupsAsync(candidates, includeOwnGroup: false).ConfigureAwait(false);
        }

        internal async Task SweepProcessedEntriesAsync()
        {
            _lastSweep = Stopwatch.GetTimestamp();

            try
            {
                var oldest = await _db.StreamRangeAsync(_options.StreamName, "-", "+", SweepBatchSize).ConfigureAwait(false);
                if (oldest.Length > 0)
                    await DeleteIfProcessedByAllGroupsAsync(oldest.Select(e => e.Id).ToArray(), includeOwnGroup: true).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to sweep processed entries from stream {Stream}", _options.StreamName);
            }
        }

        private async Task DeleteIfProcessedByAllGroupsAsync(RedisValue[] candidates, bool includeOwnGroup)
        {
            try
            {
                var groups = await _db.StreamGroupInfoAsync(_options.StreamName).ConfigureAwait(false);
                if (includeOwnGroup && groups.Length == 0)
                    return;

                var deletable = new HashSet<RedisValue>(candidates);

                foreach (var group in groups)
                {
                    if (!includeOwnGroup && group.Name == _options.ConsumerGroup)
                        continue;

                    deletable.RemoveWhere(id => CompareStreamIds(id, group.LastDeliveredId) > 0);

                    if (group.PendingMessageCount > 0 && deletable.Count > 0)
                    {
                        var ordered = deletable.OrderBy(id => id, Comparer<RedisValue>.Create(CompareStreamIds)).ToArray();
                        var pending = await _db.StreamPendingMessagesAsync(
                            _options.StreamName,
                            group.Name,
                            group.PendingMessageCount,
                            RedisValue.Null,
                            ordered[0],
                            ordered[ordered.Length - 1]
                        ).ConfigureAwait(false);

                        foreach (var p in pending)
                            deletable.Remove(p.MessageId);
                    }

                    if (deletable.Count == 0)
                        return;
                }

                var deleted = await _db.StreamDeleteAsync(_options.StreamName, deletable.ToArray()).ConfigureAwait(false);
                _logger.LogDebug("Deleted {Count} processed entries from stream {Stream}; {Kept} kept for other consumer groups", deleted, _options.StreamName, candidates.Length - deletable.Count);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete processed entries from stream {Stream}; they are kept", _options.StreamName);
            }
        }

        private bool IsSweepDue()
        {
            return _lastSweep == 0 || Stopwatch.GetElapsedTime(_lastSweep) >= SweepInterval;
        }

        internal static int CompareStreamIds(RedisValue x, RedisValue y)
        {
            var (xMs, xSeq) = ParseStreamId(x.ToString());
            var (yMs, ySeq) = ParseStreamId(y.ToString());
            return xMs != yMs ? xMs.CompareTo(yMs) : xSeq.CompareTo(ySeq);
        }

        private static (ulong Ms, ulong Seq) ParseStreamId(string id)
        {
            var dash = id.IndexOf('-');
            if (dash < 0)
                return (ulong.TryParse(id, out var onlyMs) ? onlyMs : 0, 0);

            ulong.TryParse(id.Substring(0, dash), out var ms);
            ulong.TryParse(id.Substring(dash + 1), out var seq);
            return (ms, seq);
        }

        private bool IsRetryScanDue()
        {
            var interval = _options.RetryIdleTime < MaxRetryScanInterval ? _options.RetryIdleTime : MaxRetryScanInterval;
            return _lastRetryScan == 0 || Stopwatch.GetElapsedTime(_lastRetryScan) >= interval;
        }

        internal static RedisValue NextStreamId(RedisValue id)
        {
            var text = id.ToString();
            var dash = text.IndexOf('-');
            if (dash > 0
                && ulong.TryParse(text.Substring(0, dash), out var ms)
                && ulong.TryParse(text.Substring(dash + 1), out var seq))
            {
                return seq == ulong.MaxValue ? $"{ms + 1}-0" : $"{ms}-{seq + 1}";
            }

            return RedisValue.Null;
        }
    }
}
