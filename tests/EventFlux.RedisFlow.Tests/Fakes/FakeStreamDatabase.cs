using System.Reflection;
using StackExchange.Redis;

namespace EventFlux.RedisFlow.Tests.Fakes
{
    public class FakeStreamDatabase : DispatchProxy
    {
        private readonly object _gate = new object();
        private readonly Dictionary<string, List<StreamEntry>> _streams = new Dictionary<string, List<StreamEntry>>();
        private readonly Dictionary<(string Stream, string Group), GroupState> _groups = new Dictionary<(string, string), GroupState>();
        private long _lastMs;
        private long _seq;

        public long NowMs { get; set; } = 1_000_000;

        public HashSet<string> FailingAddStreams { get; } = new HashSet<string>();

        public bool FailGroupInfo { get; set; }

        public bool Unavailable { get; set; }

        public int ReadCalls { get; private set; }

        public int FailingAcknowledgements { get; set; }

        public IConnectionMultiplexer Multiplexer { get; private set; } = null!;

        public IDatabase Database { get; private set; } = null!;

        public static FakeStreamDatabase Create()
        {
            var db = Create<IDatabase, FakeStreamDatabase>();
            var fake = (FakeStreamDatabase)(object)db;
            fake.Database = db;
            var mux = Create<IConnectionMultiplexer, FakeMultiplexer>();
            ((FakeMultiplexer)(object)mux).Database = db;
            fake.Multiplexer = mux;
            return fake;
        }

        public IReadOnlyList<StreamEntry> Entries(string stream)
        {
            lock (_gate)
            {
                return _streams.TryGetValue(stream, out var list) ? list.ToArray() : Array.Empty<StreamEntry>();
            }
        }

        public IReadOnlyDictionary<string, PendingState> Pending(string stream, string group)
        {
            lock (_gate)
            {
                return _groups.TryGetValue((stream, group), out var g)
                    ? g.Pending.ToDictionary(kv => kv.Key, kv => kv.Value)
                    : new Dictionary<string, PendingState>();
            }
        }

        public string Add(string stream, params NameValueEntry[] values)
        {
            lock (_gate)
            {
                return AddCore(stream, values);
            }
        }

        public void CreateGroup(string stream, string group)
        {
            lock (_gate)
            {
                if (!_streams.ContainsKey(stream))
                    _streams[stream] = new List<StreamEntry>();
                if (!_groups.ContainsKey((stream, group)))
                    _groups[(stream, group)] = new GroupState();
            }
        }

        public void DeleteStream(string stream)
        {
            lock (_gate)
            {
                _streams.Remove(stream);
                foreach (var key in _groups.Keys.Where(k => k.Stream == stream).ToArray())
                    _groups.Remove(key);
            }
        }

        public void Ack(string stream, string group, string id)
        {
            lock (_gate)
            {
                Acknowledge(stream, group, id);
            }
        }

        public void Deliver(string stream, string group, string consumer, string id)
        {
            lock (_gate)
            {
                CreateGroup(stream, group);
                var g = _groups[(stream, group)];
                g.Pending[id] = new PendingState(consumer, 1, NowMs);
                if (Compare(id, g.LastDeliveredId) > 0)
                    g.LastDeliveredId = id;
            }
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var a = args ?? Array.Empty<object?>();
            lock (_gate)
            {
                if (Unavailable)
                    return FailedTask(targetMethod!.ReturnType, new RedisConnectionException(ConnectionFailureType.UnableToConnect, "fake redis unavailable"));
                if (targetMethod!.Name == "StreamAcknowledgeAsync" && FailingAcknowledgements > 0)
                {
                    FailingAcknowledgements--;
                    return FailedTask(targetMethod.ReturnType, new RedisTimeoutException("fake ack timeout", CommandStatus.Unknown));
                }

                switch (targetMethod!.Name)
                {
                    case "StreamCreateConsumerGroupAsync":
                        return Task.FromResult(CreateConsumerGroup(Key(a[0]), Value(a[1])));
                    case "StreamReadGroupAsync" when a[0] is RedisKey:
                        return Task.FromResult(ReadGroup(Key(a[0]), Value(a[1]), Value(a[2]), (int?)a[4]));
                    case "StreamAddAsync" when a[1] is NameValueEntry[] entries:
                        return FailingAddStreams.Contains(Key(a[0]))
                            ? Task.FromException<RedisValue>(new RedisServerException("fake add failure"))
                            : Task.FromResult((RedisValue)AddCore(Key(a[0]), entries));
                    case "StreamAcknowledgeAsync" when a[2] is RedisValue id:
                        return Task.FromResult(Acknowledge(Key(a[0]), Value(a[1]), id.ToString()));
                    case "StreamDeleteAsync":
                        return Task.FromResult(Delete(Key(a[0]), (RedisValue[])a[1]!));
                    case "StreamPendingMessagesAsync":
                        return Task.FromResult(PendingMessages(Key(a[0]), Value(a[1]), (int)a[2]!, (RedisValue?)a[4], (RedisValue?)a[5]));
                    case "StreamClaimAsync":
                        return Task.FromResult(Claim(Key(a[0]), Value(a[1]), Value(a[2]), (long)a[3]!, (RedisValue[])a[4]!));
                    case "StreamDeleteConsumerGroupAsync":
                        return Task.FromResult(_groups.Remove((Key(a[0]), Value(a[1]))));
                    case "StreamConsumerInfoAsync":
                        return Task.FromResult(ConsumerInfo(Key(a[0]), Value(a[1])));
                    case "StreamRangeAsync":
                        return Task.FromResult(Entries(Key(a[0])).Take((int?)a[3] ?? int.MaxValue).ToArray());
                    case "StreamGroupInfoAsync":
                        return Task.FromResult(GroupInfo(Key(a[0])));
                    case "StreamPendingAsync":
                        return Task.FromResult(PendingSummary(Key(a[0]), Value(a[1])));
                }
            }

            throw new NotSupportedException(targetMethod.Name);
        }

        private static object FailedTask(Type taskType, Exception ex)
        {
            var result = taskType.GetGenericArguments()[0];
            return typeof(Task).GetMethod(nameof(Task.FromException), 1, new[] { typeof(Exception) })!
                .MakeGenericMethod(result)
                .Invoke(null, new object[] { ex })!;
        }

        private bool CreateConsumerGroup(string stream, string group)
        {
            if (_groups.ContainsKey((stream, group)))
                throw new RedisServerException("BUSYGROUP Consumer Group name already exists");
            CreateGroup(stream, group);
            return true;
        }

        private StreamEntry[] ReadGroup(string stream, string group, string consumer, int? count)
        {
            if (!_groups.TryGetValue((stream, group), out var g))
                throw new RedisServerException("NOGROUP");
            g.Consumers[consumer] = NowMs;
            ReadCalls++;
            var result = Entries(stream)
                .Where(e => Compare(e.Id.ToString(), g.LastDeliveredId) > 0)
                .Take(count ?? int.MaxValue)
                .ToArray();
            foreach (var e in result)
            {
                g.Pending[e.Id.ToString()] = new PendingState(consumer, 1, NowMs);
                g.LastDeliveredId = e.Id.ToString();
            }
            return result;
        }

        private string AddCore(string stream, NameValueEntry[] values)
        {
            if (!_streams.TryGetValue(stream, out var list))
                _streams[stream] = list = new List<StreamEntry>();
            if (NowMs > _lastMs)
            {
                _lastMs = NowMs;
                _seq = 0;
            }
            else
            {
                _seq++;
            }
            var id = $"{_lastMs}-{_seq}";
            list.Add(new StreamEntry(id, values));
            return id;
        }

        private long Acknowledge(string stream, string group, string id)
        {
            return _groups.TryGetValue((stream, group), out var g) && g.Pending.Remove(id) ? 1 : 0;
        }

        private long Delete(string stream, RedisValue[] ids)
        {
            if (!_streams.TryGetValue(stream, out var list))
                return 0;
            var set = ids.Select(i => i.ToString()).ToHashSet();
            return list.RemoveAll(e => set.Contains(e.Id.ToString()));
        }

        private StreamPendingMessageInfo[] PendingMessages(string stream, string group, int count, RedisValue? minId, RedisValue? maxId)
        {
            if (!_groups.TryGetValue((stream, group), out var g))
                throw new RedisServerException("NOGROUP");
            return g.Pending
                .Where(kv => minId == null || minId.Value.IsNull || Compare(kv.Key, minId.Value.ToString()) >= 0)
                .Where(kv => maxId == null || maxId.Value.IsNull || Compare(kv.Key, maxId.Value.ToString()) <= 0)
                .OrderBy(kv => kv.Key, Comparer<string>.Create(Compare))
                .Take(count)
                .Select(kv => CreatePendingInfo(kv.Key, kv.Value.Consumer, NowMs - kv.Value.LastDeliveredMs, kv.Value.DeliveryCount))
                .ToArray();
        }

        private StreamEntry[] Claim(string stream, string group, string consumer, long minIdle, RedisValue[] ids)
        {
            var g = _groups[(stream, group)];
            g.Consumers[consumer] = NowMs;
            var result = new List<StreamEntry>();
            foreach (var id in ids.Select(i => i.ToString()))
            {
                if (!g.Pending.TryGetValue(id, out var p) || NowMs - p.LastDeliveredMs < minIdle)
                    continue;
                var entry = Entries(stream).FirstOrDefault(e => e.Id.ToString() == id);
                if (entry.IsNull)
                {
                    g.Pending.Remove(id);
                    continue;
                }
                g.Pending[id] = new PendingState(consumer, p.DeliveryCount + 1, NowMs);
                result.Add(entry);
            }
            return result.ToArray();
        }

        private StreamGroupInfo[] GroupInfo(string stream)
        {
            if (FailGroupInfo)
                throw new RedisServerException("fake group info failure");
            if (!_streams.ContainsKey(stream))
                throw new RedisServerException("ERR no such key");
            return _groups
                .Where(kv => kv.Key.Stream == stream)
                .Select(kv => (StreamGroupInfo)Activator.CreateInstance(
                    typeof(StreamGroupInfo),
                    BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                    null,
                    new object?[] { kv.Key.Group, kv.Value.Consumers.Count, kv.Value.Pending.Count, kv.Value.LastDeliveredId, null, null },
                    null)!)
                .ToArray();
        }

        private StreamConsumerInfo[] ConsumerInfo(string stream, string group)
        {
            if (!_groups.TryGetValue((stream, group), out var g))
                throw new RedisServerException("NOGROUP");
            return g.Consumers
                .Select(kv => (StreamConsumerInfo)Activator.CreateInstance(
                    typeof(StreamConsumerInfo),
                    BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                    null,
                    new object[] { kv.Key, g.Pending.Count(p => p.Value.Consumer == kv.Key), NowMs - kv.Value },
                    null)!)
                .ToArray();
        }

        public IReadOnlyList<string> Groups(string stream)
        {
            lock (_gate)
            {
                return _groups.Keys.Where(k => k.Stream == stream).Select(k => k.Group).OrderBy(n => n).ToArray();
            }
        }

        public void DeleteGroup(string stream, string group)
        {
            lock (_gate)
            {
                _groups.Remove((stream, group));
            }
        }

        public void Touch(string stream, string group, string consumer)
        {
            lock (_gate)
            {
                CreateGroup(stream, group);
                _groups[(stream, group)].Consumers[consumer] = NowMs;
            }
        }

        private StreamPendingInfo PendingSummary(string stream, string group)
        {
            if (!_groups.TryGetValue((stream, group), out var g))
                throw new RedisServerException("NOGROUP");
            var ids = g.Pending.Keys.OrderBy(k => k, Comparer<string>.Create(Compare)).ToArray();
            return (StreamPendingInfo)Activator.CreateInstance(
                typeof(StreamPendingInfo),
                BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                null,
                new object[] { ids.Length, ids.Length > 0 ? (RedisValue)ids[0] : RedisValue.Null, ids.Length > 0 ? (RedisValue)ids[^1] : RedisValue.Null, Array.Empty<StreamConsumer>() },
                null)!;
        }

        private static StreamPendingMessageInfo CreatePendingInfo(string id, string consumer, long idle, int deliveries)
        {
            return (StreamPendingMessageInfo)Activator.CreateInstance(
                typeof(StreamPendingMessageInfo),
                BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                null,
                new object[] { (RedisValue)id, (RedisValue)consumer, idle, deliveries },
                null)!;
        }

        private static int Compare(string x, string y)
        {
            var (xm, xs) = Parse(x);
            var (ym, ys) = Parse(y);
            return xm != ym ? xm.CompareTo(ym) : xs.CompareTo(ys);
        }

        private static (ulong Ms, ulong Seq) Parse(string id)
        {
            var parts = id.Split('-');
            return (ulong.Parse(parts[0]), parts.Length > 1 ? ulong.Parse(parts[1]) : 0);
        }

        private static string Key(object? o) => ((RedisKey)o!).ToString();

        private static string Value(object? o) => ((RedisValue)o!).ToString();

        private class GroupState
        {
            public string LastDeliveredId { get; set; } = "0-0";

            public Dictionary<string, PendingState> Pending { get; } = new Dictionary<string, PendingState>();

            public Dictionary<string, long> Consumers { get; } = new Dictionary<string, long>();
        }

        public class FakeMultiplexer : DispatchProxy
        {
            public IDatabase Database { get; set; } = null!;

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                if (targetMethod!.Name == "GetDatabase")
                    return Database;
                if (targetMethod.Name is "Dispose")
                    return null;
                if (targetMethod.Name is "DisposeAsync")
                    return ValueTask.CompletedTask;
                throw new NotSupportedException(targetMethod.Name);
            }
        }
    }

    public record PendingState(string Consumer, int DeliveryCount, long LastDeliveredMs);
}
