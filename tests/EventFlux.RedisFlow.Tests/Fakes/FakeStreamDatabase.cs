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
                        return Task.FromResult(PendingMessages(Key(a[0]), Value(a[1]), (int)a[2]!, (RedisValue?)a[4]));
                    case "StreamClaimAsync":
                        return Task.FromResult(Claim(Key(a[0]), Value(a[1]), Value(a[2]), (long)a[3]!, (RedisValue[])a[4]!));
                }
            }

            throw new NotSupportedException(targetMethod.Name);
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

        private StreamPendingMessageInfo[] PendingMessages(string stream, string group, int count, RedisValue? minId)
        {
            if (!_groups.TryGetValue((stream, group), out var g))
                throw new RedisServerException("NOGROUP");
            return g.Pending
                .Where(kv => minId == null || minId.Value.IsNull || Compare(kv.Key, minId.Value.ToString()) >= 0)
                .OrderBy(kv => kv.Key, Comparer<string>.Create(Compare))
                .Take(count)
                .Select(kv => CreatePendingInfo(kv.Key, kv.Value.Consumer, NowMs - kv.Value.LastDeliveredMs, kv.Value.DeliveryCount))
                .ToArray();
        }

        private StreamEntry[] Claim(string stream, string group, string consumer, long minIdle, RedisValue[] ids)
        {
            var g = _groups[(stream, group)];
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
