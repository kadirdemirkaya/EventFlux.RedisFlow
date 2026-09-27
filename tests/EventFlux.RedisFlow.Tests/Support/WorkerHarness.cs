using EventFlux.RedisFlow.Abstractions;
using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Tests.Fakes;
using EventFlux.RedisFlow.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventFlux.RedisFlow.Tests.Support
{
    public sealed class WorkerHarness : IAsyncDisposable
    {
        public const string Stream = "orders";
        public const string Group = "billing";
        public const string Consumer = "worker-1";

        private readonly ServiceProvider _provider;
        private CancellationTokenSource? _running;

        private WorkerHarness(FakeStreamDatabase fake, ServiceProvider provider, RedisStreamWorker worker, RedisStreamOptions options)
        {
            Fake = fake;
            _provider = provider;
            Worker = worker;
            Options = options;
        }

        public FakeStreamDatabase Fake { get; }

        public RedisStreamWorker Worker { get; }

        public RedisStreamOptions Options { get; }

        public static WorkerHarness Create(params (string Key, string Value)[] settings)
        {
            return Create(FakeStreamDatabase.Create(), Group, settings);
        }

        public static WorkerHarness Create(FakeStreamDatabase fake, string group, params (string Key, string Value)[] settings)
        {
            var values = new Dictionary<string, string?>
            {
                ["RedisStream:ConnectionString"] = "fake:6379",
                ["RedisStream:StreamName"] = Stream,
                ["RedisStream:ConsumerGroup"] = group,
                ["RedisStream:ConsumerName"] = Consumer + "-" + group,
            };
            foreach (var (key, value) in settings)
                values["RedisStream:" + key] = value;
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddEventBus(typeof(WorkerHarness).Assembly);
            services.Configure<RedisStreamOptions>(configuration.GetSection("RedisStream"));
            services.AddScoped<IEventContextAccessor, EventContextAccessor>();
            services.AddSingleton(fake.Multiplexer);
            EventTypeResolver.RegisterEventsFromAssembly(typeof(WorkerHarness).Assembly);

            var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
            var options = provider.GetRequiredService<IOptions<RedisStreamOptions>>();
            var worker = new RedisStreamWorker(fake.Multiplexer, options, provider, NullLogger<RedisStreamWorker>.Instance);
            return new WorkerHarness(fake, provider, worker, options.Value);
        }

        public string Publish(string type, string data, params NameValueEntry[] extra)
        {
            return Fake.Add(Stream, new[] { new NameValueEntry("type", type), new NameValueEntry("data", data) }.Concat(extra).ToArray());
        }

        public async Task StartAsync()
        {
            _running = new CancellationTokenSource();
            await Worker.StartAsync(_running.Token);
        }

        public async Task RunUntilAsync(Func<bool> condition, int timeoutMs = 5000)
        {
            await StartAsync();
            var until = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition() && DateTime.UtcNow < until && Worker.ExecuteTask is { IsCompleted: false })
                await Task.Delay(20);
            await StopAsync();
        }

        public static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
        {
            var until = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition() && DateTime.UtcNow < until)
                await Task.Delay(20);
            return condition();
        }

        public async Task RunForAsync(int ms)
        {
            await StartAsync();
            await Task.Delay(ms);
            await StopAsync();
        }

        public async Task StopAsync(CancellationToken shutdownTimeout = default)
        {
            if (_running == null)
                return;
            await Worker.StopAsync(shutdownTimeout);
            _running.Dispose();
            _running = null;
            if (Worker.ExecuteTask is { IsFaulted: true } faulted)
                throw faulted.Exception!.GetBaseException();
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_running != null)
                    await Worker.StopAsync(CancellationToken.None);
            }
            catch
            {
            }
            Worker.Dispose();
            await _provider.DisposeAsync();
        }
    }
}
