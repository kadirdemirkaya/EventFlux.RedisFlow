using EventFlux.Abstractions;
using EventFlux.Options;
using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Tests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class RegistrationTests
    {
        private static readonly IConfiguration Configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RedisStream:ConnectionString"] = "localhost:6379",
                ["RedisStream:StreamName"] = "orders",
                ["RedisStream:ConsumerGroup"] = "billing",
            })
            .Build();

        private static ServiceProvider Build(IServiceCollection services)
        {
            services.AddLogging();
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
        }

        [Fact]
        public async Task AddRedisEventQueue_WithConfigurationOnly_RegistersAndValidates()
        {
            var services = new ServiceCollection();
            services.AddRedisEventQueue(Configuration);

            await using var provider = Build(services);
            using var scope = provider.CreateScope();

            Assert.NotNull(scope.ServiceProvider.GetRequiredService<IEventBus>());
            Assert.Contains(provider.GetServices<IHostedService>(), s => s is Workers.RedisStreamWorker);
            Assert.Equal("orders", provider.GetRequiredService<IOptions<RedisStreamOptions>>().Value.StreamName);
        }

        [Fact]
        public async Task AddRedisEventQueue_WithAssembly_RegistersAndValidates()
        {
            var services = new ServiceCollection();
            services.AddRedisEventQueue(Configuration, typeof(OrderPlaced).Assembly);

            await using var provider = Build(services);
            using var scope = provider.CreateScope();

            Assert.NotNull(scope.ServiceProvider.GetRequiredService<IEventBus>());
            Assert.Single(scope.ServiceProvider.GetServices<IEventHandler<OrderPlaced>>());
        }

        [Fact]
        public async Task AddRedisEventQueue_WithNamedArguments_AppendsGroupSuffix()
        {
            var services = new ServiceCollection();
            services.AddRedisEventQueue(Configuration, assemblies: new[] { typeof(OrderPlaced).Assembly }, appendGuidToConsumerGroup: true);

            await using var provider = Build(services);

            Assert.StartsWith("billing-", provider.GetRequiredService<IOptions<RedisStreamOptions>>().Value.ConsumerGroup);
        }

        [Fact]
        public async Task AddRedisEventQueue_WithAssembly_InvokesHandlerOnce()
        {
            Probe.Reset();
            var services = new ServiceCollection();
            services.AddRedisEventQueue(Configuration, typeof(OrderPlaced).Assembly);

            await using var provider = Build(services);
            using var scope = provider.CreateScope();
            await scope.ServiceProvider.GetRequiredService<IEventBus>().PublishAsync(new OrderPlaced { OrderId = 1 }, CancellationToken.None);

            Assert.Equal(1, Probe.Calls);
        }

        [Fact]
        public async Task AddRedisEventQueue_WithUserAddEventBusBeforeAndAfter_RegistersHandlerOnce()
        {
            Probe.Reset();
            var services = new ServiceCollection();
            services.AddEventBus(typeof(OrderPlaced).Assembly);
            services.AddRedisEventQueue(Configuration, typeof(OrderPlaced).Assembly);
            services.AddEventBus(typeof(OrderPlaced).Assembly);

            Assert.Single(services, d => d.ServiceType == typeof(IEventHandler<OrderPlaced>));

            await using var provider = Build(services);
            using var scope = provider.CreateScope();
            await scope.ServiceProvider.GetRequiredService<IEventBus>().PublishAsync(new OrderPlaced { OrderId = 1 }, CancellationToken.None);

            Assert.Equal(1, Probe.Calls);
        }

        [Fact]
        public async Task AddRedisEventQueue_WithoutAssemblies_UsesHandlersFromUserAddEventBus()
        {
            Probe.Reset();
            var services = new ServiceCollection();
            services.AddRedisEventQueue(Configuration);
            services.AddEventBus(typeof(OrderPlaced).Assembly);

            await using var provider = Build(services);
            using var scope = provider.CreateScope();
            await scope.ServiceProvider.GetRequiredService<IEventBus>().PublishAsync(new OrderPlaced { OrderId = 1 }, CancellationToken.None);

            Assert.Equal(1, Probe.Calls);
        }

        [Fact]
        public void AddRedisEventQueue_AfterUserAddEventBusWithOptions_KeepsConfiguredHandlerLifetime()
        {
            var services = new ServiceCollection();
            services.AddEventBus(o => o.HandlerLifetime = ServiceLifetime.Scoped, typeof(OrderPlaced).Assembly);
            services.AddRedisEventQueue(Configuration, typeof(OrderPlaced).Assembly);

            var descriptor = Assert.Single(services, d => d.ServiceType == typeof(IEventHandler<OrderPlaced>));
            Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
        }
    }
}
