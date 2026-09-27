using System;
using System.Reflection;
using System.Linq;
using EventFlux.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventFlux.RedisFlow
{
    using EventFlux.RedisFlow.Abstractions;
    using Redis;

    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRedisEventQueue(
            this IServiceCollection services,
            IConfiguration configuration,
            params Assembly[] assemblies)
        {
            return AddRedisEventQueue(services, configuration, assemblies as Assembly[] ?? null, false, false);
        }

        public static IServiceCollection AddRedisEventQueue(
            this IServiceCollection services,
            IConfiguration configuration,
            Assembly[]? assemblies = null,
            bool appendMachineNameToConsumerGroup = false,
            bool appendGuidToConsumerGroup = false)
        {
            var scanAssemblies = assemblies == null
                ? Array.Empty<Assembly>()
                : assemblies.Where(a => a != null).Distinct().ToArray();

            if (scanAssemblies.Length == 0)
                scanAssemblies = new[] { typeof(ServiceCollectionExtensions).Assembly };

            services.AddEventBus(scanAssemblies);

            services.Configure<RedisStreamOptions>(configuration.GetSection("RedisStream"));

            if (appendMachineNameToConsumerGroup || appendGuidToConsumerGroup)
            {
                var suffix = appendGuidToConsumerGroup
                    ? "-" + Guid.NewGuid().ToString("N")
                    : "-" + Environment.MachineName;

                services.PostConfigure<RedisStreamOptions>(opts =>
                {
                    if (!string.IsNullOrEmpty(opts.ConsumerGroup) && !opts.ConsumerGroup.EndsWith(suffix))
                    {
                        if (appendGuidToConsumerGroup)
                            opts.EphemeralGroupBaseName = opts.ConsumerGroup;

                        opts.ConsumerGroup = opts.ConsumerGroup + suffix;
                    }
                });
            }

            services.AddSingleton<IRedisStreamPublisher, RedisStreamPublisher>();
            services.AddHostedService<Workers.RedisStreamWorker>();
            services.AddHostedService<Hosted.EventDiscoveryLogger>();

            services.AddScoped<IEventContextAccessor, EventContextAccessor>();

            services.AddSingleton<IConnectionMultiplexer>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<RedisStreamOptions>>().Value;
                return ConnectionMultiplexer.Connect(CreateConnectionOptions(opts.ConnectionString));
            });

            foreach (var a in scanAssemblies)
            {
                Redis.EventTypeResolver.RegisterEventsFromAssembly(a);
            }

            var handlerInterface = typeof(IEventHandler<>);
            var registered = new System.Collections.Generic.List<(Type iface, Type impl)>();
            foreach (var asm in scanAssemblies)
            {
                try
                {
                    foreach (var t in asm.GetTypes())
                    {
                        if (!t.IsClass || t.IsAbstract) continue;

                        var ifaces = t.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == handlerInterface);
                        foreach (var iface in ifaces)
                        {
                            services.TryAddEnumerable(ServiceDescriptor.Transient(iface, t));
                            registered.Add((iface, t));
                        }
                    }
                }
                catch { }
            }

            services.AddSingleton(new RegisteredHandlers(registered.ToArray()));

            return services;
        }

        internal static ConfigurationOptions CreateConnectionOptions(string connectionString)
        {
            var options = ConfigurationOptions.Parse(connectionString);

            var abortConnectSet = connectionString
                .Split(',')
                .Select(part => part.Split('=')[0].Trim())
                .Any(key => string.Equals(key, "abortConnect", StringComparison.OrdinalIgnoreCase));

            if (!abortConnectSet)
                options.AbortOnConnectFail = false;

            return options;
        }
    }
}
