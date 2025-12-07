using System;
using System.Reflection;
using System.Linq;
using EventFlux.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventFlux.RedisFlow
{
    using EventFlux.Extensions;
    using Redis;

    public static class ServiceCollectionExtensions
    {
        // Compatibility overload: original signature accepted a params Assembly[]
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
            services.AddEventBus();

            services.Configure<RedisStreamOptions>(configuration.GetSection("RedisStream"));

            if (appendMachineNameToConsumerGroup || appendGuidToConsumerGroup)
            {
                var suffix = appendGuidToConsumerGroup
                    ? "-" + Guid.NewGuid().ToString("N")
                    : "-" + Environment.MachineName;

                // Post configure so we can append after configuration binding
                services.PostConfigure<RedisStreamOptions>(opts =>
                {
                    if (!string.IsNullOrEmpty(opts.ConsumerGroup) && !opts.ConsumerGroup.EndsWith(suffix))
                    {
                        opts.ConsumerGroup = opts.ConsumerGroup + suffix;
                    }
                });
            }

            services.AddSingleton<IRedisStreamPublisher, RedisStreamPublisher>();
            services.AddHostedService<Workers.RedisStreamWorker>();
            services.AddHostedService<Hosted.EventDiscoveryLogger>();

            services.AddSingleton<IConnectionMultiplexer>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<RedisStreamOptions>>().Value;
                return ConnectionMultiplexer.Connect(opts.ConnectionString);
            });

            if (assemblies == null || assemblies.Length == 0)
            {
                Redis.EventTypeResolver.RegisterEventsFromAssembly(typeof(ServiceCollectionExtensions).Assembly);
            }
            else
            {
                foreach (var a in assemblies)
                {
                    if (a != null)
                        Redis.EventTypeResolver.RegisterEventsFromAssembly(a);
                }
            }

            Assembly[] scanAssemblies;
            if (assemblies == null || assemblies.Length == 0)
                scanAssemblies = new[] { typeof(ServiceCollectionExtensions).Assembly };
            else
                scanAssemblies = assemblies.Where(a => a != null).ToArray()!;

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
                            services.AddTransient(iface, t);
                            registered.Add((iface, t));
                        }
                    }
                }
                catch { }
            }

            services.AddSingleton(new RegisteredHandlers(registered.ToArray()));

            return services;
        }
    }
}
