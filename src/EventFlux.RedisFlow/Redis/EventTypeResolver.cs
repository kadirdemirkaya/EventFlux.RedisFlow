using EventFlux.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace EventFlux.RedisFlow.Redis
{
    public static class EventTypeResolver
    {
        private static readonly ConcurrentDictionary<string, Type> _map = new();

        public static void RegisterEvent<T>() where T : IEventRequest
        {
            _map.TryAdd(GetEventTypeName(typeof(T)), typeof(T));
        }

        public static void RegisterEvent(Type eventType)
        {
            if (!typeof(IEventRequest).IsAssignableFrom(eventType))
                throw new ArgumentException("Type must implement IEventRequest", nameof(eventType));

            _map.TryAdd(GetEventTypeName(eventType), eventType);
        }

        public static void RegisterEventsFromAssembly(Assembly assembly)
        {
            var types = assembly.GetTypes()
                .Where(t => typeof(IEventRequest).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract);

            foreach (var t in types)
            {
                _map.TryAdd(GetEventTypeName(t), t);
            }
        }

        public static Type Resolve(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (_map.TryGetValue(name, out var t))
                return t;

            var loaded = AppDomain.CurrentDomain.GetAssemblies();
            foreach (var la in loaded)
            {
                try
                {
                    var found = la.GetTypes().FirstOrDefault(x => x.Name == name || x.FullName == name);
                    if (found != null && typeof(IEventRequest).IsAssignableFrom(found))
                    {
                        _map.TryAdd(name, found);
                        return found;
                    }
                }
                catch { }
            }

            throw new KeyNotFoundException($"Event type '{name}' is not registered. Call EventTypeResolver.RegisterEvent<T>() or RegisterEventsFromAssembly(...), or ensure the type is loadable in the AppDomain.");
        }

        internal static string GetEventTypeName(Type eventType)
        {
            return eventType.Name;
        }

        public static IReadOnlyDictionary<string, Type> GetRegisteredEventTypes()
        {
            return new Dictionary<string, Type>(_map);
        }
    }
}
