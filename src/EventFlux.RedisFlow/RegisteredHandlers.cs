using System;

namespace EventFlux.RedisFlow
{
    public class RegisteredHandlers
    {
        public RegisteredHandlers((Type iface, Type impl)[] items)
        {
            Items = items ?? Array.Empty<(Type, Type)>();
        }

        public (Type iface, Type impl)[] Items { get; }
    }
}
