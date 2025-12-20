using System;
using System.Collections.Generic;

namespace EventFlux.RedisFlow.Abstractions
{
    public class EventContext
    {
        public string StreamId { get; set; } = string.Empty;
        public string Version { get; set; } = string.Empty;
        public Dictionary<string, string> Configs { get; set; } = new Dictionary<string, string>();
        public DateTime Timestamp { get; set; }
    }
}
