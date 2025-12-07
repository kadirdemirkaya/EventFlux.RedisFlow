using System;

namespace EventFlux.RedisFlow.Redis
{
    public class RedisStreamOptions
    {
        public string ConnectionString { get; set; } = string.Empty;
        public string StreamName { get; set; } = "event-stream";
        public string ConsumerGroup { get; set; } = "event-group";
        public string ConsumerName { get; set; } = Environment.MachineName;
        public int BatchSize { get; set; } = 10;
    }
}
