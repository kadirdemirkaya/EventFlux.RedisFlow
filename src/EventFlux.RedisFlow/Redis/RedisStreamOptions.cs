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

        /// <summary>
        /// Retries entries whose handler failed and entries left pending by a consumer that stopped.
        /// When <c>false</c>, a failed entry stays pending in the consumer group and is not read again.
        /// Default is <c>true</c>.
        /// </summary>
        public bool EnableRetry { get; set; } = true;

        /// <summary>
        /// Number of deliveries after which a failing entry is moved to <see cref="DeadLetterStreamName"/>
        /// instead of being retried again. Zero or a negative value retries without limit. Default is 5.
        /// </summary>
        public int MaxDeliveryAttempts { get; set; } = 5;

        /// <summary>
        /// How long an entry must stay pending without being acknowledged before it is claimed and retried.
        /// Set it longer than your slowest handler, otherwise an entry still being handled can be claimed again.
        /// Default is 30 seconds.
        /// </summary>
        public TimeSpan RetryIdleTime { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Stream that receives entries that exceeded <see cref="MaxDeliveryAttempts"/>.
        /// When empty, <c>{StreamName}-dead-letter</c> is used.
        /// </summary>
        public string? DeadLetterStreamName { get; set; }

        internal string ResolveDeadLetterStreamName()
        {
            return string.IsNullOrEmpty(DeadLetterStreamName) ? StreamName + "-dead-letter" : DeadLetterStreamName!;
        }
    }
}
