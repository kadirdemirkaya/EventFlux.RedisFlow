using System.Threading.Tasks;

namespace EventFlux.RedisFlow.Redis
{
    using EventFlux.RedisFlow.Abstractions;

    public interface IRedisStreamPublisher
    {
        Task PublishAsync(string eventType, string payload);
        Task PublishAsync(string eventType, string payload, EventContext context);
    }
}
