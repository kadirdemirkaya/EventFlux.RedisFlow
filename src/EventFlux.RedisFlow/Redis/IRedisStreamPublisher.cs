using System.Threading.Tasks;

namespace EventFlux.RedisFlow.Redis
{
    public interface IRedisStreamPublisher
    {
        Task PublishAsync(string eventType, string payload);
    }
}
