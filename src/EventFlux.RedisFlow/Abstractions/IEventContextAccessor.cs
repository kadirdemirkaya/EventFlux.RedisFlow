namespace EventFlux.RedisFlow.Abstractions
{
    public interface IEventContextAccessor
    {
        EventContext? EventContext { get; set; }
    }
}
