namespace EventFlux.RedisFlow.Abstractions
{
    using System.Threading;

    public class EventContextAccessor : IEventContextAccessor
    {
        private static readonly AsyncLocal<EventContext?> _eventContextCurrent = new AsyncLocal<EventContext?>();

        public EventContext? EventContext
        {
            get => _eventContextCurrent.Value;
            set => _eventContextCurrent.Value = value;
        }
    }
}
