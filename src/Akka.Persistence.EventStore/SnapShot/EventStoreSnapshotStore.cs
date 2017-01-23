namespace Akka.Persistence.EventStore
{
    using System;
    using System.ComponentModel;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Akka.Event;
    using Akka.Persistence.Serialization;
    using Akka.Persistence.Snapshot;
    using Akka.Serialization;
    using global::EventStore.ClientAPI;
    using Newtonsoft.Json;

    public class EventStoreSnapshotStore : SnapshotStore
    {
        private readonly EventStoreSettings settings;
        private readonly ILoggingAdapter log;
        private Serializer serializer;

        private IEventStoreConnection connection;

        public EventStoreSnapshotStore()
        {
            settings = EventStorePersistence.Get(Context.System).SnapshotStoreSettings;
            log = Context.GetLogger();

            var serialization = Context.System.Serialization;
            serializer = serialization.FindSerializerForType(typeof(SelectedSnapshot));

            connection = EventStoreConnection.Create(settings.ConnectionString, settings.ConnectionName);
            connection.ConnectAsync().Wait();
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var streamName = GetStreamName(persistenceId);
            var requestedSnapVersion = (int)criteria.MaxSequenceNr;
            StreamEventsSlice slice = null;

            if (criteria.MaxSequenceNr == long.MaxValue)
            {
                requestedSnapVersion = StreamPosition.End;
                slice = await connection.ReadStreamEventsBackwardAsync(streamName, requestedSnapVersion, 1, false);
            }
            else
            {
                slice = await connection.ReadStreamEventsBackwardAsync(streamName, StreamPosition.End, requestedSnapVersion, false);
            }

            if (slice.Status == SliceReadStatus.StreamNotFound)
            {
                await connection.SetStreamMetadataAsync(streamName, ExpectedVersion.Any, StreamMetadata.Data);
                return null;
            }

            if (slice.Events.Any())
            {
                log.Debug("Found snapshot of {0}", persistenceId);

                ResolvedEvent? resolvedEvent = null;
                foreach (var @event in slice.Events)
                {
                    if (requestedSnapVersion == @event.OriginalEvent.EventNumber
                        || requestedSnapVersion == StreamPosition.End)
                    {
                        resolvedEvent = @event;
                    }
                }

                resolvedEvent = resolvedEvent ?? new ResolvedEvent?(slice.Events.First());

                var originalEvent = resolvedEvent.Value.OriginalEvent;

                return (SelectedSnapshot)serializer.FromBinary(originalEvent.Data, typeof(SelectedSnapshot));
            }

            return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var streamName = GetStreamName(metadata.PersistenceId);
            var data = serializer.ToBinary(new SelectedSnapshot(metadata, snapshot));
            var eventData = new EventData(Guid.NewGuid(), typeof(Snapshot).Name, false, data, new byte[0]);

            await connection.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData);
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            // Event store cannot delete
            return Task.FromResult<object>(null);
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            // Event store cannot delete
            return Task.FromResult<object>(null);
        }

        protected override void PreStart()
        {
            base.PreStart();
        }

        private static string GetStreamName(string persistenceId)
        {
            return $"snapshot-{persistenceId}";
        }

        private Task<IEventStoreConnection> GetConnection()
        {
            return null;
            //return connectionFactory.Value;
        }


        public class StreamMetadata
        {
            private static readonly StreamMetadata Instance = new StreamMetadata();

            public static byte[] Data => Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(Instance));

            [JsonProperty("$maxCount")]
            public int MaxCount => 1;

        }
    }
}
