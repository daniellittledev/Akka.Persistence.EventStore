namespace Akka.Persistence.EventStore
{
    using System;
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
        private Lazy<Task<IEventStoreConnection>> connectionFactory;
        private Serializer serializer;

        public EventStoreSnapshotStore()
        {
            settings = EventStorePersistence.Get(Context.System).JournalSettings;
            log = Context.GetLogger();
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var connection = await GetConnection();
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

                var @event = slice.Events
                    .First(t => requestedSnapVersion == StreamPosition.End 
                             || t.OriginalEvent.EventNumber == requestedSnapVersion)
                    .OriginalEvent;

                return (SelectedSnapshot)serializer.FromBinary(@event.Data, typeof(SelectedSnapshot));
            }

            return null;
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var connection = await GetConnection();

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

            var serialization = Context.System.Serialization;
            serializer = serialization.FindSerializerForType(typeof(SelectedSnapshot));

            connectionFactory = new Lazy<Task<IEventStoreConnection>>(
                async () =>
                {
                    try
                    {
                        var eventStoreConnection = EventStoreConnection.Create(settings.ConnectionString, settings.ConnectionName);
                        await eventStoreConnection.ConnectAsync();
                        return eventStoreConnection;
                    }
                    catch (Exception e)
                    {
                        log.Error(e, "Failed to create a connection to EventStore");
                        throw;
                    }
                },
                LazyThreadSafetyMode.ExecutionAndPublication);
        }

        private static string GetStreamName(string persistenceId)
        {
            return $"snapshot-{persistenceId}";
        }

        private Task<IEventStoreConnection> GetConnection()
        {
            return connectionFactory.Value;
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
