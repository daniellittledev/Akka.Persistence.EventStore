namespace Akka.Persistence.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization.Formatters;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Akka.Actor;
    using Akka.Event;
    using Akka.Persistence.Journal;
    using Akka.Serialization;

    using global::EventStore.ClientAPI;

    using Newtonsoft.Json;

    public class EventStoreJournal : AsyncWriteJournal
    {
        private readonly int batchSize = 500;
        private readonly EventStoreSettings settings;
        private readonly ActorSystem system;

        private readonly ILoggingAdapter log;
        private readonly JsonSerializerSettings serializerSettings;
        private Serializer serializer;

        private IEventStoreConnection connection;

        public EventStoreJournal()
        {
            system = Context.System;
            settings = EventStorePersistence.Get(Context.System).JournalSettings;
            log = Context.GetLogger();

            serializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects,
                TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
                Formatting = Formatting.Indented,
                Converters =
                {
                    new ActorRefConverter(Context)
                }
            };

            var serialization = Context.System.Serialization;
            serializer = serialization.FindSerializerForType(typeof(SelectedSnapshot));

            connection = EventStoreConnection.Create("ConnectTo=tcp://admin:changeit@127.0.0.1:1113;", settings.ConnectionName);
            connection.ConnectAsync().Wait();
        }

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            try
            {
                if (toSequenceNr < fromSequenceNr || max == 0) return;

                if (fromSequenceNr == toSequenceNr)
                {
                    max = 1;
                }

                if (toSequenceNr > fromSequenceNr && max == toSequenceNr)
                {
                    max = toSequenceNr - fromSequenceNr + 1;
                }

                var count = 0L;
                var start = (int)fromSequenceNr - 1;
                var localBatchSize = batchSize;

                StreamEventsSlice slice;
                do
                {
                    if (max == long.MaxValue && toSequenceNr > fromSequenceNr)
                    {
                        max = toSequenceNr - fromSequenceNr + 1;
                    }

                    if (max < localBatchSize)
                    {
                        localBatchSize = (int)max;
                    }

                    try
                    {
                        slice = await connection.ReadStreamEventsForwardAsync(persistenceId, start, localBatchSize, false);
                    }
                    catch (Exception e)
                    {
                        throw;
                    }

                    foreach (var @event in slice.Events)
                    {
                        var jsonText = Encoding.UTF8.GetString(@event.OriginalEvent.Data);
                        var representation = JsonConvert.DeserializeObject<IPersistentRepresentation>(jsonText, serializerSettings);

                        recoveryCallback(representation);
                        count++;

                        if (count == max)
                        {
                            return;
                        }
                    }

                    start = slice.NextEventNumber;

                } while (!slice.IsEndOfStream);
            }
            catch (Exception e)
            {
                log.Error(e, "Error replaying messages for: {0}", persistenceId);
                throw;
            }
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            try
            {
                var slice = await connection.ReadStreamEventsBackwardAsync(persistenceId, StreamPosition.End, 1, false);

                long sequence = 0;

                if (slice.Events.Any())
                {
                    sequence = slice.Events.First().OriginalEventNumber + 1;
                }

                return sequence;
            }
            catch (Exception e)
            {
                log.Error(e, e.Message);
                throw;
            }
        }

        /// <inheritdoc />
        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> atomicWrites)
        {
            var results = new List<Exception>();
            foreach (var atomicWrite in atomicWrites)
            {
                var persistentMessages = (IImmutableList<IPersistentRepresentation>)atomicWrite.Payload;

                var persistenceId = atomicWrite.PersistenceId;
                var lowSequenceId = persistentMessages.Min(c => c.SequenceNr) - 2;

                try
                {
                    var events = persistentMessages.Select(
                        x =>
                            {
                                var json = JsonConvert.SerializeObject(x, serializerSettings);
                                var data = Encoding.UTF8.GetBytes(json);

                                var payloadType = x.Payload.GetType();
                                var metadata = GetMetadataFromPayload(payloadType, x);
                                var eventId = GetEventIdFromPayload(payloadType, x);

                                return new EventData(eventId, x.GetType().FullName, true, data, metadata);
                            }).ToArray();

                    var pendingWrite = new { StreamId = persistenceId, ExpectedSequenceId = lowSequenceId, EventData = events, debugData = persistentMessages };
                    var expectedVersion = pendingWrite.ExpectedSequenceId < 0 ? ExpectedVersion.NoStream : (int)pendingWrite.ExpectedSequenceId;

                    await connection.AppendToStreamAsync(pendingWrite.StreamId, expectedVersion, pendingWrite.EventData);
                    results.Add(null);
                }
                catch (Exception e)
                {
                    results.Add(TryUnwrapException(e));
                }
            }

            return results.ToImmutableList();
        }

        private static Guid GetEventIdFromPayload(Type payloadType, IPersistentRepresentation x)
        {
            var eventIdProperty = payloadType.GetProperty("EventId");
            var eventId = (eventIdProperty != null) ? (Guid)eventIdProperty.GetValue(x.Payload) : Guid.NewGuid();
            return eventId;
        }

        private byte[] GetMetadataFromPayload(Type payloadType, IPersistentRepresentation x)
        {
            var meta = new byte[0];
            var metadataProperty = payloadType.GetProperty("Metadata");
            if (metadataProperty != null)
            {
                var propType = metadataProperty.PropertyType;
                var metaJson = JsonConvert.SerializeObject(metadataProperty.GetValue(x.Payload), propType, this.serializerSettings);
                meta = Encoding.UTF8.GetBytes(metaJson);
            }
            return meta;
        }

        /// <summary>
        /// Delete is not supported in Event Store
        /// </summary>
        /// <param name="persistenceId">
        /// The persistence Id.
        /// </param>
        /// <param name="toSequenceNr">
        /// The to Sequence Nr.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var tcs = new TaskCompletionSource<object>();
            tcs.SetException(new NotSupportedException());
            return tcs.Task;
        }

        internal class ActorRefConverter : JsonConverter
        {
            private readonly IActorContext context;

            public ActorRefConverter(IActorContext context)
            {
                this.context = context;
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(((IActorRef)value).Path.ToStringWithAddress());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.Value == null)
                {
                    return null;
                }

                var value = reader.Value.ToString();
                var selection = context.ActorSelection(value);
                return selection.Anchor;
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof(IActorRef).IsAssignableFrom(objectType);
            }
        }
    }
}
