using System;
using Akka.Actor;
using Akka.Configuration;

namespace Akka.Persistence.EventStore
{
    using System.Diagnostics;

    public class EventStoreSettings
    {
        public EventStoreSettings(string connectionName, string connectionString)
        {
            ConnectionName = connectionName;
            ConnectionString = connectionString;
        }

        public string ConnectionName { get; private set; }

        public string ConnectionString { get; private set; }

        public static EventStoreSettings Create(Config config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            return new EventStoreSettings(
                connectionName: config.GetString("connection-name"),
                connectionString: config.GetString("connection-string"));
        }
    }

    public class EventStorePersistence : IExtension
    {
        public EventStoreSettings JournalSettings { get; }

        public EventStoreSettings SnapshotStoreSettings { get; }

        public EventStorePersistence(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(DefaultConfig());

            JournalSettings = EventStoreSettings.Create(system.Settings.Config.GetConfig("akka.persistence.journal.eventstore"));
            SnapshotStoreSettings = EventStoreSettings.Create(system.Settings.Config.GetConfig("akka.persistence.snapshot-store.eventstore"));
        }

        public static EventStorePersistence Get(ActorSystem system)
            => system.WithExtension<EventStorePersistence, EventStorePersistenceProvider>();

        public static Config DefaultConfig()
            => ConfigurationFactory.FromResource<EventStorePersistence>("Akka.Persistence.EventStore.reference.conf");
    }

    public class EventStorePersistenceProvider : ExtensionIdProvider<EventStorePersistence>
    {
        public override EventStorePersistence CreateExtension(ExtendedActorSystem system)
        {
            return new EventStorePersistence(system);
        }
    }
}
