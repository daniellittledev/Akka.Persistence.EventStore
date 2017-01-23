namespace Akka.Persistence.EventStore.Tests
{
    using System;

    using Akka.Configuration;
    using Akka.Persistence.TestKit.Snapshot;

    public class EventStoreSnapshotSpec : SnapshotStoreSpec
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
                loglevel = DEBUG
                loggers = [""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""]
                persistence {
                    publish-plugin-commands = off
                    snapshot-store {
                        plugin = ""akka.persistence.snapshot-store.eventstore""
                        eventstore {
                            class = ""Akka.Persistence.EventStore.EventStoreSnapshotStore, Akka.Persistence.EventStore""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            connection-string = ""ConnectTo=tcp://admin:changeit@127.0.0.1:1113;""
                            connection-name = ""akka-net""
                        }
                    }
                }
            }
        ");

        private readonly IDisposable disposable;

        static EventStoreSnapshotSpec()
        {
            Logging.Init();
        }

        public EventStoreSnapshotSpec()
            : base(SpecConfig, "EventStoreSnapshotSpec")
        {
            disposable = EventStore.StartEventStore();
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            disposable.Dispose();
        }

    }

}
