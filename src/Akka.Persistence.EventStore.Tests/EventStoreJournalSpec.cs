namespace Akka.Persistence.EventStore.Tests
{
    using System;

    using Akka.Configuration;
    using Akka.Persistence.TestKit.Journal;

    public class EventStoreJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
                loglevel = DEBUG
                loggers = [""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""]
                persistence {
                    publish-plugin-commands = off
                    journal {
                        plugin = ""akka.persistence.journal.eventstore""
                        eventstore {
                            class = ""Akka.Persistence.EventStore.EventStoreJournal, Akka.Persistence.EventStore""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            connection-string = ""ConnectTo=tcp://admin:changeit@127.0.0.1:1113""
                            connection-name = ""akka-net""
                        }
                    }
                }
            }
        ");

        private readonly IDisposable disposable;

        static EventStoreJournalSpec()
        {
            Logging.Init();
        }

        public EventStoreJournalSpec()
            : base(SpecConfig, "EventStoreJournalSpec")
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
