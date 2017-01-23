namespace Akka.Persistence.EventStore.Tests
{
    using System;

    using Serilog;
    using Serilog.Core;
    using Serilog.Events;

    internal static class Logging
    {
        public static void Init()
        {
            ILogEventSink sink = new TestSink();
            Log.Logger = new LoggerConfiguration()
                .WriteTo.ColoredConsole()
                .WriteTo.Sink(sink)
                .MinimumLevel.Verbose()
                .Enrich.WithProperty("Application", "Akka.Persistence.EventStore")
                .CreateLogger();
        }
    }

    internal class TestSink : ILogEventSink
    {
        public void Emit(LogEvent logEvent)
        {
            if (logEvent.Level == LogEventLevel.Error)
            {
                Console.WriteLine(logEvent.Exception.Message);
            }
        }
    }
}
