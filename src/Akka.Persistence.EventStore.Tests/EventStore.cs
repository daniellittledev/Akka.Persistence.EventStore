namespace Akka.Persistence.EventStore.Tests
{
    using System;
    using System.Net;

    using global::EventStore.ClientAPI.Embedded;
    using global::EventStore.Core;

    public static class EventStore
    {
        public static IDisposable StartEventStore()
        {
            var noEndpoint = new IPEndPoint(IPAddress.None, 0);
            var nodeEndpoint = new IPEndPoint(IPAddress.Loopback, 1113);
            var node =
                EmbeddedVNodeBuilder.AsSingleNode()
                    .RunInMemory()
                    .WithExternalTcpOn(nodeEndpoint)
                    .WithInternalTcpOn(noEndpoint)
                    .WithExternalHttpOn(noEndpoint)
                    .WithInternalHttpOn(noEndpoint)
                    .RunProjections(global::EventStore.Common.Options.ProjectionType.All)
                    .Build();
            node.Start();

            return new NodeStopper(node);
        }

        internal class NodeStopper : IDisposable
        {
            private readonly ClusterVNode node;

            public NodeStopper(ClusterVNode node)
            {
                this.node = node;
            }

            public void Dispose()
            {
                node.Stop();
            }
        }
    }
}
