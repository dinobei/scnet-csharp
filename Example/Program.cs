using System;
using System.Threading;
using Google.Protobuf;
using SCNet;

namespace Example
{
    class Program
    {
        static void Main(string[] args)
        {
            // Packet registration
            Registry registry = Registry.Instance;
            registry.Add(new Example.DummyPacket1(), Example.DummyPacket1.Parser, (int)Example.PacketType.DummyPacket1,
                (Scnet.Header header, IMessage message) =>
                {
                    Example.DummyPacket1 pkt1 = (Example.DummyPacket1)message;
                    Console.WriteLine("[SinkCallback] onDummyPacket1() called, Title: " + pkt1.Title + "" + pkt1.Number);
                }
                );
            registry.Add(new Example.DummyPacket2(), Example.DummyPacket2.Parser, (int)Example.PacketType.DummyPacket2,
                (Scnet.Header header, IMessage message) =>
                {
                    Example.DummyPacket2 pkt2 = (Example.DummyPacket2)message;
                    Console.WriteLine("[SinkCallback] onDummyPacket2() called, StrArr.Count: " + pkt2.StrArr.Count);
                }
                );
            registry.Add(new Example.Ping(), Example.Ping.Parser, (int)Example.PacketType.Ping,
                (Scnet.Header header, IMessage message) =>
                {
                    Example.Ping ping = (Example.Ping)message;
                    Console.WriteLine("[SinkCallback] Ping");
                });

            // Connect to server
            Client client = new Client
            {
                connectingCallback = (Client client) =>
                {
                    Console.WriteLine("connecting");
                },
                connectedCallback = (Client client) =>
                {
                    Console.WriteLine("connected");
                },
                connectingFailedCallback = (Client client) =>
                {
                    Console.WriteLine("connectingFailed");
                },
                disconnectedCallback = (Client client) =>
                {
                    Console.WriteLine("disconnected");
                }
            };

            client.Attach("127.0.0.1", 9190);

            // Send test packet
            while (true)
            {
                Thread.Sleep(1000);

                // Example 1: send message (Example.DummyPacket1)
                Example.DummyPacket1 pkt1 = new Example.DummyPacket1()
                {
                    Title = "Hello",
                    Number = 123
                };
                client.Send(null, pkt1, (Scnet.Header header, IMessage message) =>
                {
                    Example.DummyPacket1 pkt1 = (Example.DummyPacket1)message;
                    Console.WriteLine("[DedicatedCallback] onDummyPacket1() called, Title: " + pkt1.Title + "" + pkt1.Number);
                    return true;
                });

                Thread.Sleep(1000);

                // Example 2: send message (Example.DummyPacket2)
                Example.DummyPacket2 pkt2 = new Example.DummyPacket2();
                pkt2.StrArr.Add("This");
                pkt2.StrArr.Add("is");
                pkt2.StrArr.Add("sample");
                pkt2.StrArr.Add("text");
                client.Send(null, pkt2, (Scnet.Header header, IMessage message) =>
                {
                    Example.DummyPacket2 pkt2 = (Example.DummyPacket2)message;
                    Console.WriteLine("[DedicatedCallback] onDummyPacket2() called, StrArr.Count: " + pkt2.StrArr.Count);
                    return false;
                });
            }

        }
    }
}
