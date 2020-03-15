using System;
using System.Text;
using System.Threading;
using Google.Protobuf;
using SCNet;

namespace Example
{
    class Program
    {
        static void Main(string[] args)
        {
            // Connection
            Client client = new Client();
            client.connectingCallback = (Client client) =>
            {
                Console.WriteLine("connecting");
            };
            client.connectedCallback = (Client client) =>
            {
                Console.WriteLine("connected");
            };
            client.connectingFailedCallback = (Client client) =>
            {
                Console.WriteLine("connectingFailed");
            };
            client.disconnectedCallback = (Client client) =>
            {
                Console.WriteLine("disconnected");
            };

            client.Attach("127.0.0.1", 9191);
            
            Registry registry = Registry.Instance;
            registry.Add(0, (byte[] message, int length) =>
            {
                string stringMsg = Encoding.UTF8.GetString(message);
                Console.WriteLine("Received rawbyte. message: " + stringMsg);
                return;
            });
            registry.Add(1, (byte[] message, int length) =>
            {
                string stringMsg = Encoding.UTF8.GetString(message);
                Console.WriteLine("Received rawbyte. message: " + stringMsg);
                return;
            });
            registry.Add(new Example.Packet1(), Example.Packet1.Parser, (int)Example.PacketType._1, 
                (IMessage message) =>
                {
                    Example.Packet1 pkt1 = (Example.Packet1)message;
                    Console.WriteLine("onPacket1() called, Number: " + pkt1.Number);
                }
                );
            registry.Add(new Example.Packet2(), Example.Packet2.Parser, (int)Example.PacketType._2,
                (IMessage message) =>
                {
                    Example.Packet2 pkt2 = (Example.Packet2)message;
                    Console.WriteLine("onPacket1() called, Str: " + pkt2.Str);
                }
                );

            // Send test data
            while (true)
            {
                Thread.Sleep(5000);

                // Example 1: send string
                client.Send(1, "hello world");

                // Example 2: send protobuf message (Example.Packet1)
                Example.Packet1 pkt1 = new Example.Packet1()
                {
                    Number = 123
                };
                client.Send(pkt1);

                // Example 3: send protobuf message (Example.Packet2)
                Example.Packet2 pkt2 = new Example.Packet2()
                {
                    Str = "packet 2 data"
                };
                client.Send(pkt2);
                
                Thread.Sleep(5000);
            }

        }
    }
}
