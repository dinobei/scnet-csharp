using System;
using Google.Protobuf;
using SCNet;
using Circular2;

namespace ExampleCircular
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
                RegistrationViewerRequest req = new RegistrationViewerRequest()
                {
                    Serial = "SET_UUID",
                    SwVersion = "1.0.0",
                    PriIP = "-",
                    PriPort = 11111
                };
                client.Send(req);
            };
            client.connectingFailedCallback = (Client client) =>
            {
                Console.WriteLine("connectingFailedCallback");
            };
            client.disconnectedCallback = (Client client) =>
            {
                Console.WriteLine("disconnectedCallback");
            };
            client.Attach("127.0.0.1", 9191);

            // Protocol Packet Registration
            Registry registry = Registry.Instance;
            registry.Add(new RegistrationViewerRequest(),
                RegistrationViewerRequest.Parser,
                (int)PacketType.RegistrationViewerRequest,
                null);
            registry.Add(new RegistrationViewerResponse(),
                RegistrationViewerResponse.Parser,
                (int)PacketType.RegistrationViewerResponse,
                (IMessage message) =>
                {
                    RegistrationViewerResponse res = (RegistrationViewerResponse)message;
                    StreamingStartRequest req = new StreamingStartRequest()
                    {
                        AccessToken = "access_token",
                        Serial = "office",
                        Uts = 0
                    };

                    client.Send(req);
                });
            registry.Add(new Circular2.StreamingStartRequest(),
                Circular2.StreamingStartRequest.Parser,
                (int)Circular2.PacketType.StreamingStartRequest, null);
            registry.Add(new Circular2.StreamingStartResponse(),
                Circular2.StreamingStartResponse.Parser,
                (int)Circular2.PacketType.StreamingStartResponse, (IMessage message) =>
                {
                    Circular2.StreamingStartResponse res = (Circular2.StreamingStartResponse)message;
                    if (res.Result == Circular2.Result.Success)
                    {
                        Console.WriteLine("streaming start success");
                    }
                    else
                    {
                        Console.WriteLine("streaming start failed");
                    }
                });
            registry.Add(new Ping(), Ping.Parser, (int)PacketType.Ping, (IMessage message) =>
            {
                Ping ping = (Ping)message;
                client.Send(ping);
            });

            registry.Add(new VideoInitFrame(), VideoInitFrame.Parser, (int)PacketType.VideoInitFrame,
                (IMessage message) =>
                {
                    VideoInitFrame videoInitFrame = (VideoInitFrame)message;
                    Console.WriteLine("VideoInitFrame. uts=" + videoInitFrame.UtsMs);
                });
            registry.Add(new VideoFrame(), VideoFrame.Parser, (int)PacketType.VideoFrame,
                (IMessage message) =>
                {
                    //VideoFrame videoFrame = (VideoFrame)message;
                    //Console.WriteLine("VideoFrame. uts=" + videoFrame.UtsMs);
                });

            while (true)
            {
                string str = Console.ReadLine();

                if (str == "q")
                {
                    Environment.Exit(0);
                }
                else if (str != "")
                {
                    Console.WriteLine("Press 'q' to exit.");
                }
            }

        }
    }
}
