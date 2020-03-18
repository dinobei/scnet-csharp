using System;
using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace SCNet
{
    static class Utils
    {
        public static void Clear<T>(this BlockingCollection<T> blockingCollection)
        {
            if (blockingCollection == null)
            {
                throw new ArgumentNullException("blockingCollection");
            }

            while (blockingCollection.Count > 0)
            {
                T item;
                blockingCollection.TryTake(out item);
            }
        }
    }
    public sealed class Registry
    {
        private static Registry instance = null;
        private static readonly object lockObj = new object();
        public delegate void RawbyteCallback(byte[] message, int length);
        public delegate void ProtobufCallback(IMessage message);

        Dictionary<string, int> nameToTypeMap = new Dictionary<string, int>();
        Dictionary<int, MessageParser> typeToParserMap = new Dictionary<int, MessageParser>();
        Dictionary<int, RawbyteCallback> rawbyteCallbackMap = new Dictionary<int, RawbyteCallback>();
        Dictionary<int, ProtobufCallback> protobufCallbackMap = new Dictionary<int, ProtobufCallback>();

        Registry()
        {

        }

        public static Registry Instance
        {
            get
            {
                lock(lockObj)
                {
                    if(instance == null)
                    {
                        instance = new Registry();
                    }
                    return instance;
                }
            }
        }

        public void Add(int packetType, RawbyteCallback callback)
        {
            rawbyteCallbackMap.Add(packetType, callback);
        }

        public void Add(IMessage message, MessageParser parser, int packetType, ProtobufCallback callback)
        {
            nameToTypeMap.Add(message.GetType().FullName, packetType);
            typeToParserMap.Add(packetType, parser);
            protobufCallbackMap.Add(packetType, callback);
        }

        public int GetType(IMessage message)
        {
            return nameToTypeMap[message.GetType().FullName];
        }

        public IMessage GetProtoMessage(int packetType, byte[] buffers, int length)
        {
            IMessage message;
            MessageParser parser = typeToParserMap[packetType];
            if(length > 0)
            {
                message = parser.ParseFrom(buffers, 0, length);
            }
            else
            {
                message = parser.ParseFrom(buffers);
            }
            return message;
        }

        public RawbyteCallback GetRawbyteCallback(int packetType)
        {
            return rawbyteCallbackMap[packetType];
        }

        public ProtobufCallback GetProtobufCallback(int packetType)
        {
            return protobufCallbackMap[packetType];
        }
    }

    public struct MessageHeader
    {
        public int dataSize;
        public int packetType;
        public int messageType;
        public int cryptType;
        public int connectionID;
    }

    public enum MessageType
    {
        Protobuf = 0,
        Rawbyte,
    }

    public struct MessageWrapper
    {
        public MessageType messageType;
        public int packetType;
        public object message; // byte[] or Message
        public int length;
    }

    public class Client
    {
        const int MagicPacketLength = 2;
        const int HeaderElements = 5;
        const int MaxPacketHeaderSize = 7 * HeaderElements;

        Socket socket;
        readonly byte[] MagicPacket = { (byte)'I', (byte)'J' };
        
        bool stop = false;
        bool isMainThreadStop = true;
        Thread mainThread;
        Thread sendThread;
        Thread recvThread;

        public delegate void serverCallback();
        public delegate void callback(Client client);
        public callback connectingCallback;
        public callback connectedCallback;
        public callback connectingFailedCallback;
        public callback disconnectedCallback;
        public serverCallback attachedCallback;
        public serverCallback detachedCallback;

        private void Connect(string ip, int port)
        {
            IPHostEntry ipHost = Dns.GetHostEntry(ip);
            IPAddress ipAddr = ipHost.AddressList[0];
            IPEndPoint ipEndPoint = new IPEndPoint(ipAddr, port);
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            IAsyncResult result = socket.BeginConnect(ipEndPoint, null, null);

            bool success = result.AsyncWaitHandle.WaitOne(5000, true);

            if (socket.Connected)
            {
                socket.EndConnect(result);
            }
            else
            {
                // NOTE, MUST CLOSE THE SOCKET

                socket.Close();
                throw new ApplicationException("Failed to connect server.");
            }
        }

        public bool Attach(string ip, int port)
        {
            if (mainThread != null)
            {
                return false;
            }
            isMainThreadStop = false;
            mainThread = new Thread(() => MainThread(ip, port));
            mainThread.Start();
            return true;
        }

        public bool Detach()
        {
            if (mainThread == null)
            {
                Console.WriteLine("Already detached");
                return false;
            }
            stop = true;
            isMainThreadStop = true;

            mainThread.Interrupt();
            return true;
        }

        private MessageHeader MakeHeader(byte[] buffer)
        {
            MessageHeader header = new MessageHeader();
            CodedInputStream codedInput = new CodedInputStream(buffer);
            header.dataSize = codedInput.ReadInt32();
            header.packetType = codedInput.ReadInt32();
            header.messageType = codedInput.ReadInt32();
            header.cryptType = codedInput.ReadInt32();
            header.connectionID = codedInput.ReadInt32();
            codedInput.Dispose();
            return header;
        }

        private void SafeReceive(byte[] buffer, int length)
        {
            int rcvTotal = 0;
            while (true)
            {
                int rcv = socket.Receive(buffer, rcvTotal, length - rcvTotal, SocketFlags.None);
                if(rcv == 0)
                {
                    socket.Close();
                    continue;
                }
                rcvTotal += rcv;
                if (rcvTotal >= length)
                {
                    break;
                }

            }
        }

        public MessageHeader ReadMessageHeader()
        {
            byte[] mp = new byte[2];
            SafeReceive(mp, 2);
            
            if (mp[0] != MagicPacket[0] || mp[1] != MagicPacket[1])
            {
                throw new SocketException(1);
            }

            byte[] headerBuffers = new byte[MaxPacketHeaderSize];

            int readingHeaderSize = 0;
            int receivedHeaderComponent = 0;
            while (true)
            {
                socket.Receive(headerBuffers, readingHeaderSize, 1, SocketFlags.None);
                
                if((headerBuffers[readingHeaderSize++] & 0xFF) > 127) {
                    continue;
                }

                if(++receivedHeaderComponent == HeaderElements)
                {
                    break;
                }
            }

            MessageHeader header = MakeHeader(headerBuffers);
            return header;
        }
        public void Send(int packetType, byte[] message, int length=0)
        {
            MessageWrapper mw = new MessageWrapper
            {
                message = message,
                length = length,
                messageType = MessageType.Rawbyte,
                packetType = packetType
            };
            queue.Add(mw);
        }

        public void Send(int packetType, string message)
        {
            MessageWrapper mw = new MessageWrapper
            {
                message = Encoding.UTF8.GetBytes(message),
                length = message.Length,
                messageType = MessageType.Rawbyte,
                packetType = packetType
            };
            queue.Add(mw);
        }

        public void Send(IMessage message)
        {
            Registry registry = Registry.Instance;
            MessageWrapper mw = new MessageWrapper
            {
                message = message,
                length = message.CalculateSize(),
                messageType = MessageType.Protobuf,
                packetType = registry.GetType(message)
            };
            queue.Add(mw);
        }

        public void MainThread(string ip, int port)
        {
            attachedCallback?.Invoke();
            connectingCallback?.Invoke(this);

            while (!isMainThreadStop)
            {
                try
                {
                    Connect(ip, port);
                }
                catch (Exception)
                {
                    Thread.Sleep(1000);
                    connectingFailedCallback?.Invoke(this);
                    continue;
                }
                
                stop = false;
                sendThread = new Thread(new ThreadStart(SendThread));
                sendThread.Start();
                recvThread = new Thread(new ThreadStart(RecvThread));
                recvThread.Start();

                Thread.Sleep(500);
                connectedCallback?.Invoke(this);

                try
                {
                    sendThread.Join();
                    recvThread.Join();
                }
                catch(ThreadInterruptedException e)
                {
                    socket.Close();
                }

                disconnectedCallback?.Invoke(this);
            }

            //Console.WriteLine("MainThread finished");
            mainThread = null;
            sendThread = null;
            recvThread = null;

            detachedCallback?.Invoke();
        }

        private BlockingCollection<MessageWrapper> queue = new BlockingCollection<MessageWrapper>(10);
        public void SendThread()
        {
            Utils.Clear<MessageWrapper>(queue);
            while(!stop)
            {
                try
                {
                    MessageWrapper messageWrapper;
                    if (!queue.TryTake(out messageWrapper, 1000))
                    {
                        continue; // send timeout
                    }
                    switch (messageWrapper.messageType)
                    {
                        case MessageType.Protobuf:
                            {
                                IMessage message = (IMessage)messageWrapper.message;
                                int size = MagicPacketLength + MaxPacketHeaderSize + messageWrapper.length;
                                byte[] buf = new byte[size];
                                CodedOutputStream co = new CodedOutputStream(buf);
                                co.WriteRawTag(MagicPacket[0], MagicPacket[1]);
                                co.WriteInt32(messageWrapper.length);
                                co.WriteInt32(messageWrapper.packetType);
                                co.WriteInt32((int)MessageType.Protobuf);
                                co.WriteInt32(0);
                                co.WriteInt32(0);

                                message.WriteTo(co);
                                co.Flush();

                                try
                                {
                                    socket.Send(buf, (int)co.Position, SocketFlags.None);
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e.ToString());
                                }
                                finally
                                {
                                    co.Dispose();
                                }

                            }
                            break;
                        case MessageType.Rawbyte:
                            {
                                byte[] message = (byte[])messageWrapper.message;
                                int size = MagicPacketLength + MaxPacketHeaderSize + messageWrapper.length;
                                byte[] buf = new byte[size];
                                CodedOutputStream co = new CodedOutputStream(buf);
                                co.WriteRawTag(MagicPacket[0], MagicPacket[1]);
                                co.WriteInt32(messageWrapper.length);
                                co.WriteInt32(messageWrapper.packetType);
                                co.WriteInt32((int)MessageType.Rawbyte);
                                co.WriteInt32(0);
                                co.WriteInt32(0);
                                //co.WriteBytes(ByteString.CopyFrom(message));
                                Buffer.BlockCopy(message, 0, buf, (int)co.Position, messageWrapper.length);

                                co.Flush();

                                try
                                {
                                    socket.Send(buf, (int)co.Position + messageWrapper.length, SocketFlags.None);
                                }
                                catch (SocketException e)
                                {
                                    if (e.ErrorCode == 10053)
                                    {
                                        stop = true;
                                    }
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e.ToString());
                                }
                                finally
                                {
                                    co.Dispose();
                                }
                            }
                            break;
                        default:
                            Console.WriteLine("Invalid message type: " + messageWrapper.messageType);
                            break;
                    }
                }
                catch(ThreadInterruptedException)
                {
                    stop = true;
                    break;
                }
            }

            //Console.WriteLine("SendThread finished");
        }

        public void RecvThread()
        {
            socket.ReceiveTimeout = 1000;

            Registry registry = Registry.Instance;
            while (!stop)
            {
                try
                {
                    MessageHeader messageHeader = ReadMessageHeader();
                    byte[] buf = new byte[messageHeader.dataSize];
                    if (messageHeader.dataSize != 0)
                    {
                        SafeReceive(buf, messageHeader.dataSize);
                    }

                    switch (messageHeader.messageType)
                    {
                        case (int)MessageType.Protobuf:
                            {
                                IMessage message = registry.GetProtoMessage(messageHeader.packetType, buf, messageHeader.dataSize);
                                Registry.ProtobufCallback cb = registry.GetProtobufCallback(messageHeader.packetType);
                                cb(message);
                            }
                            break;
                        case (int)MessageType.Rawbyte:
                            {
                                Registry.RawbyteCallback cb = registry.GetRawbyteCallback(messageHeader.packetType);
                                cb(buf, messageHeader.dataSize);
                            }
                            break;
                        default:
                            Console.WriteLine("Received invalid messageType : " + messageHeader.messageType);
                            break;
                    }

                }
                catch (SocketException e)
                {
                    if(e.ErrorCode == 1)
                    {
                        Console.WriteLine("Received invalid magic packet");
                    }
                    else if(e.ErrorCode == 10060)
                    {
                        //timeout
                    }
                    else if(e.ErrorCode == 10053 || e.ErrorCode == 10054)
                    {
                        // Closed socket by server
                        stop = true;
                    }
                    else
                    {
                        Console.WriteLine(e.ToString() + "ErrorCode: " + e.ErrorCode);
                    }
                }
                catch(ObjectDisposedException)
                {
                    stop = true;
                }
                catch(ThreadInterruptedException)
                {
                    stop = true;
                }
                catch(Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
            //Console.WriteLine("RecvThread finished");
        }
    }
}
