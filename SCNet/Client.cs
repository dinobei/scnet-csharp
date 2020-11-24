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
    public class Client
    {
        const int MagicPacketLength = 2;

        Socket socket;
        readonly byte[] MagicPacket = { (byte)'I', (byte)'J' };
        
        bool stop = false;
        bool isMainThreadStop = true;
        Thread mainThread;
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
            IPAddress addr = IPAddress.Parse(ip);
            IPEndPoint ipEndPoint = new IPEndPoint(addr, port);
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveBufferSize = 5 * 1024 * 1024
            };
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
                return false;
            }
            stop = true;
            isMainThreadStop = true;

            mainThread.Interrupt();
            return true;
        }

        private void SafeReceive(byte[] buffer, int offset, int length)
        {
            int rcvTotal = 0;
            while (true)
            {
                int rcv = socket.Receive(buffer, offset + rcvTotal, length - rcvTotal, SocketFlags.None);
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
                recvThread = new Thread(new ThreadStart(RecvThread));
                recvThread.Start();

                Thread.Sleep(500);
                connectedCallback?.Invoke(this);

                try
                {
                    recvThread.Join();
                }
                catch(ThreadInterruptedException e)
                {
                    socket.Close();
                }

                disconnectedCallback?.Invoke(this);
            }

            mainThread = null;
            recvThread = null;

            detachedCallback?.Invoke();
        }

        public void RecvThread()
        {
            socket.ReceiveTimeout = 1000;

            Registry registry = Registry.Instance;
            while (!stop)
            {
                try
                {
                    const int headLength = MagicPacketLength + 4 + 2;
                    byte[] headBuf = new byte[headLength];
                    SafeReceive(headBuf, 0, headLength);
                    if(headBuf[0] != MagicPacket[0] || headBuf[1] != MagicPacket[1])
                    {
                        throw new SocketException(1);
                    }
                    int packetSize = headBuf[2] << 24 | headBuf[3] << 16 | headBuf[4] << 8 | headBuf[5];
                    int headerSize = headBuf[6] << 8 | headBuf[7];
                    byte[] packetBuf = new byte[packetSize];

                    if(packetSize > 0) SafeReceive(packetBuf, 0, packetSize);
                    Scnet.Header header = Scnet.Header.Parser.ParseFrom(packetBuf, 0, headerSize);
                    IMessage message = registry.GetProtoMessage(header.PacketType, packetBuf, headerSize, packetSize-headerSize);
                    if(header.ResCb>0)
                    {
                        DedicatedCallback dedicatedCallback = dedicatedCallbackMap[header.ResCb];
                        bool? result = dedicatedCallback?.Invoke(header, message);
                        if(result.HasValue ? result.Value:false)
                        {
                            Registry.SinkCallback sinkCallback = registry.GetProtobufCallback(header.PacketType);
                            sinkCallback?.Invoke(header, message);
                        }
                    }
                    else {
                        Registry.SinkCallback sinkCallback = registry.GetProtobufCallback(header.PacketType);
                        sinkCallback?.Invoke(header, message);
                    }
                }
                catch (SocketException e)
                {
                    if(e.ErrorCode == 1)
                    {
                        Console.WriteLine("Received invalid magic packet");
                        stop = true;
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
        }

        public delegate bool DedicatedCallback(Scnet.Header header, IMessage message);
        Dictionary<int, DedicatedCallback> dedicatedCallbackMap = new Dictionary<int, DedicatedCallback>();
        int callbackKeyRef = 1000;

        public void Send(Scnet.Header _header, IMessage message, DedicatedCallback callback)
        {
            Registry registry = Registry.Instance;
            uint packetType = registry.GetType(message);
            Scnet.Header header = new Scnet.Header
            {
                PacketType = (uint)packetType
            };
            if(callback != null)
            {
                header.ReqCb = callbackKeyRef;
                dedicatedCallbackMap[callbackKeyRef] = callback;
                callbackKeyRef += 1;
            }
            
            if (_header != null)
            {
                header.ResCb = _header.ReqCb;
            }

            int packetSize = header.CalculateSize() + message.CalculateSize();
            int headerSize = header.CalculateSize();
            byte[] buf = new byte[MagicPacketLength + 4 + 2 + packetSize];
            CodedOutputStream co = new CodedOutputStream(buf);
            co.WriteRawTag(MagicPacket[0], MagicPacket[1]);
            co.WriteRawTag((byte)((packetSize << 24) & 0xFF));
            co.WriteRawTag((byte)((packetSize << 16) & 0xFF));
            co.WriteRawTag((byte)((packetSize << 8) & 0xFF));
            co.WriteRawTag((byte)(packetSize & 0xFF));
            co.WriteRawTag((byte)((headerSize << 8) & 0xFF));
            co.WriteRawTag((byte)(headerSize & 0xFF));
            header.WriteTo(co);
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
    }
}
