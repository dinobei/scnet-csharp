using System;
using System.Collections.Generic;
using Google.Protobuf;

namespace SCNet
{
    public sealed class Registry
    {
        private static Registry instance = null;
        private static readonly object lockObj = new object();
        public delegate void SinkCallback(Scnet.Header header, IMessage message);

        Dictionary<string, uint> nameToTypeMap = new Dictionary<string, uint>();
        Dictionary<uint, MessageParser> typeToParserMap = new Dictionary<uint, MessageParser>();
        Dictionary<uint, SinkCallback> protobufCallbackMap = new Dictionary<uint, SinkCallback>();

        Registry()
        {

        }

        public static Registry Instance
        {
            get
            {
                lock (lockObj)
                {
                    if (instance == null)
                    {
                        instance = new Registry();
                    }
                    return instance;
                }
            }
        }

        public void Add(IMessage message, MessageParser parser, uint packetType, SinkCallback callback)
        {
            nameToTypeMap.Add(message.GetType().FullName, packetType);
            typeToParserMap.Add(packetType, parser);
            protobufCallbackMap.Add(packetType, callback);
        }

        public uint GetType(IMessage message)
        {
            return nameToTypeMap[message.GetType().FullName];
        }

        public IMessage GetProtoMessage(uint packetType, byte[] buffers, int offset, int length)
        {
            IMessage message;
            MessageParser parser = typeToParserMap[packetType];
            message = parser.ParseFrom(buffers, offset, length);
            return message;
        }

        public SinkCallback GetProtobufCallback(uint packetType)
        {
            return protobufCallbackMap[packetType];
        }
    }
}
