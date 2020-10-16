using System;
using System.Collections.Generic;
using Google.Protobuf;

namespace SCNet
{
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
            if (length > 0)
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
}
