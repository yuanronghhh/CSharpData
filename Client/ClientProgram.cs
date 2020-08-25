using CommonLib.CommonDAL;
using StackExchange.Redis;
using System;

namespace Client
{
    class ClientProgram
    {
        public static void SubMessage(ChannelMessage cm)
        {
            Console.WriteLine((string)cm.Message);
        }

        static void Main(string[] args)
        {
            using (RedisClientService redis = new RedisClientService())
            {
                redis.SubscribeItem("messages", SubMessage);

                Console.WriteLine("{0}", "Listening");
                Console.ReadKey();
            }
        }
    }
}
