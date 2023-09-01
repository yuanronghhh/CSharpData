using CommonLib.RabbitMQ;
using RabbitMQ.Client;
using System;

namespace BLLService
{
    public class SClient : IDisposable
    {
        public static RabbitMQClient s;

        public bool Handler(string msg)
        {
            return true;
        }

        public SClient()
        {
            string cfg = "amqp://admin:runone%402016@192.168.1.54:5672";
            s = new RabbitMQClient(cfg);
        }
        public void Run()
        {
            s.ListenBugFix("bot.robot.message", Handler);
        }

        public void Dispose()
        {
            s.Dispose();
        }
    }
}
