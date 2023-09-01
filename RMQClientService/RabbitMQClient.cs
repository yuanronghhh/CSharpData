using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CommonLib.RabbitMQ
{
    public class RabbitMQClientBase: IDisposable
    {
        protected ConnectionFactory factory = null;
        protected IConnection connection = null;
        public ManualResetEvent waitHandle = new ManualResetEvent(false);

        public RabbitMQClientBase(string connStr)
        {
            if(string.IsNullOrWhiteSpace(connStr)) { throw new Exception("connStr should not null"); }

            factory = new ConnectionFactory();
            factory.Uri = new Uri(connStr);
            
            connection = GetConnection();
        }

        public IConnection GetConnection()
        {
            if(connection == null || !connection.IsOpen || connection.CloseReason != null)
            {
                connection = factory.CreateConnection();
            }

            return connection;
        }

        public void ListenQosExchangeQueue(string queueName, Func<string, bool> listenQosQueueMethod)
        {
            string consumerTag = null;
            using (IModel channel = GetConnection().CreateModel())
            {
                EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                channel.BasicQos(0, 1, false);

                consumer.Received += (ch, ea) =>
                {
                    byte[] body = ea.Body.ToArray();
                    string str = Encoding.GetEncoding("utf-8").GetString(body);
                    if (listenQosQueueMethod(str))
                    {
                        ((EventingBasicConsumer)ch).Model.BasicAck(ea.DeliveryTag, false);
                    }
                    else
                    {
                        ((EventingBasicConsumer)ch).Model.BasicReject(ea.DeliveryTag, true);
                    }
                };

                consumerTag = channel.BasicConsume(queue: queueName,
                                    autoAck: false,
                                    consumer: consumer);

                waitHandle.WaitOne();
                waitHandle.Dispose();
            }
        }

        public void Dispose()
        {
            waitHandle.Set();
            connection.Close();
        }

        public bool PublishExchangeQueue(string exchangeName, string routingKey, string message, bool durable = true)
        {
            using (IModel channel = GetConnection().CreateModel())
            {
                IBasicProperties props = channel.CreateBasicProperties();
                props.ContentType = "text/plain";
                if (durable)
                {
                    props.Persistent = true;
                    props.DeliveryMode = 2;
                }
                   
                props.Expiration = "259200000";

                byte[] body = Encoding.GetEncoding("utf-8").GetBytes(message);
                channel.BasicPublish(exchangeName, routingKey, props, body);
                return true;
            }
        }

        public void ListenBugFix(string queueName, Func<string, bool> listenQosQueueMethod)
        {
            using (IModel channel = GetConnection().CreateModel())
            {
                channel.ListenQosExchangeQueue(queueName, listenQosQueueMethod);
            }
        }
    }

    public class RabbitMQClient: RabbitMQClientBase
    {
        public RabbitMQClient(string connStr) : base(connStr)
        {
        }
    }

    public static class RMQExtension
    {
        public static void ListenQosExchangeQueue(this IModel model, string queueName, Func<string, bool> listenQosQueueMethod)
        {
        }
    }
}
