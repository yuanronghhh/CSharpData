using log4net;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CommonLib.RabbitMQ
{
    public class RabbitMQClientBase
    {
        protected ConnectionFactory factory = null;
        protected IConnection connection = null;

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
            ManualResetEvent waitHandle = new ManualResetEvent(false);
            Task th = new Task(() =>
            {
                string consumerTag = null;
                using (IModel channel = GetConnection().CreateModel())
                {
                    try
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
                    catch (Exception ex)
                    {
                    }
                    finally
                    {
                        if (!string.IsNullOrEmpty(consumerTag))
                        {
                            channel.BasicCancel(consumerTag);
                        }
                    }
                }
            });
            th.Start();
        }

        public bool PublishExchangeQueue(string exchangeName, string routingKey, string message, bool durable = true)
        {
            try
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
            catch (Exception ex)
            {
                return false;
            }
        }
    }

    public class RabbitMQClient: RabbitMQClientBase
    {
        public RabbitMQClient(string connStr) : base(connStr)
        {
        }
    }
}
