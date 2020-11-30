using log4net;
using MongoDB.Bson;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
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
            Thread th = new Thread(new ParameterizedThreadStart((obj) =>
            {
                string consumerTag = null;
                using (IModel channel = GetConnection().CreateModel())
                {
                    try
                    {
                        //订阅模式 (有消息到达将被自动接收) 消费者  
                        EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                        //输入1，那如果接收一个消息，但是没有应答，则客户端不会收到下一个消息
                        channel.BasicQos(0, 1, false);

                        //注册接收事件  
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

                        //要释放的资源  
                        consumerTag = channel.BasicConsume(queue: queueName,
                                            autoAck: false,//和tcp协议的ack一样，为false则服务端必须在收到客户端的回执（ack）后才能删除本条消息
                                            consumer: consumer);
                        LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType).Info(string.Format(queueName + "侦听开始"));

                        waitHandle.WaitOne();
                        waitHandle.Dispose();
                    }
                    catch (Exception ex)
                    {
                        LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType).Error("侦听异常{0}：" + ex);
                    }
                    finally
                    {
                        if (!string.IsNullOrEmpty(consumerTag))
                        {
                            channel.BasicCancel(consumerTag);//当不希望继续订阅时,取消订阅(消费者) 
                        }
                        LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType).Info(string.Format(queueName + "侦听结束"));
                    }
                }
            }));
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
                LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType).Error("推送异常{0}：" + ex);
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
