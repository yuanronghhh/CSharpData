using log4net;
using log4net.Config;
using ServiceManager;
using SuperSocket.WebSocket;
using System;
using System.Threading;

[assembly: XmlConfigurator(ConfigFile = "log4net.config", Watch = true)]

namespace BLLService
{
    public class WebSocketService : MainServiceBase
    {
        public static ILog log = LogManager.GetLogger("WebSocketService");
        public static SServer wsServer = null;
        public ManualResetEvent wo = new ManualResetEvent(false);
        public Thread th = null;

        public WebSocketService() : base()
        {
        }

        public void StartUp(object obj)
        {
            wsServer = new SServer("0.0.0.0", 7071);
            if (!wsServer.Start())
            {
                wo.Set();
                log.Error("websocket server start failed");
            }

            wo.WaitOne();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                log.Info("[Service Starting]");

                th = new Thread(StartUp);
                th.IsBackground = false;
                th.Start();
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }

        protected override void OnStop()
        {
            try
            {
                wsServer.Stop();
                wo.Set();
                th.Join();

                log.Info("[Service Stopping]");
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }
    }
}
