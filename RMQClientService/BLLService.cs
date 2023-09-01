using log4net;
using log4net.Config;
using ServiceManager;
using System;
using System.Threading;

[assembly: XmlConfigurator(ConfigFile = "log4net.config", Watch = true)]

namespace BLLService
{
    public class RMQClientService : MainServiceBase
    {
        public static ILog log = LogManager.GetLogger("RMQClientService");
        public static SClient sClient = null;
        Thread th;

        public RMQClientService() : base()
        {
        }
        public void OnService(object obj)
        {
            SClient sClient = (SClient)obj;
            sClient.Run();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                log.Info("[Service Starting]");

                sClient = new SClient();

                th = new Thread(OnService);
                th.Start(sClient);
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
                sClient.Dispose();
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
