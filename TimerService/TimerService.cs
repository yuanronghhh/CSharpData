using log4net;
using log4net.Config;
using ServiceManager;
using System;
using System.Threading;

[assembly: XmlConfigurator(ConfigFile = "log4net.config")]

namespace BLLService
{
    public class SDaemonService : MainServiceBase
    {
        public static ILog log = LogManager.GetLogger("TimerService");
        Thread th = null;
        public static STimerBLL ISTimerBLL = new STimerBLL();

        public SDaemonService() : base()
        {
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                log.Info("[Service Starting]");
                th = new Thread(ISTimerBLL.Start);
                th.IsBackground = true;
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
                log.Info("[Service Stopping]");
                ISTimerBLL.Stop();
                th.Join();
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }
    }

    static class SDaemon
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        static void Main(string[] args)
        {
            SDaemonService main = new SDaemonService();
            SelfManager.ServiceInteractive(args, main);
        }
    }
}
