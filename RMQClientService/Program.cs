using ServiceManager;

namespace BLLService
{
    static class Program
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        static void Main(string[] args)
        {
            RMQClientService main = new RMQClientService();
            SelfManager.ServiceInteractive(args, main);
        }
    }
}
