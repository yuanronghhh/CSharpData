using System;
using System.Configuration.Install;
using System.Reflection;
using System.ServiceProcess;

namespace ServiceManager
{
    public class SelfTools
    {
        private static readonly string executePath = Assembly.GetEntryAssembly().Location;

        public static void Install()
        {
            ManagedInstallerClass.InstallHelper(new string[] { executePath });
        }

        public static void Uninstall()
        {
            ManagedInstallerClass.InstallHelper(new string[] { "/u", executePath });
        }
    }

    public partial class SelfManager
    {
        public SelfManager() : base()
        {
            InitializeComponent();
        }

        public static void RunService(MainServiceBase cService)
        {
            ServiceBase[] servicesToRun;

            servicesToRun = new ServiceBase[] { cService };
            ServiceBase.Run(servicesToRun);
        }

        public static void ServiceInteractive(string[] args, MainServiceBase cService)
        {
            if (!Environment.UserInteractive)
            {
                RunService(cService);
                return;
            }

            char keyChar;
            Console.WriteLine("============================================");
            Console.WriteLine("1.RunOnStart(Debug on Console)");
            Console.WriteLine("2.RunOnStop");
            Console.WriteLine("3.安装服务(install)");
            Console.WriteLine("4.卸载服务(uninstall)");
            Console.WriteLine("============================================");

            keyChar = Console.ReadKey().KeyChar;
            Console.WriteLine("");
            switch (keyChar)
            {
                case '1':
                    cService.RunOnStart(args);
                    Console.WriteLine("RunOnStart Done");
                    Console.ReadKey();
                    break;
                case '2':
                    cService.RunOnStop(args);
                    Console.WriteLine("RunOnStop Done");
                    Console.ReadKey();
                    break;
                case '3':
                    SelfTools.Install();
                    break;
                case '4':
                    SelfTools.Uninstall();
                    break;
                default:
                    break;
            }
        }
    }
}
