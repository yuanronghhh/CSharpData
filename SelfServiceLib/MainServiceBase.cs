using System;
using System.IO;
using System.ServiceProcess;

namespace ServiceManager
{
    public partial class MainServiceBase : ServiceBase
    {
        public MainServiceBase()
        {
            InitializeComponent();
        }

        public void RunOnStart(string[] args)
        {
            OnStart(args);
        }

        public void RunOnStop(string[] args)
        {
            OnStop();
        }
    }
}
