using System.ComponentModel;

namespace ServiceManager
{
    [RunInstaller(true)]
    public partial class SelfInstaller : SelfManager
    {
        public SelfInstaller() : base()
        {
            serviceInstaller.ServiceName = "RMQClientService";
            serviceInstaller.DisplayName = "RMQClientService";
            serviceInstaller.Description = "RMQClientService";
            serviceInstaller.StartType = System.ServiceProcess.ServiceStartMode.Manual;
        }

        private void InitializeComponent()
        {
            // 
            // serviceProcessInstaller
            // 
            this.serviceProcessInstaller.AfterInstall += new System.Configuration.Install.InstallEventHandler(this.serviceProcessInstaller_AfterInstall);
            // 
            // serviceInstaller
            // 
            this.serviceInstaller.AfterInstall += new System.Configuration.Install.InstallEventHandler(this.serviceInstaller_AfterInstall);

        }

        private void serviceProcessInstaller_AfterInstall(object sender, System.Configuration.Install.InstallEventArgs e)
        {

        }

        private void serviceInstaller_AfterInstall(object sender, System.Configuration.Install.InstallEventArgs e)
        {

        }
    }
}
