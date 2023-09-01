using System.ComponentModel;

namespace ServiceManager
{
    [RunInstaller(true)]
    public partial class SelfInstaller : SelfManager
    {
        public SelfInstaller() : base()
        {
            serviceInstaller.ServiceName = "TimerService";
            serviceInstaller.DisplayName = "TimerService";
            serviceInstaller.Description = "TimerService";
        }
    }
}
