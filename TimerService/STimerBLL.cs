using CommonLib.STimer;
using System;
using System.Collections.Generic;
using System.Threading;

namespace BLLService
{
    public class STimerBLL
    {
        STimer ter = new STimer();

        public void ThreadCallback(object o)
        {
            STimer ter = (STimer)o;
            while (true)
            {
                DateTime tm3 = DateTime.Now.AddSeconds(2);

                ter.AddAcTimer(tm3, (td2, d2) =>
                {
                    Console.WriteLine("[ThreadCallback] {0}", td2.TriggerTime.ToString("HH:mm:ss"));
                    return true;
                }, null);
                Thread.Sleep(3 * 1000);
            }
        }

        public void Start()
        {
            DateTime now = DateTime.Now;
            DateTime tm = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0);

            Thread th = new Thread(ThreadCallback);
            th.IsBackground = true;
            th.Start(ter);

            List<string> list = new List<string>();


            for(int i = 0; i < 1000; i++)
            {
                ter.AddAcLoopTimer(now, 5, (td, userdata) =>
                {
                    // Console.WriteLine("ok");
                    return true;
                }, null);
            }
            ter.Run();
        }

        public void Stop()
        {
            ter.Stop();
        }
    }
}
