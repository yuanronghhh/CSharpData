using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace CommonLib.STimer
{
    public class STimer
    {
        public enum STimerTypeEnum
        {
            SClockTimer,
            SClockLoopTimer,
            SRepeatTimer
        };

        public enum STimerPeriodType
        {
            SSecond,
            SHour,
            SDay,
            SMonth,           // 每月间隔不一样
        }

        public delegate bool STimerCallback(STimerData cfg, object UserData);

        public LinkedList<STimerData> timeList = new LinkedList<STimerData>();
        private static List<Task<STimerData>> taskList = new List<Task<STimerData>>();

        public class STimerData
        {
            public STimerTypeEnum TimerType { get; set; }
            /// <summary>
            /// 
            /// </summary>
            public DateTime TriggerTime { get; set; }
            /// <summary>
            /// 最小精度为 秒
            /// </summary>
            public int IntervalSeconds { get; set; }
            /// <summary>
            /// 
            /// </summary>
            public STimerCallback Callback { get; set; }
            /// <summary>
            /// 
            /// </summary>
            public LinkedListNode<STimerData> CallbackNode { get; set; }
            /// <summary>
            /// 
            /// </summary>
            public bool CallbackResult { get; set; }
            /// <summary>
            /// 
            /// </summary>
            public int RunCount { get; set; }
            /// <summary>
            /// 
            /// </summary>
            public object UserData { get; set; }
        }

        public static ManualResetEvent mw = new ManualResetEvent(false);
        public static System.Timers.Timer timer = new System.Timers.Timer();
        public static SemaphoreSlim slm = new SemaphoreSlim(3000);

        private void AddTimer(STimerData td)
        {
            timeList.AddLast(td);
        }

        /// <summary>
        /// 定点循环运行，intervalSeconds 为周期 例如 60 为 每分钟，60 * 60 为每小时。
        /// 注意：程序运行时间尽量不要超过间隔时间
        /// </summary>
        /// <param name="time">首次运行时间</param>
        /// <param name="intervalSeconds">间隔时间</param>
        /// <param name="callback"></param>
        /// <param name="userData"></param>
        public void AddAcLoopTimer(DateTime time, int intervalSeconds, STimerCallback callback, object userData)
        {
            STimerData td = new STimerData();

            td.TimerType = STimerTypeEnum.SClockLoopTimer;
            td.TriggerTime = time;
            td.Callback = (td2, d2) =>
            {
                td2.CallbackResult = callback(td2, userData);
                td2.TriggerTime = td2.TriggerTime.AddSeconds(intervalSeconds);

                return false;
            };
            td.UserData = userData;
            td.RunCount = 0;
            AddTimer(td);
        }
        /// <summary>
        /// 周期定点，分类型允许，解决月这种周期不固定情况
        /// </summary>
        /// <param name="time"></param>
        /// <param name="period"></param>
        /// <param name="periodValue"></param>
        /// <param name="callback"></param>
        /// <param name="userData"></param>
        public void AddAcLoopTimer(DateTime time, STimerPeriodType period, int? periodValue, STimerCallback callback, object userData)
        {
            STimerData td = new STimerData();
            periodValue = (!periodValue.HasValue || periodValue <= 0) ? 1 : periodValue.Value;

            td.TimerType = STimerTypeEnum.SClockLoopTimer;
            td.TriggerTime = time;
            td.Callback = (td2, d2) =>
            {
                td2.CallbackResult = callback(td2, userData);

                switch (period)
                {
                    case STimerPeriodType.SSecond:
                        td2.TriggerTime = td2.TriggerTime.AddSeconds(periodValue.Value);
                        break;
                    case STimerPeriodType.SHour:
                        td2.TriggerTime = td2.TriggerTime.AddHours(periodValue.Value);
                        break;
                    case STimerPeriodType.SDay:
                        td2.TriggerTime = td2.TriggerTime.AddDays(periodValue.Value);
                        break;
                    case STimerPeriodType.SMonth:
                        td2.TriggerTime = td2.TriggerTime.AddMonths(periodValue.Value);
                        break;
                }

                return false;
            };
            td.UserData = userData;
            td.RunCount = 0;
            AddTimer(td);
        }

        /// <summary>
        /// 单次定点运行
        /// </summary>
        /// <param name="time"></param>
        /// <param name="callback"></param>
        /// <param name="userData"></param>
        public void AddAcTimer(DateTime time, STimerCallback callback, object userData)
        {
            STimerData td = new STimerData();

            td.TimerType = STimerTypeEnum.SClockTimer;
            td.TriggerTime = time;
            td.Callback = callback;
            td.UserData = userData;
            td.RunCount = 0;

            AddTimer(td);
        }

        /// <summary>
        /// 重复延时执行
        /// </summary>
        /// <param name="secondsSpan"></param>
        /// <param name="callback"></param>
        /// <param name="userData"></param>
        public void AddRepeatTimer(int secondsSpan, STimerCallback callback, object userData)
        {
            STimerData td = new STimerData();

            td.TimerType = STimerTypeEnum.SRepeatTimer;
            td.IntervalSeconds = secondsSpan;
            td.Callback = callback;
            td.UserData = userData;
            td.TriggerTime = DateTime.Now;
            td.RunCount = 0;

            AddTimer(td);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="taskList"></param>
        /// <param name="nnode"></param>
        public void AddTask(List<Task<STimerData>> taskList, LinkedListNode<STimerData> nnode)
        {
            STimerData td = nnode.Value;

            slm.Wait();
            Task<STimerData> tsk = Task.Factory.StartNew(() => {

                td.RunCount += 1;
                td.CallbackNode = nnode;
                nnode.Value.CallbackResult = true;
                nnode.Value.CallbackResult = td.Callback(td, td.UserData);

                return td;
            });
            slm.Release();

            taskList.Add(tsk);
        }

        public void Stop()
        {
            mw.Set();
        }

        public void TimerLoop(object source, ElapsedEventArgs e)
        {
            lock (taskList)
            {
                LinkedListNode<STimerData> node = timeList.First;
                while (node != null)
                {
                    STimerData td = node.Value;
                    DateTime current = DateTime.Now;

                    switch (td.TimerType)
                    {
                        case STimerTypeEnum.SClockTimer:
                        case STimerTypeEnum.SClockLoopTimer:
                            if (td.TriggerTime <= current)
                            {
                                AddTask(taskList, node);
                            }
                            break;
                        case STimerTypeEnum.SRepeatTimer:
                            if ((current - td.TriggerTime).Seconds >= td.IntervalSeconds)
                            {
                                AddTask(taskList, node);
                                td.TriggerTime = current;
                            }
                            break;
                        default:
                            new Exception("No type set on timer.");
                            break;
                    }

                    node = node.Next;
                }
                Task.WaitAll(taskList.ToArray());

                foreach (var t in taskList)
                {
                    LinkedListNode<STimerData> tnode = t.Result.CallbackNode;
                    STimerData ttd = tnode.Value;
                    switch (ttd.TimerType)
                    {
                        case STimerTypeEnum.SClockTimer:
                            timeList.Remove(tnode);
                            break;
                        default:
                            break;
                    }
                }
                taskList.Clear();
            }

        }

        public void Run()
        {
            int interval = 1000; // 每秒

            timer.Enabled = true;
            timer.Interval = interval;
            timer.Elapsed += new ElapsedEventHandler(TimerLoop);
            timer.Start();

            mw.WaitOne();
        }
    }
}
