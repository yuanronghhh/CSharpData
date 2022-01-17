using System.Collections;
using System;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace CommonLib.Utils
{
    public class TaskQueueData
    {
        public Action<object[]> method { get; set; }
        public object[] param { get; set; }
    }

    public class TaskQueue : ConcurrentQueue<TaskQueueData>
    {
        public void EnqueueTask(Action<object[]> method, params object[] param)
        {
            TaskQueueData qd = new TaskQueueData()
            {
                method = method,
                param = param
            };

            base.Enqueue(qd);
        }
    }

    public class DataConvert
    {
        public static string ObjectToString(object obj)
        {
            if(obj == null) { return ""; }

            return JsonConvert.SerializeObject(obj);
        }

        public static T StringToObject<T>(string str)
        {
            return JsonConvert.DeserializeObject<T>(str);
        }

        public static byte[] ObjectToBytes(object obj)
        {
            if (obj == null) { return new byte[] { }; }

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }

        public static T BytesToObject<T>(byte[] bts)
        {
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream(bts))
            {
                return (T)bf.Deserialize(ms);
            }
        }
    }
}