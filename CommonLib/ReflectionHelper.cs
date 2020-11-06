using System;
using System.Collections.Generic;
using System.Reflection;

namespace Commonlib.Reflection
{
    public class ReflectionCommon
    {
        #region 公共方法
        public static T2 ConvertData<T1, T2>(T1 data)
        {
            T2 t2Data = Activator.CreateInstance<T2>();
            PropertyInfo[] props = t2Data.GetType().GetProperties();
            foreach (PropertyInfo m in props)
            {
                PropertyInfo t1Prop = data.GetType().GetProperty(m.Name);
                if (t1Prop == null)
                {
                    continue;
                }

                var dataPropValue = t1Prop.GetValue(data);
                m.SetValue(t2Data, dataPropValue);
            }

            return t2Data;
        }

        public static List<T2> ConvertData<T1, T2>(List<T1> dataList)
        {
            List<T2> t2List = new List<T2>();

            foreach (T1 t1 in dataList)
            {
                t2List.Add(ConvertData<T1, T2>(t1));
            }

            return t2List;
        }

        public static void SetValue<T>(T data, string propName, object value)
        {
            PropertyInfo dataProp = data.GetType().GetProperty(propName);
            if (dataProp == null)
            {
                return;
            }

            dataProp.SetValue(data, value);
        }

        public static object GetValueDict(Dictionary<string, object> data, string propName)
        {
            if (!data.ContainsKey(propName))
            {
                return null;
            }

            return data[propName];
        }

        public static object GetValue<T>(T data, string propName)
        {
            Type tp = data.GetType();

            if (tp == typeof(Dictionary<string, object>))
            {
                return GetValueDict(data as Dictionary<string, object>, propName);
            }

            PropertyInfo dataProp = tp.GetProperty(propName);
            if (dataProp == null)
            {
                return null;
            }

            return dataProp.GetValue(data, null);
        }

        public static PropertyInfo GetProperty<T>(T data, string propName)
        {
            return data.GetType().GetProperty(propName);
        }

        public static object PropertyGetValue<T>(T data, PropertyInfo dataProp)
        {
            return dataProp.GetValue(data, null);
        }
        #endregion

    }
}
