using System;
using System.Collections.Generic;
using System.Reflection;

namespace Commonlib.Reflection
{
    public class ReflectionCommon
    {
        #region 公共方法
        public static T2 ConvertData<T1, T2>(T1 data, PropertyInfo[] toProps = null)
        {
            T2 t2Data = Activator.CreateInstance<T2>();
            PropertyInfo[] props = toProps == null ? typeof(T2).GetProperties() : toProps;
            foreach (PropertyInfo m in props)
            {
                PropertyInfo t1Prop = GetProperty<T1>(m.Name);
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
            if(dataList == null) { return t2List; }

            PropertyInfo[] toProps = typeof(T2).GetProperties();

            foreach (T1 t1 in dataList)
            {
                t2List.Add(ConvertData<T1, T2>(t1, toProps));
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

        public static void SetValue<T>(T data, PropertyInfo dataProp, object value)
        {
            if (dataProp == null || !dataProp.CanWrite)
            {
                return;
            }

            dataProp.SetValue(data, value);
        }

        public static object GetValueDict(Dictionary<string, object> data, string propName)
        {
            if(data == null) { return null; }

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

        public static object GetValue<T>(T data, PropertyInfo dataProp)
        {
            if (dataProp == null || !dataProp.CanRead)
            {
                return null;
            }

            return dataProp.GetValue(data, null);
        }

        public static PropertyInfo GetProperty<T>(string propName)
        {
            PropertyInfo dataProp = typeof(T).GetProperty(propName);
            return dataProp;
        }

        public static List<PropertyInfo> GetFieldProperties<T>(Func<PropertyInfo, bool> isFieldHandle = null)
        {
            Type tp = typeof(T);
            List<PropertyInfo> Fields = new List<PropertyInfo>();

            foreach (PropertyInfo p in tp.GetProperties())
            {
                if (isFieldHandle == null)
                {
                    Fields.Add(p);
                }
                else
                {
                    if (isFieldHandle(p))
                    {
                        Fields.Add(p);
                    }
                }
            }

            return Fields;
        }
        #endregion
    }
}
