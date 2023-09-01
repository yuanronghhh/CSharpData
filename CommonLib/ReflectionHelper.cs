using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace Commonlib.Reflection
{
    /// <summary>
    /// 
    /// </summary>
    public class ReflectionCommon
    {
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

            if (dataList != null)
            {
                Type t1Type = typeof(T1);
                PropertyInfo[] t2Props = typeof(T2).GetProperties();

                foreach (T1 t1 in dataList)
                {
                    t2List.Add(ConvertData<T1, T2>(t1, t1Type, t2Props));
                }
            }

            return t2List;
        }

        public static T2 ConvertData<T1, T2>(T1 data, Type t1Type, PropertyInfo[] t2Props)
        {
            try
            {
                T2 t2Data = Activator.CreateInstance<T2>();
                foreach (PropertyInfo m in t2Props)
                {
                    PropertyInfo t1Prop = t1Type.GetProperty(m.Name);
                    if (t1Prop == null || !t1Prop.CanRead || !m.CanWrite)
                    {
                        continue;
                    }

                    var dataPropValue = t1Prop.GetValue(data);
                    m.SetValue(t2Data, dataPropValue);
                }

                return t2Data;
            }
            catch (Exception ex)
            {
            }

            return default(T2);
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
        /// <summary>
        /// 
        /// </summary>
        public struct PropertyMap<T1, T2>
        {
            /// <summary>
            /// 
            /// </summary>
            public string Name;
            /// <summary>
            /// 
            /// </summary>
            public Func<T1, object> t1Getter;
            /// <summary>
            /// 
            /// </summary>
            public Action<T2, object> t2Setter;
        };
        /// <summary>
        /// 
        /// </summary>
        /// <param name="p1"></param>
        /// <returns></returns>
        public static string GetNameMap(PropertyInfo p1)
        {
            return p1.Name;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <returns></returns>
        public static List<PropertyMap<T1, T2>> CreatePMap<T1, T2>(Func<PropertyInfo, string> nameMap = null)
        {
            PropertyInfo t2Prop;
            PropertyInfo[] t1Props = typeof(T1).GetProperties();
            List<PropertyMap<T1, T2>> pmap = new List<PropertyMap<T1, T2>>();

            nameMap = nameMap ?? GetNameMap;

            foreach (PropertyInfo prop in t1Props)
            {
                string mname = nameMap(prop);
                if (string.IsNullOrWhiteSpace(mname))
                {
                    continue;
                }

                t2Prop = typeof(T2).GetProperty(mname);
                if (t2Prop == null || !t2Prop.CanRead || !t2Prop.CanWrite)
                {
                    continue;
                }

                pmap.Add(new PropertyMap<T1, T2>()
                {
                    Name = prop.Name,
                    t1Getter = FastInvoke.BuildUntypedGetter<T1>(prop),
                    t2Setter = FastInvoke.BuildUntypedSetter<T2>(t2Prop),
                });
            }

            return pmap;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="dataList"></param>
        /// <param name="pmap"></param>
        /// <returns></returns>
        public static List<T2> ConvertData2<T1, T2>(List<T1> dataList, List<PropertyMap<T1, T2>> pmap = null)
        {
            List<T2> t2List = new List<T2>();
            if (dataList == null)
            {
                return null;
            }
            pmap = pmap ?? CreatePMap<T1, T2>();

            foreach (T1 t1 in dataList)
            {
                T2 t2 = Activator.CreateInstance<T2>();
                foreach (var mp in pmap)
                {
                    var dataPropValue = mp.t1Getter(t1);
                    mp.t2Setter(t2, dataPropValue);
                }
                t2List.Add(t2);
            }

            return t2List;
        }
        /// <summary>
        /// better performance
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="t1"></param>
        /// <param name="pmap"></param>
        /// <returns></returns>
        public static T2 ConvertData2<T1, T2>(T1 t1, List<PropertyMap<T1, T2>> pmap = null)
        {
            T2 t2 = Activator.CreateInstance<T2>();
            pmap = pmap ?? CreatePMap<T1, T2>();

            foreach (var mp in pmap)
            {
                var dataPropValue = mp.t1Getter(t1);
                mp.t2Setter(t2, dataPropValue);
            }

            return t2;
        }
    }


    /// <summary>
    /// 
    /// </summary>
    public static class FastInvoke
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memberInfo"></param>
        /// <returns></returns>
        public static Func<T, object> BuildUntypedGetter<T>(MemberInfo memberInfo)
        {
            var targetType = memberInfo.DeclaringType;
            var exInstance = Expression.Parameter(targetType, "t");

            var exMemberAccess = Expression.MakeMemberAccess(exInstance, memberInfo);       // t.PropertyName
            var exConvertToObject = Expression.Convert(exMemberAccess, typeof(object));     // Convert(t.PropertyName, typeof(object))
            var lambda = Expression.Lambda<Func<T, object>>(exConvertToObject, exInstance);

            var action = lambda.Compile();
            return action;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memberInfo"></param>
        /// <returns></returns>
        public static Action<T, object> BuildUntypedSetter<T>(MemberInfo memberInfo)
        {
            var targetType = memberInfo.DeclaringType;
            var exInstance = Expression.Parameter(targetType, "t");

            var exMemberAccess = Expression.MakeMemberAccess(exInstance, memberInfo);

            // t.PropertValue(Convert(p))
            var exValue = Expression.Parameter(typeof(object), "p");
            var exConvertedValue = Expression.Convert(exValue, GetUnderlyingType(memberInfo));
            var exBody = Expression.Assign(exMemberAccess, exConvertedValue);

            var lambda = Expression.Lambda<Action<T, object>>(exBody, exInstance, exValue);
            var action = lambda.Compile();
            return action;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <returns></returns>
        public static Func<T, object> BuildEmitGetter<T>(PropertyInfo property)
        {
            if (property == null)
                throw new ArgumentNullException("property");

            MethodInfo getMethod = property.GetGetMethod(true);

            DynamicMethod dm = new DynamicMethod("PropertyGetter", typeof(object),
                new Type[] { typeof(object) },
                property.DeclaringType, true);

            ILGenerator il = dm.GetILGenerator();

            if (!getMethod.IsStatic)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.EmitCall(OpCodes.Callvirt, getMethod, null);
            }
            else
                il.EmitCall(OpCodes.Call, getMethod, null);

            if (property.PropertyType.IsValueType)
                il.Emit(OpCodes.Box, property.PropertyType);
            il.Emit(OpCodes.Ret);
            return (Func<T, object>)dm.CreateDelegate(typeof(Func<T, object>));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <returns></returns>
        public static Action<T, object> BuildEmitSetter<T>(PropertyInfo property)
        {
            if (property == null)
                throw new ArgumentNullException("property");

            MethodInfo setMethod = property.GetSetMethod(true);

            DynamicMethod dm = new DynamicMethod("PropertySetter", null,
                new Type[] { typeof(T), typeof(object) }, property.DeclaringType, true);

            ILGenerator il = dm.GetILGenerator();

            if (!setMethod.IsStatic)
            {
                il.Emit(OpCodes.Ldarg_0);
            }
            il.Emit(OpCodes.Ldarg_1);

            EmitCastToReference(il, property.PropertyType);
            if (!setMethod.IsStatic && !property.DeclaringType.IsValueType)
            {
                il.EmitCall(OpCodes.Callvirt, setMethod, null);
            }
            else
                il.EmitCall(OpCodes.Call, setMethod, null);

            il.Emit(OpCodes.Ret);
            return (Action<T, object>)dm.CreateDelegate(typeof(Action<T, object>));
        }

        private static void EmitCastToReference(ILGenerator il, Type type)
        {
            if (type.IsValueType)
                il.Emit(OpCodes.Unbox_Any, type);
            else
                il.Emit(OpCodes.Castclass, type);
        }

        private static Type GetUnderlyingType(this MemberInfo member)
        {
            switch (member.MemberType)
            {
                case MemberTypes.Event:
                    return ((EventInfo)member).EventHandlerType;
                case MemberTypes.Field:
                    return ((FieldInfo)member).FieldType;
                case MemberTypes.Method:
                    return ((MethodInfo)member).ReturnType;
                case MemberTypes.Property:
                    return ((PropertyInfo)member).PropertyType;
                default:
                    throw new ArgumentException
                    (
                     "Input MemberInfo must be if type EventInfo, FieldInfo, MethodInfo, or PropertyInfo"
                    );
            }
        }
    }
}
