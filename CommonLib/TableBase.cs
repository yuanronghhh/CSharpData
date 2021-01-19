using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Commonlib.Reflection;
using CommonLib.TableData;
using DnsClient;

/// <summary>
/// BaseTable 将实体数据如同表一样获取
/// 通过修饰器TableFields，TableDecorator
/// </summary>
namespace CommonLib.TableData
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class TableName : Attribute
    {
        public string Value { get; set; }

        public TableName(string name)
        {
            Value = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class DatabaseFields : Attribute
    {
        public bool IsTableField { get; set; }

        public DatabaseFields(bool isTableField = true)
        {
            IsTableField = isTableField;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class PrimaryKey : Attribute
    {
        public bool IsPrimaryKey { get; set; }

        public PrimaryKey(bool isPrimaryKey = true)
        {
            IsPrimaryKey = isPrimaryKey;
        }
    }
}

namespace CommonLib.TableBasePackage
{
    public enum TableOrderType
    {
        ASCENDING, DESCENDING
    }

    public enum TableFilterType
    {
        AND, OR
    }

    public enum TableCompareType
    {
        EQ, NE, IN, NIN, LT, LTE, GT, GTE, REGEX, TEXT, LIKE, STREE
    }

    public struct FilterCondition
    {
        public FilterCondition(string key, TableCompareType? compareType = null, object value = null, 
            TableFilterType? filterType = null, TableOrderType? orderType = null, 
            string groupName = "_default", TableFilterType? groupConnection = null)
        {
            Key = key;
            Value = value;
            CompareType = compareType;
            FilterType = filterType;
            OrderType = orderType;
            GroupName = groupName;
            GroupConnection = groupConnection;
            GroupConnection = groupConnection;
            Regxv = null;
        }
        public string Key { get; set; }
        public object Value { get; set; }
        public TableCompareType? CompareType { get; set; }
        public TableFilterType? FilterType { get; set; }
        public TableOrderType? OrderType { get; set; }

        public string GroupName { get; set; }
        public TableFilterType? GroupConnection { get; set; }

        private Regex Regxv { get; set; }
        public Regex GetRegexValue()
        {
            if (Regxv != null)
            {
                return Regxv;
            }

            if (Value == null)
            {
                return null;
            }

            return new Regex(Value.ToString(), RegexOptions.Compiled);
        }
    }

    public struct PageCondition
    {
        public PageCondition(int pageNo, int pageSize, int total = 0)
        {
            PageNo = pageNo;
            PageSize = pageSize;
            Total = total;
        }

        public int PageNo { get; set; } /* CurrentPageNo */
        public int PageSize { get; set; }
        public int Total { get; set; }
    }

    public class TableClass
    {
        public static string GetTableName<T>(T data)
        {
            Type tp = data.GetType();
            TableName decorator;

            var attribute = tp.GetCustomAttributes(typeof(TableName), false).FirstOrDefault();
            if (attribute == null)
            {
                return "";
            }
            decorator = attribute as TableName;

            return decorator.Value;
        }
        public static List<PropertyInfo> GetFieldProperties<T>(Func<PropertyInfo, bool> isFieldHandle)
        {
            Type tp = typeof(T);
            List<PropertyInfo> Fields = new List<PropertyInfo>();
            if(isFieldHandle == null) { return Fields; }

            foreach (PropertyInfo p in tp.GetProperties())
            {
                if (isFieldHandle(p))
                {
                    Fields.Add(p);
                }
            }

            return Fields;
        }
        public static PropertyInfo GetFieldProperty<T>(T data, string name, Func<PropertyInfo, bool> isFieldHandle)
        {
            if(string.IsNullOrWhiteSpace(name)) { return null; }

            Type tp = data.GetType();
            PropertyInfo p = tp.GetProperty(name);
            if (isFieldHandle == null) { return p; }


            if (p == null || !isFieldHandle(p))
            {
                return null;
            }

            return p;
        }

        public static bool IsPrimaryKey(PropertyInfo p)
        {
            if (p == null) { return false; }

            PrimaryKey decorator;
            object attribute = p.GetCustomAttributes(typeof(PrimaryKey), false).FirstOrDefault();
            if (attribute == null)
            {
                return false;
            }

            decorator = attribute as PrimaryKey;
            return decorator.IsPrimaryKey;
        }

        public static bool IsTableField(PropertyInfo p)
        {
            if (p == null) { return false; }

            DatabaseFields decorator;
            object attribute = p.GetCustomAttributes(typeof(DatabaseFields), false).FirstOrDefault();
            if (attribute == null)
            {
                return false;
            }

            decorator = attribute as DatabaseFields;
            return decorator.IsTableField;
        }

        public static List<object> GetTableValues<T>(T data)
        {
            List<PropertyInfo> props = GetTableFieldProperties<T>();
            return props.ConvertAll(d => d.GetValue(data, null));
        }

        public static List<string> GetTableFieldNames<T>()
        {
            List<PropertyInfo> props = GetTableFieldProperties<T>();
            return props.ConvertAll(d => d.Name);
        }
        public static bool HasTableProperty<T>(string propName)
        {
            PropertyInfo p = typeof(T).GetProperty(propName);
            if (p == null || !IsTableField(p))
            {
                return false;
            }

            return true;
        }
        public static List<PropertyInfo> GetTableFieldProperties<T>()
        {
            return GetFieldProperties<T>(IsTableField);
        }

        #region Dictionary
        public static List<object> GetTableDictValuesDict(Dictionary<string, object> data)
        {
            List<object> props = new List<object>();
            if(data == null) { return props; }

            foreach (var kp in data)
            {
                props.Add(kp.Value);
            }

            return props;
        }

        public static List<string> GetTableNamesDict(Dictionary<string, object> data)
        {
            List<string> props = new List<string>();
            if(data == null) { return props; }

            foreach (var kp in data)
            {
                props.Add(kp.Key);
            }

            return props;
        }
        #endregion
    }
}

namespace CommonLib.TableSecurity
{
}