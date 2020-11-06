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
        EQ, NE, IN, LT, LTE, GT, GTE, REGEX, TEXT, LIKE, 
    }

    public struct FilterCondition
    {
        public FilterCondition(string key, TableCompareType? compareType = null, object pattern = null, 
            TableFilterType? filterType = null, TableOrderType? orderType = null, 
            string groupName = "_default", TableFilterType? groupConnection = null)
        {
            Key = key;
            Pattern = pattern;
            CompareType = compareType;
            FilterType = filterType;
            OrderType = orderType;
            GroupName = groupName;
            GroupConnection = groupConnection;
            GroupConnection = groupConnection;
            Regxv = null;
        }
        public string Key { get; set; }
        public object Pattern { get; set; }
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

            if (Pattern == null)
            {
                return null;
            }

            return new Regex(Pattern.ToString(), RegexOptions.Compiled);
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
        public static List<PropertyInfo> GetFieldProperties<T>(T data, Func<PropertyInfo, bool> isFieldHandle)
        {
            Type tp = data.GetType();
            List<PropertyInfo> Fields = new List<PropertyInfo>();

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
            Type tp = data.GetType();
            PropertyInfo p = tp.GetProperty(name);
            if (p == null || !isFieldHandle(p))
            {
                return null;
            }

            return p;
        }

        public static bool IsPrimaryKey(PropertyInfo p)
        {
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
            Type tp = typeof(T);
            if(tp == typeof(Dictionary<string, object>))
            {
                return GetTableDictValues(data as Dictionary<string, object>);
            }
            else if (tp.IsClass)
            {
                List<PropertyInfo> props = GetTableFieldProperties(data);
                return props.ConvertAll(d => d.GetValue(data, null));
            }
            else
            {
                return new List<object>();
            }
        }

        public static List<string> GetTableFieldNames<T>(T data)
        {
            Type tp = typeof(T);

            if(tp == typeof(Dictionary<string, object>))
            {
                List<string> props = GetTableDictNames(data as Dictionary<string, object>);
                return props;
            } 
            else if (tp.IsClass)
            {
                List<PropertyInfo> props = GetTableFieldProperties(data);
                return props.ConvertAll(d => d.Name);
            }
            else
            {
                return new List<string>();
            }
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
        public static List<PropertyInfo> GetTableFieldProperties<T>(T data)
        {
            return GetFieldProperties(data, IsTableField);
        }

        #region Dictionary
        public static List<object> GetTableDictValues(Dictionary<string, object> data)
        {
            List<object> props = new List<object>();
            foreach (var kp in data)
            {
                props.Add(kp.Value);
            }

            return props;
        }

        public static List<string> GetTableDictNames(Dictionary<string, object> data)
        {
            List<string> props = new List<string>();
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