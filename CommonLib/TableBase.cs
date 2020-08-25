using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using CommonLib.TableData;

/// <summary>
/// BaseTable 将实体数据如同表一样获取
/// 通过修饰器TableFields，TableDecorator
/// </summary>
namespace CommonLib.TableData
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class TableDecorator : Attribute
    {
        public string TableName { get; set; }

        public TableDecorator(string name)
        {
            TableName = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class TableFields : Attribute
    {
        public bool IsTableField { get; set; }

        public TableFields(bool isTableField = true)
        {
            IsTableField = isTableField;
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
            TableDecorator decorator;

            var attribute = tp.GetCustomAttributes(typeof(TableDecorator), false).FirstOrDefault();
            if (attribute == null)
            {
                return "";
            }
            decorator = attribute as TableDecorator;

            return decorator.TableName;
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

        public static bool IsTableField(PropertyInfo p)
        {
            TableFields decorator;
            object attribute = p.GetCustomAttributes(typeof(TableFields), false).FirstOrDefault();
            if (attribute == null)
            {
                return false;
            }

            decorator = attribute as TableFields;
            return decorator.IsTableField;
        }
        public static List<object> GetTableValues<T>(T data)
        {
            List<PropertyInfo> props = GetTableFieldProperties(data);
            return props.ConvertAll(d => d.GetValue(data, null));
        }
        public static List<string> GetTableFieldName<T>(T data)
        {
            List<PropertyInfo> props = GetTableFieldProperties(data);
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
        public static List<PropertyInfo> GetTableFieldProperties<T>(T data)
        {
            return GetFieldProperties(data, IsTableField);
        }
    }
}

namespace CommonLib.TableSecurity
{
}