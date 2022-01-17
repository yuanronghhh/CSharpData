using System;
using System.Collections.Generic;
using System.Data;

namespace CommonLib.TableBasePackage
{
    public interface ITableBase: IDisposable
    {
        bool InsertItem<T>(string tableName, T data);
        bool InsertItemList<T>(string tableName, List<T> data);

        bool RemoveItem<T>(string tableName, List<FilterCondition> filter);
        bool RemoveItem<T>(string tableName, FilterCondition filter);
        bool RemoveItemList<T>(string tableName, List<FilterCondition> filter);
        bool RemoveAllItem<T>(string tableName);

        bool UpdateItem<T>(string tableName, FilterCondition filter, T data, string[] columns);
        bool UpdateItem<T>(string tableName, FilterCondition filter, string column, object value);

        T GetItem<T>(string tableName, List<FilterCondition> filter);
        T GetItem<T>(string tableName, FilterCondition filter);

        List<T> GetItemList<T>(string tableName, List<FilterCondition> filter);
        List<T> GetItemList<T>(string tableName, FilterCondition filter);

        List<T> GetAllItem<T>(string tableName);
        int CountItemList<T>(string tableName, List<FilterCondition> filter);
    }

    public abstract class ABSTableBase
    {
        public abstract List<T> GetItemList<T>(string tableName, List<FilterCondition> filter, ref PageCondition page);
    }

    public static class TableExtend
    {
        public static Dictionary<string, object> GetRecord(this IDataReader reader)
        {
            IDataRecord rc = reader;
            Dictionary<string, object> data = new Dictionary<string, object>();

            for (int i = 0; i < rc.FieldCount; i++)
            {
                data.Add(rc.GetName(i), rc.GetValue(i));
            }

            return data;
        }
    }
}
