using System;
using System.Collections.Generic;

namespace CommonLib.TableBasePackage
{
    public interface ITableBase: IDisposable
    {
        void BeginTransaction();
        void Commit();
        void RollBack();

        bool InsertItem<T>(string tableName, T data);
        bool InsertItemList<T>(string tableName, List<T> data);

        bool RemoveItem<T>(string tableName, string property, string id);
        bool RemoveItemList<T>(string tableName, List<FilterCondition> where);
        bool RemoveAllItem<T>(string tableName);

        bool UpdateItem<T>(string tableName, string property, string id, T data, string[] columns);

        T GetItem<T>(string tableName, string property, string id);

        List<T> GetItemList<T>(string tableName, List<FilterCondition> where);
        List<T> GetItemList<T>(string tableName, FilterCondition where);

        List<T> GetAllItem<T>(string tableName);
        int CountItemList<T>(string tableName, List<FilterCondition> where);

        #region Dictionary Mode
        List<Dictionary<string, object>> GetAllItemDict(string tableName);

        Dictionary<string, object> GetItemDict(string tableName, string property, string id);

        List<Dictionary<string, object>> GetItemListDict(string tableName, List<FilterCondition> where);
        List<Dictionary<string, object>> GetItemListDict(string tableName, FilterCondition where);
        #endregion
    }

    public abstract class ABSTableBase
    {
        public abstract List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page);

        public abstract List<Dictionary<string, object>> GetItemListDict(string tableName, List<FilterCondition> where, ref PageCondition page);
    }
}
