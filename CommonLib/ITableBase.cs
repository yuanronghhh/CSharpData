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
    }

    public abstract class ABSTableBase
    {
        public abstract List<T> GetItemList<T>(string tableName, List<FilterCondition> where, PageCondition page);
    }
}
