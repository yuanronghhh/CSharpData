using System;
using System.Collections.Generic;
using System.Linq;
using CommonLib.SQLTablePackage;
using CommonLib.TableBasePackage;
using Dapper;

namespace CommonLib.DatabaseClient
{
    public interface ISQLiteBase
    {
    }

    public abstract class SQLiteClientBase : SQLTableBase, ISQLiteBase
    {
        public SQLiteClientBase(string cStr = null) : base(cStr)
        {
            tableUtils.SetEscapeChar("\"");
        }

        public override List<Dictionary<string, object>> GetItemListDict(string tableName, List<FilterCondition> where, ref PageCondition page)
        {
            List<Dictionary<string, object>> list;
            string sql = string.Empty;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);

            if (sort == string.Empty)
            {
                throw new Exception("SQL paging without Order");
            }

            page.Total = CountItemList<Dictionary<string, object>>(tableName, filter);
            sql = string.Format("SELECT * FROM {0} WHERE {1} ORDER BY {2} LIMIT {3} OFFSET {3}*{4};",
                tableName, filter, sort, page.PageSize, page.PageNo - 1);

            Console.WriteLine("SQL: {0}", sql);
            list = conn.QueryDictionary(sql, null, transaction).ToList();
            return list;
        }

        public override List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page)
        {
            List<T> list;
            string sql = string.Empty;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);

            if (sort == string.Empty)
            {
                throw new Exception("SQL paging without Order");
            }

            page.Total = CountItemList<T>(tableName, filter);
            sql = string.Format("SELECT * FROM {0} WHERE {1} ORDER BY {2} LIMIT {3} OFFSET {3}*{4};", 
                tableName, filter, sort, page.PageSize, page.PageNo - 1);

            Console.WriteLine("SQL: {0}", sql);
            list = conn.Query<T>(sql, null, transaction).ToList();
            return list;
        }
    }
}