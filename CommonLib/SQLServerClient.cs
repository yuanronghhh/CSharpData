using CommonLib.SQLTablePackage;
using CommonLib.TableBasePackage;
using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Transactions;

namespace CommonLib.DatabaseClient
{
    public interface ISQLServerBase
    {
        bool BulkInsertItemList<T>(string tableName, List<T> data, List<PropertyInfo> propList = null);
    }

    public abstract class SQLServerClientBase : SQLTableBase, ISQLServerBase
    {
        public SQLServerClientBase(string cStr = null) : base(cStr)
        {
        }

        public override List<T> GetItemList<T>(string tableName, List<FilterCondition> where, PageCondition page)
        {
            List<T> list;
            string sql = string.Empty;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);

            if (sort == string.Empty)
            {
                throw new System.Exception("SQL paging without Order");
            }

            page.Total = CountItemList<T>(tableName, filter);
            sql = string.Format(@"SELECT TOP({0}) * FROM (
                                        SELECT ROW_NUMBER() OVER(ORDER BY {1}) as RowNumber, * FROM {2}
                                        WHERE {3}
                                    ) AS t1 WHERE RowNumber > {4};",
                                    page.PageSize, sort, tableName, filter, (page.PageNo - 1) * page.PageSize);
            list = conn.Query<T>(sql, null, transaction).ToList();
            return list;
        }
        
        public bool BulkInsertItemList<T>(string tableName, List<T> data, List<PropertyInfo> propList = null)
        {
            if (data == null || data.Count == 0)
            {
                return true;
            }

            if (propList == null)
            {
                propList = TableClass.GetTableFieldProperties<T>(data.First());
            }

            SqlTransaction strans = transaction as SqlTransaction;
            SqlConnection sconn = conn as SqlConnection;

            SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sconn, SqlBulkCopyOptions.TableLock, strans);
            DataTable dTable = new DataTable();
            sqlBulkCopy.BatchSize = data.Count();
            sqlBulkCopy.DestinationTableName = tableName;

            foreach (PropertyInfo prop in propList)
            {
                sqlBulkCopy.ColumnMappings.Add(prop.Name, prop.Name);
                dTable.Columns.Add(prop.Name, prop.PropertyType);
            }

            foreach (T d in data)
            {
                object[] values = new object[propList.Count];

                for (int i = 0; i < propList.Count; i++)
                {
                    PropertyInfo p = propList[i];
                    values[i] = p.GetValue(d);
                }

                dTable.Rows.Add(values);
            }

            sqlBulkCopy.WriteToServer(dTable);
            return true;
        }
    }
}
