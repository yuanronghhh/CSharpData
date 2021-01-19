using CommonLib.SQLTablePackage;
using CommonLib.TableBasePackage;
using Dapper;
using System;
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
        List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page, List<string> columns = null);
        List<T> GetItemList2<T>(string tableName, List<FilterCondition> where, ref PageCondition page, List<string> columns = null);

        List<Dictionary<string, object>> GetItemListDict2(string tableName, List<FilterCondition> where, ref PageCondition page, List<string> columns = null);

        bool BulkInsertItemList<T>(string tableName, List<T> data, List<PropertyInfo> props = null);
        bool BulkInsertItemListDict(string tableName, List<Dictionary<string, object>> data, List<string> columns = null);
    }

    public abstract class SQLServerClientBase : SQLTableBase, ISQLServerBase
    {
        public SQLServerClientBase(string cStr = null) : base(cStr)
        {
        }

        public List<Dictionary<string, object>> GetAllReferenceKeyTable()
        {
            string sql = string.Format(@"SELECT o.name AS ReferenceKey, o1.name AS TableName FROM sysobjects AS o
	            LEFT JOIN (
		            SELECT * FROM sysobjects WHERE type = 'U'
	            ) AS o1 ON o.parent_obj = o1.id
	            WHERE o.type IN ('F', 'K') ORDER BY o.type;");

            return QuerySQLDict(sql, null);
        }

        public List<Dictionary<string, object>> GetAllUserTypes()
        {
            string sql = string.Format(@"SELECT t1.name AS UserDefineType, CONCAT(t2.name, 
	                                        CASE WHEN LOWER(t2.name) IN ('nvarchar', 'char', 'varchar') THEN '(' + CAST(t1.max_length AS VARCHAR(10)) + ')' ELSE '' END,
	                                        CASE WHEN LOWER(t2.name) in ('numeric', 'decimal') THEN '(' + CAST(t1.precision AS VARCHAR(10)) + ',' + CAST(t1.scale AS VARCHAR(10)) + ')' ELSE '' END
                                        ) AS SystemType FROM sys.types AS t1
                                        LEFT JOIN sys.types AS t2 ON t1.system_type_id = t2.user_type_id
                                        WHERE t1.SCHEMA_ID = 1");

            return QuerySQLDict(sql, null);
        }

        public bool DropTable(string tableName)
        {
            string sql = string.Format("IF OBJECT_ID('{0}', N'U') IS NOT NULL BEGIN DROP TABLE {0} END;", tableName);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public bool TruncateTable(string tableName)
        {
            string sql = string.Format("IF OBJECT_ID('{0}', N'U') IS NOT NULL BEGIN Truncate TABLE {0} END;", tableName);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public string GetCreateScript(string tableName, bool checkExists = false)
        {
            string sql = string.Format(@"DECLARE @table_name SYSNAME
                            SELECT @table_name = 'dbo.{0}'

                            DECLARE 
                                    @object_name SYSNAME
                                , @object_id INT

                            SELECT 
                                    @object_name = '[' + s.name + '].[' + o.name + ']'
                                , @object_id = o.[object_id]
                            FROM sys.objects o WITH (NOWAIT)
                            JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
                            WHERE s.name + '.' + o.name = @table_name
                                AND o.[type] = 'U'
                                AND o.is_ms_shipped = 0

                            DECLARE @SQL NVARCHAR(MAX) = ''

                            ;WITH index_column AS 
                            (
                                SELECT 
                                        ic.[object_id]
                                    , ic.index_id
                                    , ic.is_descending_key
                                    , ic.is_included_column
                                    , c.name
                                FROM sys.index_columns ic WITH (NOWAIT)
                                JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
                                WHERE ic.[object_id] = @object_id
                            ),
                            fk_columns AS 
                            (
                                    SELECT 
                                        k.constraint_object_id
                                    , cname = c.name
                                    , rcname = rc.name
                                FROM sys.foreign_key_columns k WITH (NOWAIT)
                                JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id 
                                JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
                                WHERE k.parent_object_id = @object_id
                            )
                            SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((
                                SELECT CHAR(9) + ', [' + c.name + '] ' + 
                                    CASE WHEN c.is_computed = 1
                                        THEN 'AS ' + cc.[definition] 
                                        ELSE UPPER(tp.name) + 
                                            CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary')
                                                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                                                    WHEN tp.name IN ('nvarchar', 'nchar')
                                                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                                                    WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset') 
                                                    THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                                                    WHEN tp.name = 'decimal' 
                                                    THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
                                                ELSE ''
                                            END +
                                            CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
                                            CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
                                            CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END + 
                                            CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END 
                                    END + CHAR(13)
                                FROM sys.columns c WITH (NOWAIT)
                                JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
                                LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
                                LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
                                LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                                WHERE c.[object_id] = @object_id
                                ORDER BY c.column_id
                                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
                                + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' + 
                                                (SELECT STUFF((
                                                        SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
                                                        FROM sys.index_columns ic WITH (NOWAIT)
                                                        JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                                                        WHERE ic.is_included_column = 0
                                                            AND ic.[object_id] = k.parent_object_id 
                                                            AND ic.index_id = k.unique_index_id     
                                                        FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
                                        + ')' + CHAR(13)
                                        FROM sys.key_constraints k WITH (NOWAIT)
                                        WHERE k.parent_object_id = @object_id 
                                            AND k.[type] = 'PK'), '') + ')'  + CHAR(13)
                                + ISNULL((SELECT (
                                    SELECT CHAR(13) +
                                            'ALTER TABLE ' + @object_name + ' WITH' 
                                        + CASE WHEN fk.is_not_trusted = 1 
                                            THEN ' NOCHECK' 
                                            ELSE ' CHECK' 
                                            END + 
                                            ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY(' 
                                            + STUFF((
                                            SELECT ', [' + k.cname + ']'
                                            FROM fk_columns k
                                            WHERE k.constraint_object_id = fk.[object_id]
                                            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                                            + ')' +
                                            ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
                                            + STUFF((
                                            SELECT ', [' + k.rcname + ']'
                                            FROM fk_columns k
                                            WHERE k.constraint_object_id = fk.[object_id]
                                            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                                            + ')'
                                        + CASE 
                                            WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE' 
                                            WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
                                            WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT' 
                                            ELSE '' 
                                            END
                                        + CASE 
                                            WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
                                            WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
                                            WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'  
                                            ELSE '' 
                                            END 
                                        + CHAR(13) + 'ALTER TABLE ' + @object_name + ' CHECK CONSTRAINT [' + fk.name  + ']' + CHAR(13)
                                    FROM sys.foreign_keys fk WITH (NOWAIT)
                                    JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
                                    WHERE fk.parent_object_id = @object_id
                                    FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')
                                + ISNULL(((SELECT
                                        CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END 
                                            + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + @object_name + ' (' +
                                            STUFF((
                                            SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                                            FROM index_column c
                                            WHERE c.is_included_column = 0
                                                AND c.index_id = i.index_id
                                            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'  
                                            + ISNULL(CHAR(13) + 'INCLUDE (' + 
                                                STUFF((
                                                SELECT ', [' + c.name + ']'
                                                FROM index_column c
                                                WHERE c.is_included_column = 1
                                                    AND c.index_id = i.index_id
                                                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '')  + CHAR(13)
                                    FROM sys.indexes i WITH (NOWAIT)
                                    WHERE i.[object_id] = @object_id
                                        AND i.is_primary_key = 0
                                        AND i.[type] = 2
                                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
                                ), '');
                            SELECT @SQL AS script;", tableName);
            string script;

            Dictionary<string, object> obj = conn.QueryEntityFirst(sql, null, transaction);
            if (obj == null || !obj.ContainsKey("script") || obj["script"] == null) { return null; }

            script = obj["script"].ToString();
            if (script == null) { return null; }

            if (checkExists)
            {
                script = string.Format(@"IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = '{0}') 
                                        BEGIN 
                                            {1} 
                                        END", tableName, script);
            }

            return script;
        }

        public override List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page)
        {
            Type tp = typeof(T);
            List<string> columns = TableClass.GetTableFieldNames<T>();

            return GetItemList<T>(tableName, where, ref page, columns);
        }

        public List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page, List<string> columns = null)
        {
            List<T> list;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);
            object param = tableUtils.FilterConditionToParam(where);
            string cols = columns == null ? "*" : tableUtils.JoinStringList(columns);

            if (string.IsNullOrWhiteSpace(sort))
            {
                throw new Exception("SQL paging without Order");
            }

            page.Total = CountItemList<T>(tableName, where);
            string sql = string.Format(@"SELECT TOP({0}) {4} FROM (
                                        SELECT ROW_NUMBER() OVER(ORDER BY {1}) as RowNumber, * FROM {2}
                                        WHERE {3}
                                    ) AS t1 WHERE RowNumber > {4};",
                        page.PageSize, sort, tableName, filter, (page.PageNo - 1) * page.PageSize, cols);
            list = conn.Query<T>(sql, param, transaction).ToList();
            return list;
        }

        public List<T> GetItemList2<T>(string tableName, List<FilterCondition> where, ref PageCondition page, List<string> columns = null)
        {
            List<T> list;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);
            object param = tableUtils.FilterConditionToParam(where);
            string cols = columns == null ? "*" : tableUtils.JoinStringList(columns);

            if (string.IsNullOrWhiteSpace(sort))
            {
                throw new Exception("SQL paging without Order");
            }

            page.Total = CountItemList<T>(tableName, where);
            string sql = string.Format(@"SELECT * FROM (
                                        SELECT {5} FROM {2} 
                                        WHERE {3}
								    ) AS t1
								    ORDER BY {1} OFFSET {4} ROWS FETCH NEXT {0} ROWS ONLY;",
                        page.PageSize, sort, tableName, filter, (page.PageNo - 1) * page.PageSize, cols);
            list = conn.QueryEntity<T>(sql, param, transaction).ToList();
            return list;
        }

        public List<Dictionary<string, object>> GetItemListDict2(string tableName, List<FilterCondition> where, ref PageCondition page, List<string> columns = null)
        {
            List<Dictionary<string, object>> list;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);
            object param = tableUtils.FilterConditionToParam(where);
            string cols = columns == null ? "*" : tableUtils.JoinStringList(columns);

            if (string.IsNullOrWhiteSpace(sort))
            {
                throw new Exception("SQL paging without Order");
            }

            page.Total = CountItemList(tableName, where);
            string sql = string.Format(@"SELECT * FROM (
                                        SELECT {5} FROM {2} 
                                        WHERE {3}
								    ) AS t1
								    ORDER BY {1} OFFSET {4} ROWS FETCH NEXT {0} ROWS ONLY;",
                        page.PageSize, sort, tableName, filter, (page.PageNo - 1) * page.PageSize, cols);
            list = conn.QueryEntity(sql, param, transaction).ToList();
            return list;
        }

        public bool BulkInsertItemListDict(string tableName, List<Dictionary<string, object>> data, List<string> columns = null)
        {
            if (data == null || data.Count == 0)
            {
                return true;
            }
            List<KeyValuePair<string, object>> propList = data.First().ToList();
            if (columns != null)
            {
                propList = propList.Where(k =>
                {
                    return columns.Exists(c => c == k.Key);
                }).ToList();
            }

            SqlTransaction strans = transaction as SqlTransaction;
            SqlConnection sconn = conn as SqlConnection;

            SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sconn, SqlBulkCopyOptions.TableLock, strans);
            DataTable dTable = new DataTable();
            sqlBulkCopy.BatchSize = data.Count();
            sqlBulkCopy.DestinationTableName = tableName;

            foreach (var prop in propList)
            {
                sqlBulkCopy.ColumnMappings.Add(prop.Key, prop.Key);
                Type tp = prop.Value != null ? prop.Value.GetType() : typeof(object);

                dTable.Columns.Add(prop.Key, tp);
            }

            foreach (var d in data)
            {
                object[] values = new object[propList.Count];

                for (int i = 0; i < propList.Count; i++)
                {
                    KeyValuePair<string, object> p = propList[i];
                    if(d.TryGetValue(p.Key, out values[i]))
                    {
                    }
                }

                dTable.Rows.Add(values);
            }

            sqlBulkCopy.WriteToServer(dTable);
            return true;
        }

        public bool BulkInsertItemList<T>(string tableName, List<T> data, List<PropertyInfo> propList = null)
        {
            if (data == null || data.Count == 0)
            {
                return true;
            }

            if (propList == null)
            {
                propList = TableClass.GetTableFieldProperties<T>();
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
