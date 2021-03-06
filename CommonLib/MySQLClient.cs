﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CommonLib.SQLTablePackage;
using CommonLib.TableBasePackage;
using Dapper;

namespace CommonLib.DatabaseClient
{
    public interface IMySQLBase
    {
        bool BulkDataToFile<T>(string fName, List<T> data, string valueEncloseChar = "`");
        bool BulkLoadFromFile(string tableName, string fName, string valueEncloseChar = "`");

    }

    public abstract class MySQLClientBase : SQLTableBase, IMySQLBase
    {
        public MySQLClientBase(string cStr = null) : base(cStr)
        {
            tableUtils.SetEscapeChar("\"");
        }
        
        public override List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page)
        {
            List<T> list;
            string sql = string.Empty;
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);
            object param = tableUtils.FilterConditionToParam(where);

            if (string.IsNullOrWhiteSpace(sort))
            {
                throw new Exception("SQL paging without Order");
            }

            page.Total = CountItemList<T>(tableName, where);
            sql = string.Format("SELECT * FROM {0} WHERE {1} ORDER BY {2} LIMIT {3}, {4};", 
                tableName, filter, sort, (page.PageNo - 1) * page.PageSize, page.PageSize);


            list = conn.Query<T>(sql, param, transaction).ToList();
            return list;
        }

        public bool BulkDataToFile<T>(string fName, List<T> data, string valueEncloseChar = "`")
        {
            if(data == null) { return false; }

            tableUtils.SetEscapeChar(valueEncloseChar);
            using (FileStream fs = File.OpenWrite(fName))
            {
                using (StreamWriter sw = new StreamWriter(fs))
                {
                    foreach (T d in data)
                    {
                        List<object> vl = TableClass.GetTableValues<T>(d);
                        string val = tableUtils.JoinObjectList(vl);

                        sw.WriteLine(val);
                    }
                }
            }
            tableUtils.SetEscapeChar("\"");

            return true;
        }

        public bool BulkLoadFromFile(string tableName, string fName, string valueEncloseChar = "`")
        {
            if (string.IsNullOrWhiteSpace(fName)) { return false; }

            fName = fName.Replace("\\", "/");
            string nline = Environment.NewLine == "\n" ? @"\n" : @"\r\n";

            string sql = string.Format(@"LOAD DATA LOCAL INFILE '{0}'
INTO TABLE {1}
FIELDS TERMINATED BY ',' ENCLOSED BY '{2}' ESCAPED BY '\\'
LINES TERMINATED BY '{3}';", fName, tableName, valueEncloseChar, nline);

            return Execute(sql) > 0;
        }
    }
}