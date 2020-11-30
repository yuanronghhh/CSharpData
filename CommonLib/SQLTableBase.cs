using Commonlib.Reflection;
using CommonLib.TableBasePackage;
using Dapper;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Transactions;

namespace CommonLib.SQLTablePackage
{
    public class SQLTableUtils
    {
        public string esChar = "\'";

        public void SetEscapeChar(string eChar)
        {
            if (string.IsNullOrWhiteSpace(eChar))
            {
                return;
            }

            esChar = eChar;
        }

        public string EscapeIlleagal(string name)
        {
            if(name.IndexOf(" ") > -1) { return ""; }
            name = name.Replace(esChar, "");

            return name;
        }

        public string EscapeValue(string name)
        {
            name = name.Replace(esChar, "");

            if (string.IsNullOrWhiteSpace(name))
            {
                return string.Empty;
            }

            return string.Format("{1}{0}{1}", name, esChar);
        }

        public string CheckExistKey<T>(T data, string name)
        {
            string key = string.Empty;
            List<string> kName = TableClass.GetTableFieldNames<T>(data);

            if (!kName.Exists(d => d == name))
            {
                return key;
            }

            return key;
        }

        public Dictionary<string, List<FilterCondition>> FilterConditionToGroup(List<FilterCondition> conds)
        {
            Dictionary<string, List<FilterCondition>> cond = new Dictionary<string, List<FilterCondition>>();
            for (int i = 0; i < conds.Count; i++)
            {
                FilterCondition s = conds[i];

                if (!cond.ContainsKey(s.GroupName))
                {
                    cond[s.GroupName] = new List<FilterCondition>();
                }

                cond[s.GroupName].Add(s);
            }

            return cond;
        }

        public string FilterConditionToWhere(List<FilterCondition> conds)
        {
            string fd = string.Empty;

            Dictionary<string, List<FilterCondition>> cond = FilterConditionToGroup(conds);

            foreach (var kp in cond)
            {
                fd = GetGroupFromFilter(fd, EscapeIlleagal(kp.Key), kp.Value);
            }

            if(fd == string.Empty)
            {
                fd = "1 = 1";
            }

            return fd;
        }

        public string GetGroupFromFilter(string fd, string key, List<FilterCondition> fcs)
        {
            string nfd = string.Empty;
            TableFilterType? gft = null;

            if (fcs == null || fcs.Count == 0)
            {
                return fd;
            }

            if (key != "_default" && fd != string.Empty)
            {
                FilterCondition? c = fcs.Find(d => d.GroupConnection != null);
                if(c == null)
                {
                    throw new Exception(string.Format("{0} GroupConnection Missing Or/And connection", key));
                }
                gft = c.Value.GroupConnection;
            }
            
            foreach (FilterCondition s in fcs)
            {
                nfd = GetFromFilterType(nfd, s);
            }

            return GroupFilterByType(gft, fd, nfd, true);
        }

        public string GroupFilter(string fd, bool withGroup = false)
        {
            if (!withGroup || fd == string.Empty) { return fd; }
            return string.Format("({0})", fd);
        }

        public string ConnectFilter(string con, string fd, string nfd, bool withGroup = false)
        {
            if (fd == string.Empty) { return GroupFilter(nfd, withGroup); }
            if(nfd == string.Empty) { return fd; }

            return string.Format("{0} {1} {2}", fd, EscapeIlleagal(con), GroupFilter(nfd, withGroup));
        }

        public string GroupFilterByType(TableFilterType? ft, string fd, string nfd, bool withGroup = false)
        {
            switch (ft)
            {
                case TableFilterType.OR:
                    return ConnectFilter("OR", fd, nfd, withGroup);
                case TableFilterType.AND:
                    return ConnectFilter("AND", fd, nfd, withGroup);
                default:
                    return ConnectFilter("AND", fd, nfd, withGroup);
            }
        }

        public string GetFromFilterType(string fd, FilterCondition s, bool withGroup = false)
        {
            string nfd = GetFromCompareType(s);
            if (nfd == string.Empty) { return fd; }
            if (fd == string.Empty) {
                return GroupFilter(nfd, withGroup);
            }

            return GroupFilterByType(s.FilterType, fd, nfd, withGroup);
        }

        public string FilterConditionToSort(List<FilterCondition> conds)
        {
            StringBuilder sort = new StringBuilder();
            string sd = string.Empty;

            foreach (FilterCondition s in conds)
            {
                if (!s.OrderType.HasValue)
                {
                    continue;
                }

                if (s.OrderType == TableOrderType.DESCENDING)
                {
                    sort.Append(string.Format("{0} DESC,", s.Key));
                }
                else
                {
                    sort.Append(string.Format("{0} ASC,", s.Key));
                }
            }
            sd = sort.ToString().TrimEnd(',');

            return sd;
        }

        public string GetFromCompareType(FilterCondition s)
        {
            if (s.Pattern == null) { return string.Empty; }
            string pattern = string.Empty;

            switch (s.CompareType)
            {
                case TableCompareType.EQ:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} = {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.GT:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} > {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.GTE:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} >= {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.LT:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} < {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.LTE:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} <= {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.NE:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} <> {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.LIKE:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} LIKE {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.IN:
                    {
                        List<object> pList = null;
                        if (s.Pattern.GetType().IsArray)
                        {
                            object[] pArr = s.Pattern as object[];
                            pList = pArr.ToList();
                        }
                        else
                        {
                            pList = s.Pattern as List<object>;
                        }

                        if (pList == null)
                        {
                            throw new Exception("Pattern should be array or list");
                        }
                        pattern = JoinObjectList(pList);

                        return string.Format("{0} IN ({1})", EscapeIlleagal(s.Key), pattern);
                    }
                default:
                    {
                        pattern = EscapeValue(s.Pattern.ToString());
                        return string.Format("{0} = {1}", EscapeIlleagal(s.Key), pattern);
                    }
            }
        }

        public string JoinStringList(List<string> strArray, bool withEschar = false)
        {
            string str = string.Empty;
            if (withEschar)
            {
                str = esChar + string.Join(esChar + "," + esChar, strArray) + esChar;
            }
            else
            {
                str = string.Join(",", strArray);
            }

            return str;
        }

        public string JoinObjectList(List<object> strArray)
        {
            return JoinStringList(strArray.ConvertAll(d => d == null ? "" : d.ToString()), true);
        }

        public string JoinValueList<T>(List<T> data)
        {
            StringBuilder sb = new StringBuilder();

            foreach (T d in data)
            {
                List<object> vl = TableClass.GetTableValues<T>(d);
                string val = "(" + JoinObjectList(vl) + "),";

                sb.Append(val);
            }
            string vals = sb.ToString().TrimEnd(',');

            return vals;
        }

        public List<string> GetPropertyPair<T>(T data, string[] columns)
        {
            Type tp = data.GetType();
            List<string> pairList = new List<string>();

            foreach (string name in columns)
            {
                PropertyInfo p = ReflectionCommon.GetProperty(data, name);
                if (p == null)
                {
                    continue;
                }

                object val = p.GetValue(data, null) ?? "";

                pairList.Add(string.Format("{0} = {1}", p.Name, EscapeValue(val.ToString())));
            }

            return pairList;
        }
    }

    public interface ISQLTableBase: ITableBase
    {
        void SetEscapeChar(string esChar);
        void ChangeDataBase(string name);
        T GetItem<T>(string tableName, string property1, string id1, string property2, string id2);
        int CountItemList<T>(string tableName, string where);
        int ExecuteSql(string sql);
        List<T> QuerySql<T>(string sql);
    }

    public abstract class ABSSQLTableBase: ABSTableBase
    {
        public abstract IDbConnection GetConnection(string cStr);
    }

    public abstract class SQLTableBase : ABSSQLTableBase, ISQLTableBase
    {
        public int stackCount = 0;
        public IDbTransaction transaction = null;
        public IDbConnection conn = null;
        public string connString = string.Empty;
        public SQLTableUtils tableUtils = new SQLTableUtils();

        public void SetEscapeChar(string esChar)
        {
            tableUtils.SetEscapeChar(esChar);
        }

        public SQLTableBase(string cStr)
        {
            conn = GetConnection(cStr);
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }
            stackCount = 0;
        }

        public virtual void BeginTransaction()
        {
            stackCount += 1;
            if (transaction != null && transaction.Connection != null)
            {
                return;
            }

            transaction = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        }

        public virtual void Dispose()
        {
            if(stackCount > 1)
            {
                stackCount -= 1;
                return;
            }

            transaction.Dispose();
            conn.Close();
        }

        public virtual void Commit()
        {
            if (transaction != null)
            {
                if (transaction.Connection != null)
                {
                    transaction.Commit();
                }
            }

            transaction = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        }

        public virtual void RollBack()
        {
            if(transaction != null)
            {
                if (transaction.Connection != null)
                {
                    transaction.Rollback();
                }
            }

            transaction = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        }

        public void ChangeDataBase(string name)
        {
            conn.ChangeDatabase(name);
        }

        public bool InsertItem<T>(string tableName, T data, string cols)
        {
            List<object> valList = TableClass.GetTableValues<T>(data);
            string vals = tableUtils.JoinObjectList(valList);

            string sql = string.Format("INSERT INTO {0} ( {1} ) VALUES ( {2} );", tableName, cols, vals);

            return conn.Execute(sql, null, transaction) > 0;
        }

        public bool InsertItem<T>(string tableName, T data)
        {
            string cols = tableUtils.JoinStringList(TableClass.GetTableFieldNames<T>(data));
            return InsertItem<T>(tableName, data, cols);
        }

        public void DebugData(string data)
        {
            double size = Encoding.Default.GetBytes(data).Length / 1024.0 / 1024.0;

            Console.WriteLine("SQL size: {0}MB", size);
        }

        /// <summary>
        /// MySQL limit parameter , max_allowed_packet=4194304
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public bool InsertItemList<T>(string tableName, List<T> data)
        {
            if (data == null || data.Count == 0)
            {
                return true;
            }
            string cols = tableUtils.JoinStringList(TableClass.GetTableFieldNames<T>(data.First()));

            return InsertItemList(tableName, data, cols);
        }

        public bool InsertItemList<T>(string tableName, List<T> data, string cols)
        {
            string vals = tableUtils.JoinValueList<T>(data);

            string sql = string.Format("INSERT INTO {0} ( {1} ) VALUES {2};", tableName, cols, vals);

            // DebugData(sql);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public bool UpdateItem<T>(string tableName, string property, string id, T data, string[] columns)
        {
            string cols = tableUtils.JoinStringList(tableUtils.GetPropertyPair<T>(data, columns));
            id = tableUtils.EscapeValue(id);

            string sql = string.Format("UPDATE {0} SET {1} WHERE {2} = {3};", tableName, cols, tableUtils.EscapeIlleagal(property), id);

            return conn.Execute(sql, null, transaction) > 0;
        }

        public T GetItem<T>(string tableName, string property, string id)
        {
            id = tableUtils.EscapeValue(id);

            string sql = string.Format("SELECT * FROM {0} WHERE {1} = {2};", tableName, tableUtils.EscapeIlleagal(property), id);
            var list = conn.Query<T>(sql, null, transaction);

            return list.FirstOrDefault();
        }

        public virtual List<T> GetItemList<T>(string tableName, List<FilterCondition> where)
        {
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);

            if(sort != string.Empty)
            {
                sort = string.Format("ORDER BY {0}", sort);
            }

            string sql = string.Format("SELECT * FROM {0} WHERE {1} {2};", tableName, filter, sort);

            return conn.Query<T>(sql, null, transaction).ToList();
        }

        public virtual List<T> GetItemList<T>(string tableName, FilterCondition where)
        {
            List<FilterCondition> filter = new List<FilterCondition>() { where };
            return GetItemList<T>(tableName, filter);
        }

        public T GetItem<T>(string tableName, string property1, string id1, string property2, string id2)
        {
            id1 = tableUtils.EscapeValue(id1);
            id2 = tableUtils.EscapeValue(id2);

            string sql = string.Format("SELECT * FROM {0} WHERE {1} = {2} AND {3} = {4};", tableName, tableUtils.EscapeIlleagal(property1), id1, tableUtils.EscapeIlleagal(property2), id2);
            var list = conn.Query<T>(sql, null, transaction);

            return list.FirstOrDefault();
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            string sql = string.Format("SELECT * FROM {0} WHERE 1 = 1;", tableName);
            return conn.Query<T>(sql, null, transaction).ToList();
        }

        public bool RemoveAllItem<T>(string tableName)
        {
            string sql = string.Format("DELETE FROM {0} WHERE 1 = 1;", tableName);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public bool RemoveItem<T>(string tableName, string property, string id)
        {
            id = tableUtils.EscapeValue(id);

            string sql = string.Format("DELETE FROM {0} WHERE {1} = {2};", tableName, tableUtils.EscapeIlleagal(property), id);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public bool RemoveItemList<T>(string tableName, List<FilterCondition> where)
        {
            string filter = tableUtils.FilterConditionToWhere(where);
            if (string.IsNullOrWhiteSpace(filter)) { return false; }

            string sql = string.Format("DELETE FROM {0} WHERE {1};", tableName, filter);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public int CountItemList<T>(string tableName, List<FilterCondition> where)
        {
            string filter = tableUtils.FilterConditionToWhere(where);

            string sql = string.Format("SELECT COUNT(*) as cout FROM {0} WHERE {1};", tableName, filter);
            var sc = conn.ExecuteScalar(sql, null, transaction);

            return Convert.ToInt32(sc);
        }

        public int CountItemList<T>(string tableName, string where)
        {
            string sql = string.Format("SELECT COUNT(*) as cout FROM {0} WHERE {1};", tableName, where);
            object sc = conn.ExecuteScalar(sql, null, transaction);

            return Convert.ToInt32(sc);
        }

        public int ExecuteSql(string sql)
        {
            return conn.Execute(sql, null, transaction);
        }

        public List<T> QuerySql<T>(string sql)
        {
            return conn.Query<T>(sql, null, transaction).ToList();
        }

        #region Dicionary Mode
        public List<Dictionary<string, object>> GetAllItemDict(string tableName)
        {
            string sql = string.Format("SELECT * FROM {0} WHERE 1 = 1;", tableName);
            return conn.QueryDictionary(sql, null, transaction);
        }

        public Dictionary<string, object> GetItemDict(string tableName, string property, string id)
        {
            id = tableUtils.EscapeValue(id);

            string sql = string.Format("SELECT * FROM {0} WHERE {1} = {2};", tableName, tableUtils.EscapeIlleagal(property), id);
            var list = conn.QueryDictionary(sql, null, transaction);

            return list.FirstOrDefault();
        }

        public virtual List<Dictionary<string, object>> GetItemListDict(string tableName, List<FilterCondition> where)
        {
            string filter = tableUtils.FilterConditionToWhere(where);
            string sort = tableUtils.FilterConditionToSort(where);

            if (sort != string.Empty)
            {
                sort = string.Format("ORDER BY {0}", sort);
            }

            string sql = string.Format("SELECT * FROM {0} WHERE {1} {2};", tableName, filter, sort);

            return conn.QueryDictionary(sql, null, transaction).ToList();
        }

        public virtual List<Dictionary<string, object>> GetItemListDict(string tableName, FilterCondition where)
        {
            List<FilterCondition> filter = new List<FilterCondition>() { where };
            return GetItemListDict(tableName, filter);
        }
        #endregion
    }

    public static class DapperExtension
    {
        public static List<Dictionary<string, object>> QueryDictionary(this IDbConnection conn, string sql, object param = null, IDbTransaction transaction = null)
        {
            IEnumerable<dynamic> result = SqlMapper.Query(conn, sql, param, transaction);
            var data = result as IEnumerable<IDictionary<string, object>>;

            return data.Select(r => r.ToDictionary(k => k.Key, v => v.Value)).ToList();
        }
    }

}