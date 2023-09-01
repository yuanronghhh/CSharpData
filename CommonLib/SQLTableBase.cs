using Commonlib.Reflection;
using CommonLib.TableBasePackage;
using CommonLib.TableData;
using Dapper;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using static Dapper.SqlMapper;

namespace CommonLib.SQLTablePackage
{
    public class SQLTableUtils
    {
        public string esChar = "\'";

        public int Index = 0;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="p"></param>
        /// <returns></returns>
        public bool IsPrimaryKey(PropertyInfo p)
        {
            if (p == null) { return false; }

            PrimaryKey decorator;
            object attribute = p.GetCustomAttributes(typeof(PrimaryKey), false).FirstOrDefault();
            if (attribute == null)
            {
                return false;
            }

            decorator = attribute as PrimaryKey;
            return decorator.IsPrimaryKey;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public List<PropertyInfo> GetPrimaryKeyFields<T>()
        {
            return ReflectionCommon.GetFieldProperties<T>(IsPrimaryKey);
        }

        public void ResetUniqParam()
        {
            Index = 0;
        }

        public string GenUniqParam(string pm)
        {
            Index += 1;
            return pm + "_" + Index;
        }

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
            if (string.IsNullOrWhiteSpace(name)) { return ""; }
            if(name.IndexOf(" ") > -1) { return ""; }
            name = name.Replace(esChar, "");

            return name;
        }

        public string EscapeValue(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) { return ""; }
            name = name.Replace(esChar, "");

            if (string.IsNullOrWhiteSpace(name))
            {
                return string.Empty;
            }

            return string.Format("{0}", name);
        }

        public Dictionary<string, List<FilterCondition>> FilterConditionToGroup(List<FilterCondition> conds)
        {
            Dictionary<string, List<FilterCondition>> cond = new Dictionary<string, List<FilterCondition>>();
            if (conds == null) { return cond; }

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

        public string FilterConditionToWhere(FilterCondition cond)
        {
            return FilterConditionToWhere(new List<FilterCondition> { cond });
        }

        public string FilterConditionToWhere(List<FilterCondition> conds)
        {
            string fd = string.Empty;

            Dictionary<string, List<FilterCondition>> cond = FilterConditionToGroup(conds);

            foreach (var kp in cond)
            {
                fd = GetGroupFromFilter(fd, EscapeIlleagal(kp.Key), kp.Value);
            }

            if (string.IsNullOrWhiteSpace(fd))
            {
                fd = "1 = 1";
            }
            ResetUniqParam();

            return fd;
        }

        public DynamicParameters FilterConditionToParam(FilterCondition cond)
        {
            return FilterConditionToParam(new List<FilterCondition> { cond });
        }

        public DynamicParameters FilterConditionToParam(List<FilterCondition> conds)
        {
            if (conds == null || conds.Count == 0) { return null; }

            DynamicParameters dp = new DynamicParameters();

            foreach (var c in conds.Where(d => !d.OrderType.HasValue))
            {
                dp.Add("@W_" + GenUniqParam(c.Key), c.Value);
            }

            ResetUniqParam();
            return dp;
        }

        public string GetGroupFromFilter(string fd, string key, List<FilterCondition> fcs)
        {
            string nfd = string.Empty;
            TableFilterType? gft = null;

            if (fcs == null || fcs.Count == 0)
            {
                return fd;
            }

            if (key != "_default" && !string.IsNullOrWhiteSpace(fd))
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
            if (!withGroup || string.IsNullOrWhiteSpace(fd)) { return fd; }
            return string.Format("({0})", fd);
        }

        public string ConnectFilter(string con, string fd, string nfd, bool withGroup = false)
        {
            if (string.IsNullOrWhiteSpace(fd)) { return GroupFilter(nfd, withGroup); }
            if(string.IsNullOrWhiteSpace(nfd)) { return fd; }

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
            if (string.IsNullOrWhiteSpace(nfd)) { return fd; }
            if (string.IsNullOrWhiteSpace(fd)) {
                return GroupFilter(nfd, withGroup);
            }

            return GroupFilterByType(s.FilterType, fd, nfd, withGroup);
        }

        public string FilterConditionToSort(List<FilterCondition> conds)
        {
            if (conds == null) { return ""; }

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
                    sort.Append(string.Format("{0} DESC,", EscapeIlleagal(s.Key)));
                }
                else
                {
                    sort.Append(string.Format("{0} ASC,", EscapeIlleagal(s.Key)));
                }
            }
            sd = sort.ToString().TrimEnd(',');

            return sd;
        }

        public string GetFromCompareType(FilterCondition s)
        {
            if (s.Value == null) { return string.Empty; }
            string pattern = string.Empty;

            pattern = GenUniqParam("@W_" + EscapeValue(s.Key));
            switch (s.CompareType)
            {
                case TableCompareType.EQ:
                    {
                        return string.Format("{0} = {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.GT:
                    {
                        return string.Format("{0} > {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.GTE:
                    {
                        return string.Format("{0} >= {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.LT:
                    {
                        return string.Format("{0} < {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.LTE:
                    {
                        return string.Format("{0} <= {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.NE:
                    {
                        return string.Format("{0} <> {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.LIKE:
                    {
                        return string.Format("{0} LIKE {1}", EscapeIlleagal(s.Key), pattern);
                    }
                case TableCompareType.IN:
                    {
                        List<object> pList = null;
                        if (s.Value.GetType().IsArray)
                        {
                            object[] pArr = s.Value as object[];
                            pList = pArr.ToList();
                        }
                        else
                        {
                            pList = s.Value as List<object>;
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
                        pattern = EscapeValue(s.Key);
                        return string.Format("{0} = {1}", EscapeIlleagal(s.Key), pattern);
                    }
            }
        }

        public string JoinStringListParam(List<string> strArray)
        {
            if(strArray == null) { return ""; }

            StringBuilder str = new StringBuilder();
            foreach (string k in strArray)
            {
                str.Append("@" + GenUniqParam(k) + ",");
            }
            str.Remove(str.Length - 1, 1);
            ResetUniqParam();

            return str.ToString();
        }

        public string JoinStringList(List<string> sarr, bool withEschar = false, bool withbrackets = false)
        {
            StringBuilder str = new StringBuilder();
            if(sarr == null) { return ""; }

            if (withEschar)
            {
                str.Append(esChar + string.Join(esChar + "," + esChar, sarr) + esChar);
            }
            else if (withbrackets)
            {
                str.Append('[' + string.Join(']' + "," + '[', sarr) + ']');
            } else 
            {
                str.Append(string.Join(",", sarr));
            }

            return str.ToString();
        }

        public string JoinObjectList(List<object> sarr)
        {
            if(sarr == null) { return ""; }

            return JoinStringList(sarr.ConvertAll(d => d == null ? "" : d.ToString()), true);
        }

        public string JoinValueList<T>(List<T> data)
        {
            if(data == null) { return ""; }

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

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public string GetTableName<T>()
        {
            Type tp = typeof(T);
            TableName decorator;

            var attribute = tp.GetCustomAttributes(typeof(TableName), false).FirstOrDefault();
            if (attribute == null)
            {
                return "";
            }
            decorator = attribute as TableName;

            return decorator.Value;
        }
    }

    public interface ISQLTableBase: ITableBase
    {
        void BeginTransaction();
        void Commit();
        void RollBack();

        void SetEscapeChar(string esChar);
        void ChangeDataBase(string name);
        int CountItemList(string tableName, string where, object param = null);

        // Dictionary Mode
        bool InsertItemDict(string tableName, Dictionary<string, object> data);
        bool InsertItemListDict(string tableName, List<Dictionary<string, object>> data);
        bool InsertItemListDict(string tableName, List<string> columns, object param);

        Dictionary<string, object> GetItemDict(string tableName, List<FilterCondition> filter, List<string> columns = null);
        Dictionary<string, object> GetItemDict(string tableName, FilterCondition filter, List<string> columns = null);
        List<Dictionary<string, object>> GetAllItemDict(string tableName, List<string> columns = null);

        List<Dictionary<string, object>> GetItemListDict(string tableName, List<FilterCondition> where, List<string> columns = null);
        List<Dictionary<string, object>> GetItemListDict(string tableName, FilterCondition where, List<string> columns = null);

        bool UpdateItemDict(string tableName, FilterCondition filter, Dictionary<string, object> data, string[] columns);
        bool UpdateItemDict(string tableName, FilterCondition filter, string column, object value);

        List<Dictionary<string, object>> QueryDict(string sql, object param = null);
        Dictionary<string, object> QueryDictFirst(string sql, object param = null);

        // Inner Use
        List<T> Query<T>(string command, object param = null);
        T QueryFirst<T>(string command, object param = null);
        int Execute(string command, object param);
        T ExecuteScalar<T>(string command, object param = null);
    }

    public abstract class ABSSQLTableBase: ABSTableBase
    {
        public abstract IDbConnection GetConnection(string cStr);
    }

    public abstract class SQLTableBase : ABSSQLTableBase, ISQLTableBase
    {
        public IDbTransaction transaction = null;
        public IDbConnection conn = null;
        public SQLTableUtils tableUtils = new SQLTableUtils();

        public struct InsertQuery
        {
            public object Param;
            public string Values;
            public string Columns;
        }

        public struct SelectQuery
        {
            public object Param;
            public string Columns;
            public string Sort;
            public string Filter;
        }

        public struct DeleteQuery
        {
        }

        public struct UpdateQuery
        {
            public DynamicParameters Param;
            public string UpdateValues;
            public string Filters;
        }

        private UpdateQuery GetUpdateQuery<T>(T data, string[] columns, string where = null)
        {
            UpdateQuery QData = new UpdateQuery();
            if (columns == null || data == null) { return QData; }

            StringBuilder updateValues = new StringBuilder();
            DynamicParameters dp = new DynamicParameters();
            List<PropertyInfo> ps;

            Type tp = typeof(T);
            int gint = 0;
            for (int i = 0; i < columns.Length; i++)
            {
                gint++;
                var kp = columns[i];
                PropertyInfo prop = tp.GetProperty(kp);
                if (prop == null)
                {
                    continue;
                }

                object o = prop.GetValue(data);
                string fpk = string.Format("{0}_{1}", kp,  gint);

                updateValues.Append(string.Format("{0} = @{1},", kp, fpk));
                dp.Add("@" + fpk, o);
            }
            updateValues.Remove(updateValues.Length - 1, 1);
            QData.UpdateValues = updateValues.ToString();

            if (string.IsNullOrWhiteSpace(where))
            {
                StringBuilder fts = new StringBuilder();
                ps = tableUtils.GetPrimaryKeyFields<T>();
                for (int j = 0; j < ps.Count; j++)
                {
                    gint++;
                    var pk = ps[j];
                    object o = ReflectionCommon.GetValue(data, pk);
                    string fpk = string.Format("{0}_{1}", pk.Name, gint);

                    fts.Append(string.Format("{0} = @{1},", pk.Name, fpk));
                    dp.Add("@" + fpk, o);
                }
                fts.Remove(fts.Length - 1, 1);

                QData.Filters = fts.ToString();
            }
            else
            {
                QData.Filters = where;
            }
            QData.Param = dp;

            return QData;
        }

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

            BeginTransaction();
        }

        public void BeginTransaction()
        {
            if (transaction != null && transaction.Connection != null)
            {
                return;
            }

            transaction = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        }
               
        public virtual void Dispose()
        {
            if(transaction != null)
            {
                transaction.Dispose();
            }

            conn.Close();
        }
               
        public void Commit()
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
               
        public void RollBack()
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

        public InsertQuery GetInsertQueryData<T>(T data, List<string> columns)
        {
            InsertQuery QData = new InsertQuery();
            if (columns == null || data == null) { return QData; }

            DynamicParameters dp = new DynamicParameters();
            StringBuilder values = new StringBuilder();
            StringBuilder cols = new StringBuilder();
            Type tp = data.GetType();

            foreach (var kp in columns)
            {
                PropertyInfo prop = tp.GetProperty(kp);
                if (prop == null)
                {
                    continue;
                }
                string kpt = tableUtils.GenUniqParam(kp);

                if (data != null)
                {
                    object obj = prop.GetValue(data);
                    dp.Add("@" + kpt, obj);
                }

                cols.Append(kp + ",");
                values.Append("@" + kpt + ",");
            }
            values.Remove(values.Length - 1, 1);
            cols.Remove(cols.Length - 1, 1);
            tableUtils.ResetUniqParam();

            QData.Values = values.ToString();
            QData.Param = dp;
            QData.Columns = cols.ToString();

            return QData;
        }

        public SelectQuery GetSelectQuery<T>(List<FilterCondition> filter, List<string> columns)
        {
            SelectQuery qd = new SelectQuery();

            qd.Columns = columns == null || columns.Count == 0 ? "*" : tableUtils.JoinStringList(columns);
            qd.Filter = tableUtils.FilterConditionToWhere(filter);
            qd.Sort = tableUtils.FilterConditionToSort(filter);
            qd.Param = tableUtils.FilterConditionToParam(filter);

            return qd;
        }

        public bool InsertItem<T>(string tableName, T data, List<string> cols)
        {
            InsertQuery QData = GetInsertQueryData<T>(data, cols);

            string sql = string.Format("INSERT INTO {0} ( {1} ) VALUES ( {2} );", tableName,  QData.Columns, QData.Values);

            return conn.Execute(sql, QData.Param, transaction) > 0;
        }

        public bool InsertItem<T>(string tableName, T data)
        {
            List<string> cols = TableClass.GetTableFieldNames<T>();
            return InsertItem<T>(tableName, data, cols);
        }

        public int UpdateByColumn(string tableName, string column, string value, string where)
        {
            if (string.IsNullOrWhiteSpace(column)) { return 0; }
            if (string.IsNullOrWhiteSpace(value)) { return 0; }
            if (string.IsNullOrWhiteSpace(tableName)) { return 0; }
            if (string.IsNullOrWhiteSpace(where)) { return 0; }

            DynamicParameters dp = new DynamicParameters();
            string sql = string.Format("UPDATE {0} SET {1}=@{2} WHERE {3};", tableName, column, column, where);

            dp.Add(column, value);

            return conn.Execute(sql, dp, transaction);
        }

        /// <summary>
        /// 使用data更新表中的数据，columns为需要更新的字段，类似伪码
        ///     UPDATE <tableName> SET column[0] = data.Column0 WHERE <where>/primarykey
        ///     
        /// TableName 和 实体名称 不关联，比如，有实体A, A上有 A.IsEnable = 1，B 表上也有 IsEnable 的字段，tableName 传入 B，实体传入 A.
        /// 如果需要更加常用的关联方法，请看 UpdateByColumns 的另一个重载
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlService"></param>
        /// <param name="tableName"></param>
        /// <param name="data">值对象，SET更新的值将从这个实体中获取</param>
        /// <param name="columns"></param>
        /// <param name="where"></param>
        /// <returns></returns>
        public int UpdateByColumns<T>(string tableName, T data, string[] columns, string where = null)
        {
            UpdateQuery uq = GetUpdateQuery<T>(data, columns, where);

            string sql = string.Format("UPDATE {0} SET {1} WHERE {2};", tableName, uq.UpdateValues, uq.Filters);

            return conn.Execute(sql, uq.Param, transaction);
        }

        /// <summary>
        /// 使用data更新表中的数据，columns为需要更新的字段，类似伪码
        ///     UPDATE <tableName> SET column[0] = data.Column0 WHERE <where>/primarykey
        ///     
        /// TableName 和 实体名称 不关联，比如，有实体A, A上有 A.IsEnable = 1，B 表上也有 IsEnable 的字段，tableName 传入 B，实体传入 A.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlService"></param>
        /// <param name="data">值对象，更新的值将从这个实体中获取</param>
        /// <param name="columns"></param>
        /// <param name="where"></param>
        /// <returns></returns>
        public int UpdateByColumns<T>(T data, string[] columns, string where = null)
        {
            string tableName = tableUtils.GetTableName<T>();

            return UpdateByColumns<T>(tableName, data, columns, where);
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
            List<string> cols = TableClass.GetTableFieldNames<T>();

            return InsertItemList(tableName, data, cols);
        }

        public bool InsertItemList<T>(string tableName, List<T> data, List<string> cols)
        {
            if(data == null || cols == null) { return false; }

            string colStr = tableUtils.JoinStringList(cols);
            string vals = tableUtils.JoinStringListParam(cols);

            List<DynamicParameters> dpList = new List<DynamicParameters>();
            foreach (var d in data)
            {
                DynamicParameters dp = new DynamicParameters();
                Type tp = typeof(T);

                foreach (var p in cols)
                {
                    PropertyInfo prop = tp.GetProperty(p);
                    if (prop == null)
                    {
                        continue;
                    }

                    if (cols.Exists(c => c == prop.Name))
                    {
                        object obj = prop.GetValue(d);
                        dp.Add(tableUtils.GenUniqParam(p), obj);
                    }
                }
                tableUtils.ResetUniqParam();

                dpList.Add(dp);
            }

            string sql = string.Format("INSERT INTO {0} ( {1} ) VALUES ({2});", tableName, colStr, vals);

            // DebugData(sql);
            return conn.Execute(sql, dpList, transaction) > 0;
        }

        public bool UpdateItem<T>(string tableName, FilterCondition filter, string column, object value)
        {
            string sql;
            StringBuilder cols = new StringBuilder();
            string where = tableUtils.FilterConditionToWhere(filter);
            DynamicParameters param = tableUtils.FilterConditionToParam(filter);

            string nName = tableUtils.GenUniqParam(column);
            cols.Append(string.Format("{0} = @{1}", column, nName));
            param.Add(nName, value);
            tableUtils.ResetUniqParam();

            sql = string.Format("UPDATE {0} SET {1} WHERE {2};", tableName, cols, where);

            return conn.Execute(sql, param, transaction) > 0;
        }

        public bool UpdateItem<T>(string tableName, FilterCondition filter, T data, string[] columns)
        {
            return UpdateItem<T>(tableName, new List<FilterCondition>() { filter }, data, columns);
        }

        public bool UpdateItem<T>(string tableName, List<FilterCondition> filter, T data, string[] columns)
        {
            if(data == null) { return false; }

            string sql;
            object value;
            StringBuilder cols = new StringBuilder();
            Type tp = data.GetType();
            string where = tableUtils.FilterConditionToWhere(filter);
            DynamicParameters param = tableUtils.FilterConditionToParam(filter);

            foreach (string c in columns)
            {
                PropertyInfo prop = tp.GetProperty(c);
                if (prop == null) { continue; }

                value = prop.GetValue(data, null);
                string kpt = tableUtils.GenUniqParam(c);

                cols.Append(string.Format("{0} = @{1},", c, kpt));
                param.Add(kpt, value);
            }
            tableUtils.ResetUniqParam();

            cols = cols.Remove(cols.Length - 1, 1);
            sql = string.Format("UPDATE {0} SET {1} WHERE {2};", tableName, cols, where);

            return conn.Execute(sql, param, transaction) > 0;
        }

        public T GetItem<T>(string tableName, FilterCondition filter)
        {
            List<string> columns = TableClass.GetTableFieldNames<T>();

            return GetItem<T>(tableName, filter, columns);
        }

        public T GetItem<T>(string tableName, List<FilterCondition> filter)
        {
            List<string> columns = TableClass.GetTableFieldNames<T>();
            return GetItem<T>(tableName, filter, columns);
        }

        public virtual List<T> GetItemList<T>(string tableName, List<FilterCondition> filter)
        {
            List<string> columns = TableClass.GetTableFieldNames<T>();
            return GetItemList<T>(tableName, filter, columns);
        }

        public virtual List<T> GetItemList<T>(string tableName, FilterCondition filter)
        {
            List<string> columns = TableClass.GetTableFieldNames<T>();
            return GetItemList<T>(tableName, filter, columns);
        }

        public T GetItem<T>(string tableName, FilterCondition filter, List<string> columns)
        {
            return GetItem<T>(tableName, new List<FilterCondition> { filter }, columns);
        }

        public T GetItem<T>(string tableName, List<FilterCondition> filter, List<string> columns)
        {
            string cols = columns == null ? "*": tableUtils.JoinStringList(columns);
            string where = tableUtils.FilterConditionToWhere(filter);
            object param = tableUtils.FilterConditionToParam(filter);

            string sql = string.Format("SELECT {2} FROM {0} WHERE {1};", tableName, where, cols);
            return conn.QueryEntityFirst<T>(sql, param, transaction);
        }

        public virtual List<T> GetItemList<T>(string tableName, List<FilterCondition> where, List<string> columns)
        {
            SelectQuery qd = GetSelectQuery<T>(where, columns);
            string sort = string.Empty;

            if (!string.IsNullOrWhiteSpace(qd.Sort))
            {
                sort = string.Format("ORDER BY {0}", qd.Sort);
            }

            string sql = string.Format("SELECT {3} FROM {0} WHERE {1} {2};", tableName, qd.Filter, sort, qd.Columns);

            return conn.QueryEntity<T>(sql, qd.Param, transaction).ToList();
        }

        public virtual List<T> GetItemList<T>(string tableName, FilterCondition filter, List<string> columns)
        {
            List<FilterCondition> where = new List<FilterCondition>() { filter };
            return GetItemList<T>(tableName, where, columns);
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            List<string> columns = TableClass.GetTableFieldNames<T>();
            return GetAllItem<T>(tableName, columns);
        }

        public List<T> GetAllItem<T>(string tableName, List<string> columns = null)
        {
            string cols = columns == null ? "*" : tableUtils.JoinStringList(columns);
            string sql = string.Format("SELECT {1} FROM {0} WHERE 1 = 1;", tableName, cols);
            return conn.QueryEntity<T>(sql, null, transaction).ToList();
        }

        public bool RemoveAllItem<T>(string tableName)
        {
            string sql = string.Format("DELETE FROM {0} WHERE 1 = 1;", tableName);
            return conn.Execute(sql, null, transaction) > 0;
        }

        public bool RemoveItem<T>(string tableName, FilterCondition where)
        {
            return RemoveItem<T>(tableName, new List<FilterCondition> { where });
        }

        public bool RemoveItem<T>(string tableName, List<FilterCondition> filter)
        {
            string where = tableUtils.FilterConditionToWhere(filter);
            object param = tableUtils.FilterConditionToParam(filter);

            string sql = string.Format("DELETE FROM {0} WHERE {1};", tableName, where);
            return conn.Execute(sql, param, transaction) > 0;
        }

        public bool RemoveItemList<T>(string tableName, List<FilterCondition> filter)
        {
            string where = tableUtils.FilterConditionToWhere(filter);
            object param = tableUtils.FilterConditionToParam(filter);

            if (string.IsNullOrWhiteSpace(where)) { return false; }

            string sql = string.Format("DELETE FROM {0} WHERE {1};", tableName, where);
            return conn.Execute(sql, param, transaction) > 0;
        }

        public int CountItemList<T>(string tableName, List<FilterCondition> filter)
        {
            string where = tableUtils.FilterConditionToWhere(filter);
            object param = tableUtils.FilterConditionToParam(filter);

            return CountItemList(tableName, where, param);
        }

        public int CountItemList(string tableName, string filter, object param = null)
        {
            string sql = string.Format("SELECT COUNT(*) as cout FROM {0} WHERE {1};", tableName, filter);
            object sc = conn.ExecuteScalar(sql, param, transaction);

            return Convert.ToInt32(sc);
        }

        public string ReplaceUserTypeDefine(string sql, IEnumerable<KeyValuePair<string, string>> list)
        {
            foreach (var d in list)
            {
                sql = sql.Replace(d.Key.ToUpper(), d.Value);
            }

            return sql;
        }

        public int Execute(string sql, object param = null)
        {
            return conn.Execute(sql, param, transaction);
        }

        public T ExecuteScalar<T>(string sql, object param = null)
        {
            return conn.ExecuteScalar<T>(sql, param, transaction);
        }

        public List<T> Query<T>(string sql, object param = null)
        {
            return conn.QueryEntity<T>(sql, param, transaction).ToList();
        }

        public T QueryFirst<T>(string sql, object param = null)
        {
            return conn.QueryFirst<T>(sql, param, transaction);
        }

        /// Stream
        public DbDataReader OpenReader(string sql)
        {
            return conn.ExecuteReader(sql, null, transaction) as DbDataReader;
        }

        /// Dictionary Mode
        public InsertQuery GetInsertQueryDict(Dictionary<string, object> data, List<string> columns)
        {
            InsertQuery QData = new InsertQuery();
            if (columns == null || data == null) { return QData; }

            DynamicParameters dp = new DynamicParameters();
            StringBuilder values = new StringBuilder();
            StringBuilder cols = new StringBuilder();
            
            foreach (var kp in data)
            {
                if (!columns.Exists(p => kp.Key == p))
                {
                    continue;
                }

                string kpt = tableUtils.GenUniqParam(kp.Key);
                cols.Append(kp.Key + ",");
                values.Append("@" + kpt + ",");
                dp.Add("@" + kpt, kp.Value);
            }
            values.Remove(values.Length - 1, 1);
            cols.Remove(cols.Length - 1, 1);
            tableUtils.ResetUniqParam();

            QData.Values = values.ToString();
            QData.Param = dp;
            QData.Columns = cols.ToString();

            return QData;
        }

        public List<Dictionary<string, object>> GetAllItemDict(string tableName, List<string> columns = null)
        {
            string cols = columns == null ? "*" : tableUtils.JoinStringList(columns);
            string sql = string.Format("SELECT {1} FROM {0} WHERE 1 = 1;", tableName, cols);
            tableUtils.ResetUniqParam();

            return conn.QueryEntity(sql, null, transaction).ToList();
        }

        public virtual List<Dictionary<string, object>> GetItemListDict(string tableName, List<FilterCondition> filter, List<string> columns = null)
        {
            string cols = columns == null ? "*" : tableUtils.JoinStringList(columns);
            string where = tableUtils.FilterConditionToWhere(filter);
            string sort = tableUtils.FilterConditionToSort(filter);
            object param = tableUtils.FilterConditionToParam(filter);
            tableUtils.ResetUniqParam();

            if (!string.IsNullOrWhiteSpace(sort))
            {
                sort = string.Format("ORDER BY {0}", sort);
            }

            string sql = string.Format("SELECT {3} FROM {0} WHERE {1} {2};", tableName, where, sort, cols);

            return conn.QueryEntity(sql, param, transaction).ToList();
        }

        public virtual List<Dictionary<string, object>> GetItemListDict(string tableName, FilterCondition filter, List<string> columns = null)
        {
            return GetItemListDict(tableName, new List<FilterCondition>() { filter }, columns);
        }

        public Dictionary<string, object> GetItemDict(string tableName, FilterCondition filter, List<string> columns = null)
        {
            return GetItemDict(tableName, new List<FilterCondition> { filter }, columns);
        }

        public Dictionary<string, object> GetItemDict(string tableName, List<FilterCondition> filter, List<string> columns = null)
        {
            string cols = tableUtils.JoinStringList(columns);
            string where = tableUtils.FilterConditionToWhere(filter);
            object param = tableUtils.FilterConditionToParam(filter);
            if (columns == null) { cols = "*"; }

            string sql = string.Format("SELECT {2} FROM {0} WHERE {1};", tableName, where, cols);
            return conn.QueryEntityFirst(sql, param, transaction);
        }

        public bool UpdateItemDict(string tableName, List<FilterCondition> filter, Dictionary<string, object> data, string[] columns)
        {
            if (data == null || columns == null) { return false; }

            string sql;
            object value;
            StringBuilder cols = new StringBuilder();
            string where = tableUtils.FilterConditionToWhere(filter);
            DynamicParameters param = tableUtils.FilterConditionToParam(filter);

            foreach (string c in columns)
            {
                if (!data.ContainsKey(c))
                {
                    continue;
                }
                value = data[c];

                string kpt = "U_" + tableUtils.GenUniqParam(c);
               
                cols.Append(string.Format("{0} = @{1},", c, kpt));
                param.Add(kpt, value);
            }
            cols = cols.Remove(cols.Length - 1, 1);
            tableUtils.ResetUniqParam();

            sql = string.Format("UPDATE {0} SET {1} WHERE {2};", tableName, cols, where);

            return conn.Execute(sql, param, transaction) > 0;
        }

        public bool UpdateItemDict(string tableName, FilterCondition filter, Dictionary<string, object> data, string[] columns)
        {
            return UpdateItemDict(tableName, new List<FilterCondition>() { filter }, data, columns);
        }

        public bool UpdateItemDict(string tableName, FilterCondition filter, string column, object value)
        {
            return UpdateItem<Dictionary<string, object>>(tableName, filter, column, value);
        }

        public int CountItemList(string tableName, List<FilterCondition> filter)
        {
            return CountItemList<Dictionary<string, object>>(tableName, filter);
        }

        public List<Dictionary<string, object>> QueryDict(string sql, object param = null)
        {
            return conn.QueryEntity(sql, param, transaction).ToList();
        }

        public List<List<Dictionary<string, object>>> QueryMultipleDict(string sql, DynamicParameters param = null)
        {
            List<List<Dictionary<string, object>>> list = new List<List<Dictionary<string, object>>>();

            using (GridReader dg = conn.QueryMultiple(sql, param, transaction))
            {
                while (true)
                {
                    if (dg.IsConsumed == true)
                    {
                        break;
                    }

                    List<Dictionary<string, object>> result = dg.Read<dynamic>().Select(d => new Dictionary<string, object>(d)).ToList();
                    list.Add(result);
                }
            }

            return list;
        }

        public Dictionary<string, object> QueryDictFirst(string sql, object param = null)
        {
            return conn.QueryEntityFirst(sql, param, transaction);
        }

        public bool InsertItemDict(string tableName, Dictionary<string, object> data, List<string> columns = null)
        {
            if(columns == null || columns.Count == 0) { return false; }

            InsertQuery QData = GetInsertQueryDict(data, columns);

            string sql = string.Format("INSERT INTO {0} ( {1} ) VALUES ( {2} );", tableName, QData.Columns, QData.Values);
            string dj = JsonConvert.SerializeObject(data);
            return conn.Execute(sql, QData.Param, transaction) > 0;
        }

        public bool InsertItemDict(string tableName, Dictionary<string, object> data)
        {
            List<string> cols = TableClass.GetTableNamesDict(data);

            return InsertItemDict(tableName, data, cols);
        }

        public bool InsertItemListDict(string tableName, List<string> columns, object param)
        {
            if (columns == null || columns.Count == 0) { return false; }

            string colStr = tableUtils.JoinStringList(columns, withbrackets: true);
            string vals = tableUtils.JoinStringListParam(columns);

            string sql = string.Format("INSERT INTO {0} ( {1} ) VALUES ({2});", tableName, colStr, vals);
            
            return conn.Execute(sql, param, transaction) > 0;
        }

        public bool InsertItemListDict(string tableName, List<Dictionary<string, object>> data, List<string> cols)
        {
            if(data == null || cols == null) { return false; }

            List<DynamicParameters> paramList = new List<DynamicParameters>();
            foreach (var d in data)
            {
                DynamicParameters dp = new DynamicParameters();
                foreach (var kp in d)
                {
                    if (!cols.Exists(p => kp.Key == p))
                    {
                        continue;
                    }

                    dp.Add("@" + tableUtils.GenUniqParam(kp.Key), kp.Value);
                }
                tableUtils.ResetUniqParam();

                paramList.Add(dp);
            }

            return InsertItemListDict(tableName, cols, paramList);
        }

        public bool InsertItemListDict(string tableName, List<Dictionary<string, object>> data)
        {
            if (data == null || data.Count == 0)
            {
                return true;
            }
            List<string> cols = TableClass.GetTableNamesDict(data.First());

            return InsertItemListDict(tableName, data, cols);
        }

        public bool RemoveAllItemDict(string tableName)
        {
            return RemoveAllItem<Dictionary<string, object>>(tableName);
        }

        public bool RemoveItemDict(string tableName, FilterCondition where)
        {
            return RemoveItem<Dictionary<string, object>>(tableName, where);
        }

        public bool RemoveItemDict(string tableName, List<FilterCondition> where)
        {
            return RemoveItem<Dictionary<string, object>>(tableName, where);
        }

        public Dictionary<string, object> GetItemDict(string tableName, string property, string id)
        {
            DynamicParameters dp = new DynamicParameters();
            id = tableUtils.EscapeValue(id);

            dp.Add("@" + property, id);
            string sql = string.Format("SELECT * FROM {0} WHERE {1} = @{2};", tableName, property, property);
            var list = conn.QueryEntity(sql, dp, transaction);

            return list.FirstOrDefault();
        }
    }

    public static class DapperExtension
    {
        public static List<Dictionary<string, object>> QueryEntity(this IDbConnection conn, string sql, object param = null, IDbTransaction transaction = null)
        {
            IEnumerable<IDictionary<string, object>> result = conn.Query(sql, param, transaction) as IEnumerable<IDictionary<string, object>>;
            return result.Select(r => new Dictionary<string, object>(r)).ToList();
        }

        public static IEnumerable<T> QueryEntity<T>(this IDbConnection conn, string sql, object param = null, IDbTransaction transaction = null)
        {
           return conn.Query<T>(sql, param, transaction);
        }

        public static Dictionary<string, object> QueryEntityFirst(this IDbConnection conn, string sql, object param = null, IDbTransaction transaction = null)
        {
            dynamic dn = conn.QueryFirstOrDefault(sql, param, transaction);
            if(dn == null) { return dn; }

            IDictionary<string, object> result = dn as IDictionary<string, object>;
            Dictionary<string, object> result2 = new Dictionary<string, object>(result);

            return result2;
        }

        public static T QueryEntityFirst<T>(this IDbConnection conn, string sql, object param = null, IDbTransaction transaction = null)
        {
            return conn.QueryFirst<T>(sql, param, transaction);
        }
    }
}