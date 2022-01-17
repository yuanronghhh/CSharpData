using Commonlib.Reflection;
using CommonLib.TableBasePackage;
using CommonLib.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CommonLib.DatabaseClient
{
    public interface IRedisBase
    {
        bool RemoveItem(string tableName, string key);
        bool RemoveItemWild(string tableName, string pattern);
        bool DeleteKey(string tableName);

        bool SetItem<T>(string tableName, string key, T obj);

        List<string> GetKeys(string hostAndPort, string name);

        T GetItem<T>(string tableName, string key);
        List<T> GetItemWild<T>(string tableName, string pattern);
        List<T> GetItemWild<T>(string tableName, FilterCondition filter);
        List<T> GetAllItem<T>(string tableName);
        List<T> GetAllItem<T>(string tableName, List<string> keys);
    }

    public abstract class RedisBaseService : IRedisBase, IDisposable
    {
        public IConnectionMultiplexer conn = null;
        public IDatabase db { get { return conn.GetDatabase(); } }
        public ISubscriber sub { get { return conn.GetSubscriber(); } }
        public ITransaction transaction { get; set; }

        public RedisBaseService(string conStr)
        {
            conn = GetConnection(conStr);
            BeginTransaction();
        }

        public virtual IConnectionMultiplexer GetConnection(string connString)
        {
            if (conn == null || !conn.IsConnected)
            {
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }
                conn = ConnectionMultiplexer.Connect(connString);
            }

            return conn;
        }

        public void BeginTransaction()
        {
            if (transaction != null)
            {
                return;
            }

            transaction = db.CreateTransaction();
        }

        public void Commit()
        {
            if(transaction == null)
            {
                return;
            }

            transaction.Execute();
            transaction = null;
            
            BeginTransaction();
        }

        public void RollBack()
        {
            if (transaction == null)
            {
                return;
            }

            BeginTransaction();
        }

        public virtual void Dispose()
        {
            if (transaction != null)
            {
                transaction = null;
            }

            conn.Close();
            conn.Dispose();
        }

        public void SubscribeItem(string cName, Action<ChannelMessage> handler)
        {
            sub.Subscribe(cName).OnMessage(handler);
        }

        public void PublishItem(string cName, string msg)
        {
            sub.Publish(cName, msg);
        }

        public virtual bool RemoveItem(string tableName, string key)
        {
            transaction.HashDeleteAsync(tableName, key);
            return true;
        }

        public virtual bool RemoveItemWild(string tableName, string pattern)
        {
            RedisValue rValue = new RedisValue(pattern);
            foreach (HashEntry he in db.HashScan(tableName, rValue))
            {
                RemoveItem(tableName, he.Name);
            }

            return true;
        }

        public virtual bool DeleteKey(string tableName)
        {
            transaction.KeyDeleteAsync(tableName);
            return true;
        }

        public virtual bool SetItemList<T>(string tableName, string key, List<T> obj)
        {
            foreach (var o in obj)
            {
                SetItem(tableName, key, o);
            }

            return true;
        }

        public virtual bool SetItem<T>(string tableName, string key, T obj)
        {
            transaction.HashSetAsync(tableName, key, DataConvert.ObjectToString(obj));
            return true;
        }

        public virtual long Count(string tableName)
        {
            return db.HashLength(tableName);
        }

        public List<string> GetKeys(string hostAndPort, string name)
        {
            IServer server = conn.GetServer(hostAndPort);
            return server.Keys(db.Database, name).ToList().ConvertAll(d => d.ToString());
        }

        public T GetItem<T>(string tableName, string key)
        {
            RedisValue rv = db.HashGetAsync(tableName, key).Result;
            if (!rv.HasValue) return default(T);
            return DataConvert.StringToObject<T>(rv);
        }

        public List<T> GetItemWild<T>(string tableName, string pattern)
        {
            RedisValue rValue = new RedisValue(pattern);
            return db.HashScan(tableName, rValue).ToList().ConvertAll(d =>
            {
                return DataConvert.StringToObject<T>(d.Value);
            });
        }

        public List<T> GetItemWild<T>(string tableName, FilterCondition filter)
        {
            List<T> list = new List<T>();
            if (filter.Value == null)  { return new List<T>(); }

            RedisValue rValue = new RedisValue(filter.Value.ToString());

            foreach (var h in db.HashScan(tableName, rValue))
            {
                list.Add(DataConvert.StringToObject<T>(h.Value));
            }

            return list;
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            HashEntry[] he = db.HashGetAll(tableName);
            return he.ToList().ConvertAll(d => {
                return DataConvert.StringToObject<T>(d.Value);
            });
        }

        public List<T> GetAllItem<T>(string tableName, List<string> keys)
        {
            if(keys == null) { return new List<T>(); }
            RedisValue[] IDs = keys.ConvertAll(d => new RedisValue(d)).ToArray();

            List<T> list = new List<T>();

            RedisValue[] rv = db.HashGet(tableName, IDs);
            list = rv.ToList().ConvertAll(d =>
            {
                return DataConvert.StringToObject<T>(d);
            });

            return list;
        }

        #region 分页处理
        public List<T> GetItemListBySearchStr<T>(List<T> list, FilterCondition condition)
        {
            if(list == null) { return new List<T>(); }

            return list.FindAll(d =>
            {
                object val = ReflectionCommon.GetValue(d, condition.Key);
                if (val == null)
                {
                    return false;
                }

                return condition.GetRegexValue().IsMatch(val.ToString());
            });
        }

        public List<T> GetItemListByConditions<T>(string tableName, List<T> list, List<FilterCondition> filter)
        {
            List<T> result = new List<T>();
            List<T> subList = null;
            if(filter == null || list == null || list.Count == 0) { return result; }

            for (int i = 0; i < filter.Count; i++)
            {
                FilterCondition cond = filter[i];

                if (cond.Value != null)
                {
                    cond.CompareType = !cond.Value.ToString().Contains("*") ? TableCompareType.EQ : TableCompareType.TEXT;

                    if (cond.CompareType == TableCompareType.EQ)
                    {
                        result = result.Concat(list.FindAll(d =>
                        {
                            object val = ReflectionCommon.GetValue(d, cond.Key);
                            if (val == null)
                            {
                                return false;
                            }

                            return cond.Value.ToString() == val.ToString();
                        })).ToList();
                    }
                    else
                    {
                        subList = GetItemListBySearchStr(list, cond);
                        if (subList.Count == 0)
                        {
                            result.Clear();
                            break;
                        }

                        result = result.Concat(subList).ToList();
                    }
                }
                else
                {
                    result = list;
                }
            }

            return result;
        }

        public List<T> GetItemListByRank<T>(string tableName, int pageSize, int pageNo, List<FilterCondition> filter)
        {
            List<T> list = new List<T>();
            List<string> iSet = new List<string>();

            #region Search
            if (filter != null)
            {
                list = GetAllItem<T>(tableName);
                list = GetItemListByConditions<T>(tableName, list, filter);
            }
            #endregion

            list = OrderItemList<T>(list, filter);
            if (pageSize != 0)
            {
                list = list.Skip((pageNo - 1) * pageSize).Take(pageSize).ToList();
            }

            return list;
        }

        public IOrderedEnumerable<T> OrderItemList<T>(IOrderedEnumerable<T> list, FilterCondition cond)
        {
            return list.ThenBy(u => ReflectionCommon.GetValue(u, cond.Key));
        }

        public List<T> OrderItemList<T>(List<T> list, List<FilterCondition> conds)
        {
            IOrderedEnumerable<T> olist = null;
            if (conds == null || conds.Count == 0)
            {
                return list;
            }

            olist = list.OrderBy(d => ReflectionCommon.GetValue(d, conds[0].Key));
            foreach (FilterCondition c in conds)
            {
                olist = OrderItemList<T>(olist, c);
            }

            return olist.ToList();
        }
        #endregion
    }

    public abstract class RedisClientBase : RedisBaseService
    {
        public RedisClientBase(string conStr = null) : base(conStr)
        {
        }
    }
}
