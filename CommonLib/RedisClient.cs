using Commonlib.Reflection;
using CommonLib.TableBasePackage;
using CommonLib.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace CommonLib.DatabaseClient
{
    public abstract class ABSRedisBase
    {
        public abstract IConnectionMultiplexer GetConnection(string conStr);
    }

    public abstract class RedisBaseService : ABSRedisBase, IDisposable
    {
        public string connString = string.Empty;
        public IConnectionMultiplexer conn = null;
        public IDatabase db { get { return conn.GetDatabase(); } }
        public ISubscriber sub { get { return conn.GetSubscriber(); } }

        public RedisBaseService(string conStr)
        {
            conn = GetConnection(conStr);
        }

        public virtual void Dispose()
        {
            conn.Close();
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
            return db.HashDelete(tableName, key);
        }

        public virtual bool RemoveItemWild(string tableName, string pattern)
        {
            RedisValue rValue = new RedisValue(pattern);
            foreach (HashEntry he in db.HashScan(tableName, rValue))
            {
                if (!db.HashDelete(tableName, he.Name))
                {
                    continue;
                }
            }

            return true;
        }
        public virtual bool RemoveAllItem(string tableName)
        {
            return db.KeyDelete(tableName);
        }
        public virtual bool SetItem<T>(string tableName, string key, T obj)
        {
            return db.HashSet(tableName, key, DataConvert.ObjectToString(obj));
        }

        public List<string> GetKeys(string hostAndPort, string name)
        {
            IServer server = conn.GetServer(hostAndPort);
            return server.Keys(db.Database, name).ToList().ConvertAll(d => d.ToString());
        }

        public T GetItem<T>(string tableName, string key)
        {
            RedisValue rv = db.HashGet(tableName, key);
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
            if (filter.Pattern == null)  { return new List<T>(); }

            if(filter.CompareType == TableCompareType.EQ)
            {
                T obj = GetItem<T>(tableName, filter.Pattern.ToString());
                if(obj != null) { list.Add(obj); }

                return  list;
            }

            RedisValue rValue = new RedisValue(filter.Pattern.ToString());

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
            List<T> rlist = new List<T>();
            List<T> subList = null;

            for(int i = 0; i < filter.Count; i++)
            {
                FilterCondition cond = filter[i];

                if (cond.Pattern != null)
                {
                    cond.CompareType = !cond.Pattern.ToString().Contains("*") ? TableCompareType.EQ : TableCompareType.TEXT;

                    if (cond.CompareType == TableCompareType.EQ)
                    {
                        rlist = rlist.Concat(list.FindAll(d =>
                        {
                            object val = ReflectionCommon.GetValue(d, cond.Key);
                            if (val == null)
                            {
                                return false;
                            }

                            return cond.Pattern.ToString() == val.ToString();
                        })).ToList();
                    }
                    else
                    {
                        subList = GetItemListBySearchStr(list, cond);
                        if (subList.Count == 0)
                        {
                            rlist.Clear();
                            break;
                        }

                        rlist = rlist.Concat(subList).ToList();
                    }
                }
                else
                {
                    rlist = list;
                }
            }

            return rlist;
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
        static TaskQueue redisQueue = null;
        static Stack<TaskQueue> redisStack = new Stack<TaskQueue>();

        /// <summary>
        /// 使用redis服务需要注意：
        /// 1. 删除/新增，会放入队列，所以必须Commit才能生效, 不需要时可以RollBack
        /// 2. 如果使用了删除/新增，GetItem时是没有删掉的
        /// 3. 所有删除/新增操作均返回true, 没有false
        /// </summary>
        /// <param name="conStr"></param>
        public RedisClientBase(string conStr = null) : base(conStr)
        {
        }

        public void BeginTransaction()
        {
            redisQueue = new TaskQueue();
            redisStack.Push(redisQueue);
        }

        public override void Dispose()
        {
            redisQueue = redisStack.Pop();
            if (redisQueue.Count > 0)
            {
                RollBack();
            }

            if (redisStack.Count > 0)
            {
                redisQueue = redisStack.First();
                return;
            }

            base.Dispose();
        }

        public void Commit()
        {
            TaskQueueData qd;

            while (redisQueue.Count > 0)
            {
                qd = (TaskQueueData)redisQueue.Dequeue();
                qd.method(qd.param);
            }
        }

        public void RollBack()
        {
            redisQueue.Clear();
        }

        public override bool RemoveItem(string tableName, string key)
        {
            Action<object[]> method = new Action<object[]>(d =>
            {
                base.RemoveItem(tableName, key);
            });

            redisQueue.EnqueueTask(method, tableName, key);
            return true;
        }

        public override bool RemoveItemWild(string tableName, string pattern)
        {
            Action<object[]> method = new Action<object[]>(d =>
            {
                base.RemoveItemWild(tableName, pattern);
            });

            redisQueue.EnqueueTask(method, tableName, pattern);
            return true;
        }

        public override bool RemoveAllItem(string tableName)
        {
            Action<object[]> method = new Action<object[]>(d =>
            {
                base.RemoveAllItem(tableName);
            });

            redisQueue.EnqueueTask(method, tableName);
            return true;
        }

        public override bool SetItem<T>(string tableName, string key, T obj)
        {
            Action<object[]> method = new Action<object[]>(d =>
            {
                base.SetItem<T>(tableName, key, obj);
            });

            redisQueue.EnqueueTask(method, key, obj);
            return true;
        }
    }
}
