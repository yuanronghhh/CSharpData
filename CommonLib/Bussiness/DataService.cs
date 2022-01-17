using CommonLib.TableBasePackage;
using log4net;
using System;
using System.Collections.Generic;
using System.Reflection;

/// <summary>
/// 本文件为参考示例写法
/// </summary>
namespace CommonLib.Service
{
    public class DataService: IDisposable
    {
        public ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        static RedisClientService redis = null;
        static SQLServerClientService sqlServer = null;

        /// <summary>
        /// 使用注意：
        /// 1. redis增加/删除因为将会压入操作队列，所以不会立即生效，如果有必要，需要调用RedisCommit()，或最后Commit()一次。
        /// 2. 实体数据需要添加 TableName, DataBaseFields 两个属性才能正确使用SQL，Redis不影响
        /// </summary>
        public DataService()
        {
            if (sqlServer == null)
            {
                sqlServer = SQLServerClientService.GetInstance();
            }

            if(redis == null)
            {
                redis = RedisClientService.GetInstance();
            }
        }

        public void Dispose()
        {
            sqlServer.Dispose();
            redis.Dispose();
        }

        public void Commit()
        {
            sqlServer.Commit();
            redis.Commit();
        }

        public void RedisCommit()
        {
            redis.Commit();
        }

        public void RollBack()
        {
            sqlServer.RollBack();
        }

        public T GetItem<T>(string tableName, string key)
        {
            return redis.GetItem<T>(tableName, key);
        }

        public List<T> GetItemWild<T>(string tableName, FilterCondition filter)
        {
            return redis.GetItemWild<T>(tableName, filter);
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            return redis.GetAllItem<T>(tableName);
        }

        public bool SetItem<T>(string tableName, string redisKey, T obj)
        {
            if(!sqlServer.InsertItem<T>(tableName, obj))
            {
                return false;
            }

            redis.SetItem<T>(tableName, redisKey, obj);
            return true;
        }

        public bool RemoveItem<T>(string tableName, FilterCondition where)
        {
            if (!sqlServer.RemoveItem<T>(tableName, where))
            {
                return false;
            }

            redis.RemoveItem(tableName, where.Value.ToString());
            return true;
        }

        public bool RemoveItemWildSuffix<T>(string tableName, FilterCondition where)
        {
            string pattern = string.Format("{0}*", where.Value);

            if(!sqlServer.RemoveItem<T>(tableName, where))
            {
                return false;
            }

            return redis.RemoveItemWild(tableName, pattern);
        }

        public bool RemoveItemWildPrefix<T>(string tableName, FilterCondition where)
        {
            string pattern = string.Format("*{0}", where.Key);

            if (!sqlServer.RemoveItem<T>(tableName, where))
            {
                return false;
            }

            return redis.RemoveItemWild(tableName, pattern);
        }

        public bool RemoveAllItem<T>(string tableName)
        {
            if (!sqlServer.RemoveAllItem<T>(tableName))
            {
                return false;
            }

            return redis.DeleteKey(tableName);
        }

        public Dictionary<string, object> GetItemDict(string tableName, string key)
        {
            return GetItem<Dictionary<string, object>>(tableName, key);
        }

        public bool SetItemDict(string tableName, string redisKey, Dictionary<string, object> obj)
        {
            try
            {
                if (!sqlServer.InsertItemDict(tableName, obj))
                {
                    return false;
                }

                redis.SetItem(tableName, redisKey, obj);
                return true;
            }
            catch(Exception err)
            {
                Logger.Error("SetItemDict 失败:" + err);
                return false;
            }
        }

        public bool RemoveItemDict(string tableName, FilterCondition where)
        {
            return RemoveItem<Dictionary<string, object>>(tableName, where);
        }
    }
}
