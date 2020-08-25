using CommonLib.CommonDAL;
using System;
using System.Collections.Generic;

/// <summary>
/// 本文件为参考示例写法
/// </summary>
namespace App.Service
{
    public class DataService: IDisposable
    {
        static RedisClientService redis = null;
        static SQLiteClientService sqlite = null;

        /// <summary>
        /// 使用注意：
        /// 1. redis增加/删除因为将会压入操作队列，所以不会立即生效，如果有必要，需要调用RedisCommit()，或最后Commit()一次。
        /// 2. 实体数据需要添加 TableDecorator, TableFields 两个属性才能正确使用SQL，Redis不影响
        /// </summary>
        public DataService()
        {
            if (sqlite == null)
            {
                sqlite = new SQLiteClientService();
            }

            if(redis == null)
            { 
                redis = new RedisClientService();
            }
        }

        public void Dispose()
        {
            sqlite.Dispose();
            redis.Dispose();
        }

        public void Commit()
        {
            sqlite.Commit();
            redis.Commit();
        }

        public void RedisCommit()
        {
            redis.Commit();
        }

        public void RollBack()
        {
            sqlite.RollBack();
            redis.RollBack();
        }

        public T GetItem<T>(string tableName, string key)
        {
            return redis.GetItem<T>(tableName, key);
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            return redis.GetAllItem<T>(tableName);
        }

        public bool SetItem<T>(string tableName, string redis_key, T obj)
        {
            if(!sqlite.InsertItem<T>(tableName, obj))
            {
                return false;
            }

            redis.SetItem<T>(tableName, redis_key, obj);
            return true;
        }

        public bool RemoveItem<T>(string tableName, string primaryName, string id)
        {
            if (!sqlite.RemoveItem<T>(tableName, primaryName, id))
            {
                return false;
            }

            redis.RemoveItem(tableName, id);
            return true;
        }

        public bool RemoveItemWildSuffix<T>(string tableName, string primaryName, string id)
        {
            string pattern = string.Format("{0}*", id);

            if(!sqlite.RemoveItem<T>(tableName, primaryName, id))
            {
                return false;
            }

            return redis.RemoveItemWild(tableName, pattern);
        }

        public bool RemoveItemWildPrefix<T>(string tableName, string primaryName, string id)
        {
            string pattern = string.Format("*{0}", id);

            if (!sqlite.RemoveItem<T>(tableName, primaryName, id))
            {
                return false;
            }

            return redis.RemoveItemWild(tableName, pattern);
        }

        public bool RemoveAllItem<T>(string tableName)
        {
            if (!sqlite.RemoveAllItem<T>(tableName))
            {
                return false;
            }

            return redis.RemoveAllItem(tableName);
        }
    }
}
