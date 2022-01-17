using System;
using System.Collections.Generic;
using System.Linq;
using Commonlib.Reflection;
using CommonLib.TableBasePackage;
using MongoDB.Driver;

namespace CommonLib.DatabaseClient
{
    public abstract class ABSMongoDBTableBase : ABSTableBase
    {
    }

    public interface IMongoDBClientBase
    {
        bool BulkInsertItemList<T>(string tableName, List<T> data);
    }

    public abstract class MongoDBBase : ABSMongoDBTableBase, ITableBase
    {
        public IMongoClient conn = null;
        public string connString = string.Empty;
        public Stack<IClientSessionHandle> transactionStack = new Stack<IClientSessionHandle>();
        public IClientSessionHandle transaction = null;
        public IMongoDatabase db = null;

        public MongoDBBase(string connString)
        {
            conn = GetConnection(connString);
        }

        public IMongoClient GetConnection(string connString)
        {
            if (conn == null)
            {
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                conn = new MongoClient(connString);
            }

            return conn;
        }

        public void BeginTransaction()
        {
            if(transaction != null)
            {
                return;
            }

            transaction = conn.StartSession();
            transaction.StartTransaction(new TransactionOptions(
                readConcern: ReadConcern.Snapshot,
                writeConcern: WriteConcern.WMajority));
            transactionStack.Push(transaction);
        }

        public void Commit()
        {
            transaction.CommitTransaction();
        }

        public void RollBack()
        {
            transaction.AbortTransaction();
        }

        public virtual void Dispose()
        {
            transaction = transactionStack.Pop();
            transaction.Dispose();

            if (transactionStack.Count > 0)
            {
                transaction = transactionStack.First();
                return;
            }
        }

        public bool InsertItem<T>(string tableName, T data)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);

            collection.InsertOne(transaction, data);
            return true;
        }

        public bool InsertItemList<T>(string tableName, List<T> data)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);

            collection.InsertMany(transaction, data);
            return true;
        }

        public bool UpdateItem<T>(string tableName, FilterCondition cond, string column, object value)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(cond.Key, cond.Value);
            UpdateDefinition<T> update = null;

            update = Builders<T>.Update.AddToSet(column, value);

            return 1 == (int)collection.UpdateOne(transaction, filter, update).ModifiedCount;
        }

        public bool UpdateItem<T>(string tableName, FilterCondition cond, T data, string[] columns)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(cond.Key, cond.Value);
            UpdateDefinition<T> update = null;
            if (columns == null) { return false; }

            foreach (string col in columns)
            {
                update = Builders<T>.Update.AddToSet(col, ReflectionCommon.GetValue<T>(data, col));
            }

            return 1 == (int)collection.UpdateOne(transaction, filter, update).ModifiedCount;
        }

        public T GetItem<T>(string tableName, FilterCondition filter)
        {
            return GetItem<T>(tableName, new List<FilterCondition>() { filter });
        }

        public T GetItem<T>(string tableName, List<FilterCondition> filter)
        {

            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> where = FilterConditionToWhere<T>(filter);

            var rs = collection.Find(transaction, where).FirstOrDefault();

            return rs;
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);

            return collection.Find(transaction, Builders<T>.Filter.Where(d => true)).ToList();
        }

        public List<T> GetItemList<T>(string tableName, List<FilterCondition> filter)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> where = FilterConditionToWhere<T>(filter);
            SortDefinition<T> sort = FilterConditionToSort<T>(filter);

            return collection.Find(transaction, where).Sort(sort).ToList();
        }

        public List<T> GetItemList<T>(string tableName, FilterCondition filter)
        {
            return GetItemList<T>(tableName, new List<FilterCondition>() { filter });
        }

        public bool RemoveAllItem<T>(string tableName)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);

            return collection.DeleteMany<T>(transaction, d => true).DeletedCount > 0;
        }

        public bool RemoveItem<T>(string tableName, FilterCondition filter)
        {
            return RemoveItem<T>(tableName, new List<FilterCondition>() { filter });
        }

        public bool RemoveItem<T>(string tableName, List<FilterCondition> filter)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> where = FilterConditionToWhere<T>(filter);

            return 1 == (int)collection.DeleteOne(transaction, where).DeletedCount;
        }

        public bool RemoveItemList<T>(string tableName, List<FilterCondition> filter)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> where = FilterConditionToWhere<T>(filter);

            return (int)collection.DeleteMany(transaction, where).DeletedCount > 0;
        }

        public int CountItemList<T>(string tableName, List<FilterCondition> filter)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> where = FilterConditionToWhere<T>(filter);

            return (int)collection.CountDocuments(where);
        }

        public FilterDefinition<T> FilterConditionToWhere<T>(List<FilterCondition> filter)
        {
            FilterDefinitionBuilder<T> where = Builders<T>.Filter;
            FilterDefinition<T> fd = where.Empty;
            if (filter == null) { return fd; }

            foreach (FilterCondition s in filter)
            {
                fd = GetFromFilterType<T>(fd, s);
            }

            return fd;
        }

        public FilterDefinition<T> GetFromFilterType<T>(FilterDefinition<T> fd, FilterCondition s)
        {
            FilterDefinition<T> nfd = GetFromCompareType<T>(s);

            switch (s.FilterType)
            {
                case TableFilterType.OR:
                    return fd | nfd;
                case TableFilterType.AND:
                    return fd & nfd;
                default:
                    return fd & nfd;
            }
        }

        public FilterDefinition<T> GetFromCompareType<T>(FilterCondition s)
        {
            FilterDefinitionBuilder<T> filter = Builders<T>.Filter;
            if (s.Value == null) { return filter.Empty; }

            switch (s.CompareType)
            {
                case TableCompareType.EQ:
                    return filter.Eq(s.Key, s.Value);
                case TableCompareType.GT:
                    return filter.Gt(s.Key, s.Value);
                case TableCompareType.GTE:
                    return filter.Gte(s.Key, s.Value);
                case TableCompareType.LT:
                    return filter.Lt(s.Key, s.Value);
                case TableCompareType.LTE:
                    return filter.Lte(s.Key, s.Value);
                case TableCompareType.NE:
                    return filter.Ne(s.Key, s.Value);
                case TableCompareType.REGEX:
                    {
                        return filter.Regex(s.Key, s.Value.ToString());
                    }
                case TableCompareType.TEXT:
                    return filter.Text(s.Value.ToString());
                default:
                    return filter.Eq(s.Key, s.Value);
            }
        }

        public SortDefinition<T> FilterConditionToSort<T>(List<FilterCondition> filter)
        {
            SortDefinitionBuilder<T> sort = Builders<T>.Sort;
            SortDefinition<T> sd = null;
            if (filter == null) { return sd; }

            foreach (FilterCondition s in filter)
            {
                if(!s.OrderType.HasValue)
                {
                    continue;
                }

                if (s.OrderType == TableOrderType.DESCENDING)
                {
                    sd = sort.Combine(sort.Descending(s.Key));
                } else
                {
                    sd = sort.Combine(sort.Ascending(s.Key));
                }
            }

            return sd;
        }
    }

    public abstract class MongoDBClientBase : MongoDBBase, IMongoDBClientBase
    {
        public MongoDBClientBase(string connString) : base(connString)
        {
        }

        public override List<T> GetItemList<T>(string tableName, List<FilterCondition> where, ref PageCondition page)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = FilterConditionToWhere<T>(where);
            SortDefinition<T> sort = FilterConditionToSort<T>(where);

            page.Total = (int)collection.CountDocuments(transaction, filter);
            List<T> list = collection.Find(transaction, filter).Sort(sort).Skip((page.PageNo - 1) * page.PageSize).Limit(page.PageSize).ToList();

            return list;
        }

        public bool BulkInsertItemList<T>(string tableName, List<T> data)
        {
            if (data == null || data.Count == 0)
            {
                return true;
            }
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            List<WriteModel<T>> requests = new List<WriteModel<T>>();
            BulkWriteOptions option = new BulkWriteOptions() { IsOrdered = false };

            foreach (T d in data)
            {
                WriteModel<T> m = new InsertOneModel<T>(d);
                requests.Add(m);
            }

            return collection.BulkWrite(transaction, requests, option).InsertedCount > 0;
        }
    }
}
