using System.Collections.Generic;
using System.Linq;
using Commonlib.Reflection;
using CommonLib.TableBasePackage;
using MongoDB.Driver;

namespace CommonLib.DatabaseClient
{
    public abstract class ABSMongoDBTableBase : ABSTableBase
    {
        public abstract IMongoClient GetConnection(string conStr);

        public abstract IMongoDatabase GetDataBase(string dbname = null);
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
        public string esChar = "\"";

        public MongoDBBase(string conStr)
        {
            conn = GetConnection(conStr);
        }

        public virtual string Escape(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return string.Empty;
            }

            return string.Format("{1}{0}{1}", name, esChar);
        }

        public void BeginTransaction()
        {
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

        public void Dispose()
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

        public bool UpdateItem<T>(string tableName, string property, string id, T data, string[] columns)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(property, id);
            UpdateDefinition<T> update = null;

            foreach (string col in columns)
            {
                update = Builders<T>.Update.AddToSet(col, ReflectionCommon.GetValue<T>(data, col));
            }

            return 1 == (int)collection.UpdateOne(transaction, filter, update).ModifiedCount;
        }

        public T GetItem<T>(string tableName, string property, string id)
        {
            id = this.Escape(id);
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(property, id);

            var rs = collection.Find(transaction, filter).ToList();

            return rs.FirstOrDefault();
        }

        public List<T> GetAllItem<T>(string tableName)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);

            return collection.Find(transaction, Builders<T>.Filter.Where(d => true)).ToList();
        }

        public List<T> GetItemList<T>(string tableName, List<FilterCondition> where)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = FilterConditionToWhere<T>(where);
            SortDefinition<T> sort = FilterConditionToSort<T>(where);

            return collection.Find(transaction, filter).Sort(sort).ToList();
        }

        public List<T> GetItemList<T>(string tableName, FilterCondition where)
        {
            return GetItemList<T>(tableName, new List<FilterCondition>() { where });
        }

        public bool RemoveAllItem<T>(string tableName)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinitionBuilder<T> filter = Builders<T>.Filter;

            return collection.DeleteMany<T>(transaction, d => true).DeletedCount > 0;
        }

        public bool RemoveItem<T>(string tableName, string property, string id)
        {
            id = Escape(id);

            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinitionBuilder<T> filter = Builders<T>.Filter;
            FilterDefinition<T> fd = filter.Eq(property, id);

            return 1 == (int)collection.DeleteOne(transaction, fd).DeletedCount;
        }

        public bool RemoveItemList<T>(string tableName, List<FilterCondition> where)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> fd = FilterConditionToWhere<T>(where);

            return (int)collection.DeleteMany(transaction, fd).DeletedCount > 0;
        }

        public int CountItemList<T>(string tableName, List<FilterCondition> where)
        {
            IMongoCollection<T> collection = db.GetCollection<T>(tableName);
            FilterDefinition<T> filter = FilterConditionToWhere<T>(where);

            return (int)collection.Find(transaction, filter).CountDocuments();
        }

        public FilterDefinition<T> FilterConditionToWhere<T>(List<FilterCondition> conds)
        {
            FilterDefinitionBuilder<T> filter = Builders<T>.Filter;
            FilterDefinition<T> fd = filter.Empty;
            foreach (FilterCondition s in conds)
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
            if (s.Pattern == null) { return filter.Empty; }

            switch (s.CompareType)
            {
                case TableCompareType.EQ:
                    return filter.Eq(s.Key, s.Pattern);
                case TableCompareType.GT:
                    return filter.Gt(s.Key, s.Pattern);
                case TableCompareType.GTE:
                    return filter.Gte(s.Key, s.Pattern);
                case TableCompareType.LT:
                    return filter.Lt(s.Key, s.Pattern);
                case TableCompareType.LTE:
                    return filter.Lte(s.Key, s.Pattern);
                case TableCompareType.NE:
                    return filter.Ne(s.Key, s.Pattern);
                case TableCompareType.REGEX:
                    {
                        TextSearchOptions so = new TextSearchOptions() { CaseSensitive = false };
                        return filter.Regex(s.Key, s.Pattern.ToString());
                    }
                case TableCompareType.TEXT:
                    return filter.Text(s.Pattern.ToString());
                default:
                    return filter.Eq(s.Key, s.Pattern);
            }
        }

        public SortDefinition<T> FilterConditionToSort<T>(List<FilterCondition> conds)
        {
            SortDefinitionBuilder<T> sort = Builders<T>.Sort;
            SortDefinition<T> sd = null;

            foreach (FilterCondition s in conds)
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
        public MongoDBClientBase(string conStr) : base(conStr)
        {
        }

        public override List<T> GetItemList<T>(string tableName, List<FilterCondition> where, PageCondition page)
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
