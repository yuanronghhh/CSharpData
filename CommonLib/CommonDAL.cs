using StackExchange.Redis;
using CommonLib.Configuration;
using System.Data.SQLite;
using System.Data.SqlClient;
using CommonLib.DatabaseClient;
using MongoDB.Driver;
using System.Data;
using MySql.Data.MySqlClient;

namespace CommonLib.CommonDAL
{
    #region SQLite
    public class SQLiteClientService : SQLiteClientBase
    {
        public SQLiteClientService(string cStr = null): base(cStr)
        {
            BeginTransaction();
        }

        public override IDbConnection GetConnection(string cStr)
        {
            if (conn == null)
            {
                connString = cStr ?? ConfigClass.Get("SQLiteConnStr");
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                conn = new SQLiteConnection(connString);
            }

            return conn;
        }
    }
    #endregion

    #region MySQL
    public class MySQLClientService : MySQLClientBase
    {
        public MySQLClientService(string cStr = null) : base(cStr)
        {
            BeginTransaction();
        }

        public override IDbConnection GetConnection(string cStr)
        {
            if (conn == null)
            {
                connString = cStr ?? ConfigClass.Get("MySQLConnStr");
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                conn = new MySqlConnection(connString);
            }

            return conn;
        }
    }
    #endregion

    #region  SQLServer
    public class SQLServerClientService : SQLServerClientBase
    {
        public SQLServerClientService(string cStr = null) : base(cStr)
        {
            BeginTransaction();
        }

        public override IDbConnection GetConnection(string cStr)
        {
            if (conn == null)
            {
                connString = cStr ?? ConfigClass.Get("SQLServerConnStr");
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                conn = new SqlConnection(connString);
            }

            return conn;
        }
    }
    #endregion

    #region RedisServer
    public class RedisClientService : RedisClientBase
    {
        public RedisClientService(string cStr = null) : base(cStr)
        {
            BeginTransaction();
        }

        public override IConnectionMultiplexer GetConnection(string conStr)
        {
            if (conn == null || !conn.IsConnected)
            {
                connString = conStr ?? ConfigClass.Get("RedisServerConnStr");
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                conn = ConnectionMultiplexer.Connect(connString);
            }

            return conn;
        }
    }
    #endregion

    #region MongoDBServer
    public class MongoDBClientService : MongoDBClientBase
    {
        public MongoDBClientService(string cStr = null, string database = null) : base(cStr)
        {
            db = GetDataBase(database);
            BeginTransaction();
        }

        public override IMongoClient GetConnection(string conStr)
        {
            if (conn == null)
            {
                connString = conStr ?? ConfigClass.Get("MongoDBServerConnStr");
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                conn = new MongoClient(connString);
            }

            return conn;
        }

        public override IMongoDatabase GetDataBase(string dbname = null)
        {
            string defaultDB = dbname ?? ConfigClass.Get("MongoDBDataBase");
            if (string.IsNullOrWhiteSpace(defaultDB))
            {
                return null;
            }

            return conn.GetDatabase(defaultDB);
        }
    }
    #endregion
}
