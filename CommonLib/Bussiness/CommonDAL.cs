using StackExchange.Redis;
using System.Data.SQLite;
using System.Data.SqlClient;
using CommonLib.DatabaseClient;
using MongoDB.Driver;
using System.Data;
using MySql.Data.MySqlClient;
using System.Collections.Generic;
using Nest;
using Elasticsearch.Net;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using Aspose.Cells;

namespace CommonLib.Service
{
    #region SQLite
    public class SQLiteClientService : SQLiteClientBase
    {
        private static Dictionary<string, SQLiteClientService> instanceTable = new Dictionary<string, SQLiteClientService>();

        public SQLiteClientService(string connString): base(connString)
        {
        }

        public static SQLiteClientService GetInstance(string connString = null)
        {
            SQLiteClientService instance = null;
            if (connString != null)
            {
                if (!instanceTable.TryGetValue(connString, out instance))
                {
                    instance = new SQLiteClientService(connString);
                    instanceTable.Add(connString, instance);
                }
            }
            else
            {
                connString = ConfigClass.JGet("SQLiteConnStr") as string;
                if (string.IsNullOrWhiteSpace(connString)) { return null; }

                instance = new SQLiteClientService(connString);
                instanceTable.Add(connString, instance);
            }

            instance.BeginTransaction();

            return instance;
        }

        public override IDbConnection GetConnection(string connString)
        {
            if (conn == null)
            {
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
        private static Dictionary<string, MySQLClientService> instanceTable = new Dictionary<string, MySQLClientService>();

        public MySQLClientService(string connString) : base(connString)
        {
        }

        public static MySQLClientService GetInstance(string connString = null)
        {
            MySQLClientService instance = null;
            if (connString != null)
            {
                if (!instanceTable.TryGetValue(connString, out instance))
                {
                    instance = new MySQLClientService(connString);
                    instanceTable.Add(connString, instance);
                }
            }
            else
            {
                connString = ConfigClass.JGet("MySQLConnStr") as string;
                if (string.IsNullOrWhiteSpace(connString)) { return null; }

                instance = new MySQLClientService(connString);
                instanceTable.Add(connString, instance);
            }

            if (instance.conn.State != ConnectionState.Open)
            {
                instance.conn.Open();
            }
            instance.BeginTransaction();

            return instance;
        }

        public override IDbConnection GetConnection(string connString)
        {
            if (conn == null)
            {
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
        private static Dictionary<string, SQLServerClientService> instanceTable = new Dictionary<string, SQLServerClientService>();

        public SQLServerClientService(string connString) : base(connString)
        {
        }

        public static SQLServerClientService GetInstance(string connString = null)
        {
            SQLServerClientService instance = null;
            if (connString != null)
            {
                if (!instanceTable.TryGetValue(connString, out instance))
                {
                    instance = new SQLServerClientService(connString);
                    instanceTable.Add(connString, instance);
                }
            }
            else
            {
                connString = ConfigClass.JGet("SQLServerConnStr-local") as string;
                if (string.IsNullOrWhiteSpace(connString)) { return null; }

                instance = new SQLServerClientService(connString);
                instanceTable.Add(connString, instance);
            }
          
            if (instance.conn.State != ConnectionState.Open)
            {
                instance.conn.Open();
            }
            instance.BeginTransaction();

            return instance;
        }

        public override IDbConnection GetConnection(string connString)
        {
            if (conn == null)
            {
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
        private static Dictionary<string, RedisClientService> instanceTable = new Dictionary<string, RedisClientService>();

        public RedisClientService(string cStr = null) : base(cStr)
        {
        }

        public static RedisClientService GetInstance(string cStr = null)
        {
            RedisClientService instance = null;
            if (cStr != null)
            {
                if (!instanceTable.TryGetValue(cStr, out instance))
                {
                    instance = new RedisClientService(cStr);
                    instanceTable.Add(cStr, instance);
                }
            }
            else
            {
                cStr = ConfigClass.JGet("RedisServerConnStr") as string;
                if (string.IsNullOrWhiteSpace(cStr)) { return null; }

                instance = new RedisClientService(cStr);
                instanceTable.Add(cStr, instance);
            }

            instance.BeginTransaction();

            return instance;
        }
    }
    #endregion

    #region MongoDBServer
    public class MongoDBClientService : MongoDBClientBase
    {
        public static Dictionary<string, MongoDBClientService> instanceTable = new Dictionary<string, MongoDBClientService>();

        public MongoDBClientService(string connString, string dbName = null) : base(connString)
        {
            dbName = dbName ?? ConfigClass.JGet("MongoDBDataBase") as string;
            db = conn.GetDatabase(dbName);
        }

        public static MongoDBClientService GetInstance(string connString = null)
        {
            MongoDBClientService instance = null;
            if (!string.IsNullOrWhiteSpace(connString))
            {
                if (!instanceTable.TryGetValue(connString, out instance))
                {
                    return null;
                }
            }
            else
            {
                connString = ConfigClass.JGet("MongoDBServerConnStr") as string;
                if (string.IsNullOrWhiteSpace(connString)) { return null; }

                instance = new MongoDBClientService(connString);
                instanceTable.Add(connString, instance);
            }

            instance.BeginTransaction();
            return instance;
        }
    }
    #endregion

    #region Elasticsearch
    public class ElasticsearchService : ElasticsearchClientBase
    {
        public static Dictionary<string, ElasticsearchService> instanceTable = new Dictionary<string, ElasticsearchService>();

        public ElasticsearchService(List<Uri> connStrings) : base(connStrings)
        {
        }

        public static ElasticsearchService GetInstance(string connString = null)
        {
            ElasticsearchService instance = null;

            if (connString != null)
            {
                if (!instanceTable.TryGetValue(connString, out instance))
                {
                    instance = new ElasticsearchService(GetConfig(connString));
                    instanceTable.Add(connString, instance);
                }
            }
            else
            {
                connString = "ElasticsearchStr";
                instance = new ElasticsearchService(GetConfig(connString));
                instanceTable.Add(connString, instance);
            }

            return instance;
        }

        public static List<Uri> GetConfig(string connStrings)
        {
            JArray cfg = ConfigClass.JGet(connStrings) as JArray;
            if (cfg == null) { return null; }
            return cfg.Values<string>().ToList().ConvertAll(d => new Uri(d));
        }
    }
    #endregion

    #region ExcelClient
    public class ExcelClientService : ExcelClientBase
    {
        private static Dictionary<string, ExcelClientService> instanceTable = new Dictionary<string, ExcelClientService>();

        public ExcelClientService(string path) : base(path)
        {
        }

        public static ExcelClientService GetInstance(string path = null)
        {
            ExcelClientService instance = null;
            if (path != null)
            {
                if (!instanceTable.TryGetValue(path, out instance))
                {
                    instance = new ExcelClientService(path);
                    instanceTable.Add(path, instance);
                }
            }
            else
            {
                path = ConfigClass.JGet("ExcelPath") as string;
                if (string.IsNullOrWhiteSpace(path)) { return null; }

                instance = new ExcelClientService(path);
                instanceTable.Add(path, instance);
            }

            return instance;
        }

        public override Workbook GetConnection(string path)
        {
            if (book == null)
            {
                path = path ?? ConfigClass.JGet("ExcelPath") as string;
                if (string.IsNullOrWhiteSpace(path))
                {
                    return null;
                }

                book = new Workbook(path);
            }

            return book;
        }
    }
    #endregion
}
