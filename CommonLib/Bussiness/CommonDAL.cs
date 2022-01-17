using System.Data.SQLite;
using System.Data.SqlClient;
using CommonLib.DatabaseClient;
using System.Data;
using MySql.Data.MySqlClient;
using System.Collections.Generic;
using Aspose.Cells;

namespace CommonLib.Service
{
    #region SQLite
    public class SQLiteClientService : SQLiteClientBase
    {
        public int stackCount = 0;
        private static Dictionary<string, SQLiteClientService> instanceTable = new Dictionary<string, SQLiteClientService>();

        public SQLiteClientService(string connString): base(connString)
        {
            stackCount = 0;
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

            instance.stackCount += 1;
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

        public override void Dispose()
        {
            if (stackCount > 1)
            {
                stackCount -= 1;
                return;
            }

            base.Dispose();
        }
    }
    #endregion

    #region MySQL
    public class MySQLClientService : MySQLClientBase
    {
        public int stackCount = 0;
        private static Dictionary<string, MySQLClientService> instanceTable = new Dictionary<string, MySQLClientService>();

        public MySQLClientService(string connString) : base(connString)
        {
            stackCount = 0;
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

            instance.stackCount += 1;
            return instance;
        }

        public override void Dispose()
        {
            if (stackCount > 1)
            {
                stackCount -= 1;
                return;
            }

            base.Dispose();
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
        public int stackCount = 0;
        private static Dictionary<string, SQLServerClientService> instanceTable = new Dictionary<string, SQLServerClientService>();

        public SQLServerClientService(string connString) : base(connString)
        {
            stackCount = 0;
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

            instance.stackCount += 1;
            return instance;
        }

        public override void Dispose()
        {
            if (stackCount > 1)
            {
                stackCount -= 1;
                return;
            }

            base.Dispose();
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
        public int stackCount = 0;
        private static Dictionary<string, RedisClientService> instanceTable = new Dictionary<string, RedisClientService>();

        public RedisClientService(string cStr = null) : base(cStr)
        {
            stackCount = 0;
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

            instance.stackCount += 1;
            return instance;
        }

        public override void Dispose()
        {
            if (stackCount > 1)
            {
                stackCount -= 1;
                return;
            }

            base.Dispose();
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
