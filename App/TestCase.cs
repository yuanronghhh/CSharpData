using App.Entity;
using Commonlib.Reflection;
using CommonLib.CommonDAL;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Newtonsoft.Json;
using CommonLib.TableBasePackage;

namespace App.Test
{
    public class TestCase
    {
        public static void InitSQLServerData()
        {
            List<Data> sList = new List<Data>();
            var propList = TableClass.GetTableFieldProperties<Data>(new Data());
            // sqlServer.RemoveAllItem("Data");

            for (int i = 12000001; i <= 100000000; i++)
            {
                Data sData = new Data()
                {
                    Age = i,
                    ID = Guid.NewGuid(),
                    Name = "Name" + i,
                    Remark = "Remark" + i,
                };
                sList.Add(sData);

                if (i % 1000000 == 0)
                {
                    using (SQLServerClientService sqlServer = new SQLServerClientService())
                    {
                        sqlServer.BulkInsertItemList<Data>("Data", sList, propList);
                        sqlServer.Commit();

                        Console.WriteLine("Insert OK {0}", i);
                        sList = new List<Data>();
                    }
                }
            }
        }
        public static void TestDeleteInstert()
        {
            using (SQLServerClientService sqlServer = new SQLServerClientService())
            {
                FilterCondition filter = new FilterCondition()
                {
                    Key = "ID",
                    Pattern = 1,
                    CompareType = TableCompareType.EQ,
                };

                var rdata = sqlServer.GetItemList<Data>("Data", filter);

                foreach (var d in rdata)
                {
                    if (!sqlServer.RemoveItem<Data>("Data", "ID", d.ID.ToString()))
                    {
                        Console.WriteLine("rollback RemoveItem");
                        sqlServer.RollBack();
                    }
                }

                foreach (var d in rdata)
                {
                    d.Remark = string.Format("Remark {0}", (d.Age + 1));

                    if (!sqlServer.InsertItem<Data>("Data", d))
                    {
                        Console.WriteLine("rollback InsertItem");
                        sqlServer.RollBack();
                    }
                }

                sqlServer.Commit();
            }
        }

        public static void TestSelectUpdate()
        {
            using (SQLServerClientService sqlServer = new SQLServerClientService())
            {
                List<FilterCondition> filter = new List<FilterCondition>() { };
                PageCondition page = new PageCondition() { PageNo = 10, PageSize = 10 };

                var rdata = sqlServer.GetItemList<Data>("Data", filter, page);

                foreach (var d in rdata)
                {
                    d.Remark = string.Format("Remark {0}", (d.Age + 1));

                    if (!sqlServer.UpdateItem<Data>("Data", "ID", d.ID.ToString(), d, new string[] { "Remark" }))
                    {
                        Console.WriteLine("rollback InsertItem");
                        sqlServer.RollBack();
                    }
                }

                sqlServer.Commit();
            }
        }

        public static void TestPagingWithIndex()
        {
            List<FilterCondition> conds = new List<FilterCondition>()
            {
                new FilterCondition()
                {
                    Key = "Age",
                    Pattern = null,
                    OrderType = TableOrderType.DESCENDING,
                },
            };

            using (RedisClientService redis = new RedisClientService())
            {
                var data = redis.GetItemListByRank<Data>("Data", 10000, 2, conds);
                Console.WriteLine("DataCount: {0}", data.Count);

                redis.Commit();
            }
        }

        public static void InitMongoDBData()
        {
            // mongo.RemoveAllItem<Data>("Data");
            List<Data> sList = new List<Data>();
            for (int i = 1; i <= 100000000; i++)
            {
                Data sData = new Data()
                {
                    Age = i,
                    ID = Guid.NewGuid(),
                    Name = "Name" + i,
                    Remark = "Remark" + i,
                };
                sList.Add(sData);

                if (sList.Count % 100000 == 0)
                {
                    using (MongoDBClientService mongo = new MongoDBClientService())
                    {
                        mongo.BulkInsertItemList<Data>("Data", sList);
                        mongo.Commit();

                        sList = new List<Data>();

                        Console.WriteLine("Index: {0}", i);
                    }
                }
            }

            /*
            for (int i = 1; i <= 10; i++)
            {
                Data sData = new Data()
                {
                    Age = i,
                    ID = Guid.NewGuid(),
                    Name = "Name" + i,
                    Remark = "Remark" + i,
                };
                sList.Add(sData);

                if (sList.Count % 2 == 0)
                {
                    using (MongoDBClientService mongo = new MongoDBClientService())
                    {
                        mongo.BulkInsertItemList<Data>("Data2", sList);
                        mongo.Commit();

                        sList = new List<Data>();
                    }
                }
            }
            */
        }

        public static void BulkInSQLite()
        {
            using (SQLiteClientService sqlite = new SQLiteClientService())
            {
                // sqlite.RemoveAllItem<Data>("Data");
                List<string> propList = TableClass.GetTableFieldName<Data>(new Data());
                List<Data> sList = new List<Data>();
                for (int i = 1; i <= 100000; i++)
                {
                    Data sData = new Data()
                    {
                        Age = i,
                        ID = Guid.NewGuid(),
                        Name = "Name" + i,
                        Remark = "Remark" + i,
                    };
                    sList.Add(sData);
                }

                sqlite.InsertItemList<Data>("Data", sList);
                sqlite.Commit();
            }
        }

        public static void PrintList<T>(List<T> data, string column, string other)
        {
            foreach (var d in data)
            {
                Console.WriteLine("{0}", ReflectionCommon.GetValue(d, column));
            }

            Console.WriteLine("Other: {0}", other);
        }

        public static void SQLitePaging()
        {
            //List<FilterCondition> filter = new List<FilterCondition>() {
            //    new FilterCondition("Remark", orderType: TableOrderType.DESCENDING),
            //    new FilterCondition("Age", TableCompareType.IN, new List<object>() { 102, 103, 155 },  groupName: "a1"),
            //    new FilterCondition("Age", TableCompareType.LT, 110,  groupName: "a2", groupConnection: TableFilterType.OR),
            //    new FilterCondition("Age", TableCompareType.GT, 102, groupName: "a2", filterType: TableFilterType.AND ),
            //    new FilterCondition("Name", TableCompareType.LIKE, "%Name1%",  groupName: "g2"),
            //    new FilterCondition("Name", TableCompareType.LIKE, "%Name200%", groupName: "g2", groupConnection: TableFilterType.AND, filterType: TableFilterType.OR),
            //};

            using (SQLServerClientService sqlServer = new SQLServerClientService())
            {
                PageCondition page = new PageCondition(1, 100);
                List<FilterCondition> filter = new List<FilterCondition>() { new FilterCondition("Age", TableCompareType.LT, 200, orderType: TableOrderType.DESCENDING)};

                Guid ID = Guid.NewGuid();
                Data d2 = new Data() { Age = 1, ID = ID, Name = "Name1", Remark = "Remark1" };
                sqlServer.InsertItem<Data>("Data", d2);
                sqlServer.InsertItemList<Data>("Data", new List<Data>() { d2 });
                sqlServer.BulkInsertItemList<Data>("Data", new List<Data>() { d2 });

                sqlServer.RemoveItem<Data>("Data", "Age", "1");
                sqlServer.RemoveItemList<Data>("Data", filter);

                sqlServer.GetItemList<Data>("Data", filter);
                sqlServer.GetItemList<Data>("Data", filter, page);
                sqlServer.GetItem<Data>("Data", "Age", "1");

                sqlServer.UpdateItem<Data>("Data", "ID", ID.ToString(), d2, new string[] { "Age" });

                sqlServer.Commit();
            }
        }

        public static void MySQLPaging()
        {
            using (MySQLClientService mysql = new MySQLClientService())
            {
                List<FilterCondition> filter = new List<FilterCondition>() {
                    new FilterCondition()  { Key = "Age", CompareType = TableCompareType.LT, Pattern = 200 },
                    new FilterCondition()  { Key = "Remark", CompareType = TableCompareType.LIKE, Pattern = "%Remark2%" },
                    new FilterCondition()  { Key = "Remark", OrderType = TableOrderType.DESCENDING },
                };
                PageCondition page = new PageCondition() { PageNo = 1, PageSize = 100 };

                List<Data> list = mysql.GetItemList<Data>("Data", filter, page);
                page.Total = mysql.CountItemList<Data>("Data", filter);

                PrintList(list, "Age", page.Total.ToString());

                mysql.Commit();
            }
        }

        public static void InitMySQLData()
        {
            List<Data> sList = new List<Data>();
            // sqlServer.RemoveAllItem("Data");

            //for (int i = 1; i <= 1000000; i++)
            //{
            //    Data sData = new Data()
            //    {
            //        Age = i,
            //        ID = Guid.NewGuid(),
            //        Name = "Name" + i,
            //        Remark = "Remark" + i,
            //    };
            //    sList.Add(sData);
            //}

            using (MySQLClientService mysql = new MySQLClientService())
            {
                string fName = @"C:\Users\GreyHound\AppData\Local\Temp\tmpA4B3.tmp";
                mysql.BulkLoadFromFile("Data", fName);

                //mysql.InsertItemList<Data>("Data", sList);
                //Console.WriteLine("Insert OK {0}", "Done");
                mysql.Commit();
            }
        }

        public static void EntityNotComplete()
        {
            using (MongoDBClientService sqlite = new MongoDBClientService())
            {
                List<FilterCondition> filter = new List<FilterCondition>() {
                    new FilterCondition()  { Key = "Age", CompareType = TableCompareType.LT, Pattern = 200 },
                    new FilterCondition()  { Key = "Remark", CompareType = TableCompareType.REGEX, Pattern = "/Remark2\\w*/" },
                    new FilterCondition()  { Key = "Remark", OrderType = TableOrderType.DESCENDING },
                };
                PageCondition page = new PageCondition() { PageNo = 1, PageSize = 100 };

                List<WebData> list = sqlite.GetItemList<WebData>("Data2", filter, page);

                PrintList(list, "Remark", JsonConvert.SerializeObject(filter));

                sqlite.Commit();
            }
        }

        public static void Run()
        {
            Stopwatch sw = new Stopwatch();
            long cost1 = 0, cost2 = 0;
            /*
            sw.Start();
            InitSQLServerData();
            sw.Stop();
            Console.WriteLine("Cost Time: {0}", sw.ElapsedMilliseconds);

            sw.Start();
            TestDeleteInstert();
            sw.Stop();
            Console.WriteLine("Cost Time: {0}", sw.ElapsedMilliseconds);

            sw.Start();
            TestSelectUpdate();
            sw.Stop();
            Console.WriteLine("Cost Time: {0}", sw.ElapsedMilliseconds);

            sw.Restart();
            TestPagingNoIndex();
            sw.Stop();
            cost2 = sw.ElapsedMilliseconds;
            */

            sw.Restart();
            SQLitePaging();
            sw.Stop();
            cost1 = sw.ElapsedMilliseconds;

            Console.WriteLine("index: {0} => noIndex: {1}", cost1, cost2);
        }
    }
}
