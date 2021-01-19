using Microsoft.VisualStudio.TestTools.UnitTesting;
using CommonLib.DatabaseClient;
using System;
using CommonLib.Service;
using System.Collections.Generic;
using CommonLib.TableBasePackage;
using CommonLibTest.Entity;

namespace CommonLibTest
{
    [TestClass]
    public class TestSQLTableDict
    {
        string conn1 = ConfigClass.JGet("SQLServerConnStr-local") as string;
        string tableName = "Data";
        Guid ID1 = new Guid("687B5566-3C79-4D50-B497-1D65CEEE9262");
        Guid ID2 = new Guid("787B5566-3C79-4D50-B497-1D65CEEE9262");

        [TestInitialize]
        public void InitSetUp()
        {
            InitData();
        }

        [TestCleanup]
        public void CleanData()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);
                sqlBase.RemoveItemDict(tableName, filter);
                sqlBase.Commit();
            }
        }

        public void InitData()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                Dictionary<string, object> obj = new Dictionary<string, object>()
                {
                    { "ID", ID1 },
                    { "Name", "Name1" },
                    { "Age", 1 },
                    { "Aliase", null }
                };

                if (!sqlBase.InsertItemDict(tableName, obj))
                {
                    Assert.Fail("Insert init failed");
                }

                sqlBase.Commit();
            }
        }

        public void InitPagingData()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                for (int i = 0; i < 10; i++)
                {
                    Dictionary<string, object> obj = new Dictionary<string, object>()
                    {
                        { "ID", Guid.NewGuid() },
                        { "Name", "Name" + i },
                        { "Age", i },
                    };

                    if (!sqlBase.InsertItemDict(tableName, obj))
                    {
                        Assert.Fail("InitPagingData init failed");
                    }
                }
                sqlBase.Commit();
            }
        }

        public void CleanPagingData()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition ft = new FilterCondition("Age", TableCompareType.LT, 10);

                if (!sqlBase.RemoveItemDict(tableName, ft))
                {
                    Assert.Fail("CleanPagingData init failed");
                }

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestConnect()
        {
            using (SQLServerClientService s1 = SQLServerClientService.GetInstance(conn1))
            {
                using (SQLServerClientService s2 = SQLServerClientService.GetInstance(conn1))
                {
                    // Connect Success
                }
            }
        }

        [TestMethod]
        public void TestInsertItemDict()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                Dictionary<string, object> obj = new Dictionary<string, object>()
                {
                    { "ID", ID1 },
                    { "Name", "Name1" },
                    { "Age", 1 },
                    { "Aliase", null }
                };

                if (!sqlBase.InsertItemDict(tableName, obj))
                {
                    Assert.Fail();
                }

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestRemoveItemDict()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);
                if (!sqlBase.RemoveItemDict(tableName, filter))
                {
                    Assert.Fail("Delete failed");
                }

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestGetItemDict()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);
                var result = new Dictionary<string, object>()
                {
                    { "ID", ID1 },
                    { "Name", "Name1" },
                    { "Age", 1 },
                    { "Aliase", null }
                };

                Dictionary<string, object> obj = sqlBase.GetItemDict(tableName, filter);
                Assert.AreEqual(result["ID"], obj["ID"]);
                Assert.AreEqual(result["Name"], obj["Name"]);
                Assert.AreEqual(result["Aliase"], obj["Aliase"]);
            }
        }

        [TestMethod]
        public void TestUpdateItemDict()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);
                var result = new Dictionary<string, object>()
                {
                    { "ID", ID1 },
                    { "Name", "UpdatedName" },
                    { "Age", 1 },
                    { "Aliase", "UpdatedAliase" }
                };

                if (!sqlBase.UpdateItemDict(tableName, filter, result, new string[] { "Name", "Aliase" }))
                {
                    Assert.Fail("UpdateItemDict Failed");
                }

                var obj = sqlBase.GetItemDict(tableName, filter);

                Assert.AreEqual(result["ID"], obj["ID"]);
                Assert.AreEqual(result["Name"], obj["Name"]);
                Assert.AreEqual(result["Aliase"], obj["Aliase"]);

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestGetItemListDict()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);

                var result = new Dictionary<string, object>()
                {
                    { "ID", ID1 },
                    { "Name", "Name1" },
                    { "Age", 1 },
                    { "Aliase", null }
                };

                List<Dictionary<string, object>> list = sqlBase.GetItemListDict(tableName, filter);
                Assert.AreEqual(list.Count, 1);

                var obj = list[0];

                Assert.AreEqual(result["ID"], obj["ID"]);
                Assert.AreEqual(result["Name"], obj["Name"]);
                Assert.AreEqual(result["Aliase"], obj["Aliase"]);

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestGetItemListDictEmpty()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter2 = new FilterCondition("id", TableCompareType.EQ, Guid.NewGuid());

                var result = new Dictionary<string, object>()
                {
                    { "ID", ID1 },
                    { "Name", "d2" },
                    { "Age", 1 },
                    { "Aliase", null }
                };

                List<Dictionary<string, object>> list2 = sqlBase.GetItemListDict(tableName, filter2);

                Assert.IsNotNull(list2);
                Assert.AreEqual(list2.Count, 0);
                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestGetItemListDictPaging1()
        {
            InitPagingData();

            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                List<FilterCondition> fts = new List<FilterCondition>()
                {
                    new FilterCondition("Age", TableCompareType.LT, 10),
                    new FilterCondition("Age", TableCompareType.GTE, 5),
                };

                PageCondition pg1 = new PageCondition(2, 2);

                List<Dictionary<string, object>> list = sqlBase.GetItemListDict(tableName, fts);
                Assert.AreEqual(list.Count, 5);
                sqlBase.Commit();
            }

            CleanPagingData();
        }

        [TestMethod]
        public void TestGetItemListDictPaging2()
        {
            InitPagingData();

            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                List<FilterCondition> fts = new List<FilterCondition>()
                {
                    new FilterCondition("Age", TableCompareType.LT, 10),
                    new FilterCondition("Age", TableCompareType.GTE, 5),
                };

                List<Dictionary<string, object>> list = sqlBase.GetItemListDict(tableName, fts);
                Assert.AreEqual(list.Count, 5);
                sqlBase.Commit();
            }

            CleanPagingData();
        }

        [TestMethod]
        public void TestInsertItem()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                Data obj = new Data()
                {
                    ID = ID1,
                    Name = "d2",
                    Aliase = null
                };

                if (!sqlBase.InsertItem(tableName, obj))
                {
                    Assert.Fail();
                }

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestRemoveItem()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                Data obj = new Data()
                {
                    ID = ID1,
                    Name = "d2",
                    Aliase = null
                };

                if (!sqlBase.InsertItem(tableName, obj))
                {
                    Assert.Fail("Insert failed before delete");
                }

                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);
                if (!sqlBase.RemoveItem<Data>(tableName, filter))
                {
                    Assert.Fail("Delete failed");
                }

                sqlBase.Commit();
            }
        }

        [TestMethod]
        public void TestGetItem()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);
                Data result = new Data()
                {
                    ID = ID1,
                    Name = "Name1",
                    Aliase = null
                };

                Data obj = sqlBase.GetItem<Data>(tableName, filter);
                Assert.AreEqual(result.ID, obj.ID);
                Assert.AreEqual(result.Name, obj.Name);
                Assert.AreEqual(result.Aliase, obj.Aliase);
            }
        }

        [TestMethod]
        public void TestUpdateItem()
        {
            using (SQLServerClientService sqlBase = SQLServerClientService.GetInstance(conn1))
            {
                FilterCondition filter = new FilterCondition("id", TableCompareType.EQ, ID1);

                Data result = new Data()
                {
                    ID = ID1,
                    Name = "UpdatedName",
                    Aliase = "UpdatedAliase",
                };

                InitData();
                if (!sqlBase.UpdateItem(tableName, filter, result, new string[] { "Name", "Aliase" }))
                {
                    Assert.Fail("UpdateItem Failed");
                }

                var obj = sqlBase.GetItem<Data>(tableName, filter);

                Assert.AreEqual(result.ID, obj.ID);
                Assert.AreEqual(result.Name, obj.Name);
                Assert.AreEqual(result.Aliase, obj.Aliase);

                sqlBase.Commit();
            }
        }
    }
}
