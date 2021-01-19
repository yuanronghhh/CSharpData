using Microsoft.VisualStudio.TestTools.UnitTesting;
using CommonLib.Service;
using System;
using CommonLibTest.Entity;
using CommonLib.TableBasePackage;

namespace CommonLibTest
{
    [TestClass]
    public class ElasticsearchClientTest
    {
        [TestMethod]
        public void TestInsertBasic()
        {
            using (ElasticsearchService s1 = ElasticsearchService.GetInstance())
            {
                Data d = new Data()
                {
                    ID = Guid.NewGuid(),
                    Age = 1,
                    Name = "Name"
                };

                FilterCondition filter = new FilterCondition("bool", TableCompareType.STREE, 
                    new FilterCondition("must", TableCompareType.STREE, 
                    new FilterCondition("ID", TableCompareType.EQ, d.ID)));

                s1.InsertItem("data", d);
                Data nd = s1.GetItem<Data>("data", filter);
                Assert.AreEqual(nd.Age, d.Age);
            }
        }

        [TestMethod]
        public void TestRemoveItem()
        {
            using (ElasticsearchService s1 = ElasticsearchService.GetInstance())
            {
                Data d = new Data()
                {
                    ID = Guid.NewGuid(),
                    Age = 1,
                    Name = "Name"
                };

                FilterCondition filter = new FilterCondition("bool", TableCompareType.STREE,
                    new FilterCondition("must", TableCompareType.STREE,
                    new FilterCondition("Age", TableCompareType.EQ, d.ID)));

                s1.InsertItem("data", d);

                Assert.IsTrue(s1.RemoveItem<Data>("data", filter));
                Assert.IsNull(s1.GetItem<Data>("data", filter));
            }
        }
    }
}
