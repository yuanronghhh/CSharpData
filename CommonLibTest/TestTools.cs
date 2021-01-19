using Microsoft.VisualStudio.TestTools.UnitTesting;
using CommonLib.Service;
using System.IO;
using CsvHelper.Configuration;
using System.Globalization;
using System.Collections.Generic;
using System;
using CommonLib.Utils;

namespace CommonLibTest
{
    [TestClass]
    public class TestTools
    {
        static string conn1 = ConfigClass.JGet("SQLServerConnStr-local") as string;

        public class TestDictMap: ClassMap<Dictionary<string, object>>
        {
            public TestDictMap()
            {
                Map(m => m).Index(0);
            }
        }

        [TestMethod]
        public void TestLoadFromCsv()
        {
            string path = @"D:\GreyHound\FILES\TMP\DSRoadVehicleFlowMoney.csv";
            CsvConfiguration cfg = new CsvConfiguration(CultureInfo.InvariantCulture);
            Type tp = typeof(Dictionary<string, object>);

            using (SQLServerClientService sqlService = SQLServerClientService.GetInstance(conn1))
            {
                using (StreamReader st = new StreamReader(path))
                {
                    using (CsvHelperReader csv = new CsvHelperReader(st, cfg))
                    {
                        while (csv.Read())
                        {
                            Dictionary<string, object> obj = csv.GetRecord();
                          
                            obj["GroupUID"] = Guid.NewGuid();
                            obj["SystemUID"] = Guid.NewGuid();

                            sqlService.InsertItemDict("DSRoadVehicleFlowMoney", obj);
                        }
                    }
                }

                sqlService.Commit();
            }
        }
    }
}
