using CommonLib.DataTools;
using CommonLib.Service;
using System;
using System.Collections.Generic;

namespace DataTools
{
    public class Tools
    {
        static string conn1 = ConfigClass.JGet("SQLServerConnStr-local") as string;
        static string conn2 = ConfigClass.JGet("SQLServerConnStr-206") as string;
        static string conn3 = ConfigClass.JGet("SQLServerConnStr-204") as string;

        public static void LoadCsv(string[] args)
        {
            string path = @"D:\GreyHound\FILES\TMP\DSRoadVehicleFlowMoney.csv";
            CsvTools.LoadCsvData("DSRoadVehicleFlowMoney", path, conn1, (d) =>
            {
                d["SystemUID"] = Guid.NewGuid();
                d["GroupUID"] = Guid.NewGuid();

                return true;
            });

            Console.ReadKey();
        }

        public static void UpdateData()
        {
            string tb = "OMRoadYearTask";
            using (SQLServerClientService sv204 = SQLServerClientService.GetInstance(conn3))
            using (SQLServerClientService svLocal = SQLServerClientService.GetInstance(conn1))
            {
                List<Dictionary<string, object>> gp = sv204.GetAllItemDict(tb);
                svLocal.InsertItemListDict(tb, gp);
                svLocal.Commit();
            }
        }
    }

    class DataTools
    {
        static void Main(string[] args)
        {
            Tools.UpdateData();
            Console.WriteLine("Done");
            Console.ReadKey();
        }
    }
}
