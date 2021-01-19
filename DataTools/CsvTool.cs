using CommonLib.Service;
using CommonLib.Utils;
using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace CommonLib.DataTools
{
    public class CsvTools
    {
        public static void LoadCsvData(string tableName, string path, string dbconn, Func<Dictionary<string, object>, bool> preHandler)
        {
            CsvConfiguration cfg = new CsvConfiguration(CultureInfo.InvariantCulture, hasHeaderRecord: true, delimiter: "\t");
            using (SQLServerClientService sqlService = SQLServerClientService.GetInstance(dbconn))
            {
                using (StreamReader st = new StreamReader(path))
                {
                    using (CsvReader csv = new CsvReader(st, cfg))
                    {
                        while (csv.Read())
                        {
                            Dictionary<string, object> obj = csv.GetRecord();

                            if (!preHandler(obj))
                            {
                                continue;
                            }

                            sqlService.InsertItemDict(tableName, obj);
                        }
                    }
                }

                sqlService.Commit();
            }
        }
    }
}
