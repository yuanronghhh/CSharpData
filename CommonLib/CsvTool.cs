using CommonLib.Service;
using CsvHelper.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CommonLib.Utils;

namespace CommonLib.CsvTool
{
    public static class CsvCommon
    {
        public static bool LoadCsvData(string path, Func<Dictionary<string, object>, bool> preHandler, string delimiter = "\t", bool hasHeader = true)
        {
            CsvConfiguration cfg = new CsvConfiguration(CultureInfo.CurrentCulture);
            cfg.Delimiter = delimiter;
            cfg.HasHeaderRecord = hasHeader;

            using (StreamReader st = new StreamReader(path))
            {
                using (CsvHelperReader csv = new CsvHelperReader(st, cfg))
                {
                    while (csv.Read())
                    {
                        Dictionary<string, object> obj = csv.GetRecord();
                        if (obj == null || !preHandler(obj))
                        {
                            continue;
                        }
                    }
                }
            }

            return true;
        }

        public static bool LoadCsvToDatabase(string tableName, string path, string connString, Func<Dictionary<string, object>, bool> preHandler)
        {
            using (SQLServerClientService s2 = SQLServerClientService.GetInstance(connString))
            {
                LoadCsvData(path, (o) =>
                {
                    if (!preHandler(o))
                    {
                        return false;
                    }

                    s2.InsertItemDict(tableName, o);
                    return true;
                });

                s2.Commit();
            }

            return true;
        }

    }

    public class CsvCli
    {
        public struct DataParam
        {
            public string Name { get; set; }
            public object Value { get; set; }
            public string Type { get; set; }
        }

        public static void PrintCsvHelp()
        {
            Console.WriteLine(@"用法: PROG [参数]

参数：
    -f <filename.csv>        文件名
    -t <table-name>          表名，默认为文件名
    -c <connection-string>   连接配置           
    [-e <pair>]              预处理键值, 仅支持Guid,DateTime.

pair:
    指定自动指定的键值，例如[{Name:'SystemUID',Type:'Guid',Value:'337D2639-318B-4C42-B2D6-0393D1A06E70'}],
    给字段赋值Value，否则根据Type赋值生成的GUID.

返回值:
    成功/失败");
        }

        public static bool ProcessKey(Dictionary<string, object> data, List<DataParam> pValue)
        {
            if (data == null || pValue == null || pValue.Count == 0) { return true; }

            foreach (DataParam dp in pValue)
            {
                if (string.IsNullOrWhiteSpace(dp.Name))
                {
                    Console.WriteLine("请指定预处理中的 Name 值");
                    return false;
                }

                if (dp.Value == null)
                {
                    if (dp.Type == "Guid")
                    {
                        data[dp.Name] = Guid.NewGuid();
                    }
                    else if (dp.Type == "DateTime")
                    {
                        data[dp.Name] = DateTime.Now;
                    }
                }
                else
                {
                    data[dp.Name] = dp.Value;
                }
            }

            return true;
        }

        public static bool CsvCliMain(string[] args)
        {
            string tablename = "";
            string filename = "";
            string config = "";
            string pair = "";
            List<DataParam> pValue = null;

            if (args.Length < 1)
            {
                PrintCsvHelp(); return false;
            }

            if (!CliTools.GetArgValue(args, "f", false, ref filename)
                || !CliTools.GetArgValue(args, "c", false, ref config))
            {
                PrintCsvHelp(); return false;
            }

            if (!CliTools.GetArgValue(args, "t", false, ref tablename))
            {
                tablename = Path.GetFileName(filename);
            }

            if (CliTools.GetArgValue(args, "e", false, ref pair))
            {
                pValue = JsonConvert.DeserializeObject<List<DataParam>>(pair);
            }

            if (!CsvCommon.LoadCsvToDatabase(tablename, filename, config, (e) =>
            {
                return ProcessKey(e, pValue);
            }))
            {
                PrintCsvHelp(); return false;
            }

            PrintCsvHelp();
            return false;
        }
    }
}
