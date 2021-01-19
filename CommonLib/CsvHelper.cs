using System.Collections.Generic;
using CsvHelper;
using System.Linq;
using System.IO;
using CsvHelper.Configuration;

namespace CommonLib.Utils
{
    public static class CsvHelperExtend
    {
        public static Dictionary<string, object> GetRecord(this CsvReader reader)
        {
            IDictionary<string, object> result;
            if (reader == null) { return new Dictionary<string, object>(); }

            dynamic dyn = reader.GetRecord<dynamic>();
            if(dyn == null) { return null; }

            result = dyn as IDictionary<string, object>;
            return result.ToDictionary(d => d.Key, e => e.Value);
        }

        public static List<Dictionary<string, object>> GetRecords(this CsvReader reader)
        {
            if(reader == null) { return new List<Dictionary<string, object>>(); }

            IEnumerable<IDictionary<string, object>> result = reader.GetRecords<dynamic>() as IEnumerable<IDictionary<string, object>>;
            if(result == null) { return new List<Dictionary<string, object>>(); }

            return result.Select(r => r.ToDictionary(k => k.Key, v => v.Value)).ToList();
        }
    }

    public class CsvHelperReader: CsvReader
    {
        public CsvHelperReader(StreamReader sr, CsvConfiguration cfg) : base(sr, cfg)
        {
        }
    }
}