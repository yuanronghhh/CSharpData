using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace CommonLib.Configuration
{
    public class ConfigClass
    {
        static string configFile = string.Format("config.json");
        static string data = File.ReadAllText(configFile);

        public static string Get(string name)
        {
            Dictionary<string, string> dic = JsonConvert.DeserializeObject<Dictionary<string, string>>(data);

            return dic[name];
        }
    }
}
