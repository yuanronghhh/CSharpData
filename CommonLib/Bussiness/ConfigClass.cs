using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.IO;

namespace CommonLib.Service
{
    public class ConfigClass
    {
        static string JConfig = string.Format("config.json");        static Dictionary<string, object> _JData = null;        static Dictionary<string, object> JData
        {
            get
            {
                if (_JData != null)
                {
                    return _JData;
                }

                if(File.Exists(JConfig)) 
                {
                    _JData = JsonConvert.DeserializeObject<Dictionary<string, object>>( File.ReadAllText(JConfig));                }
                else
                {
                    _JData = new Dictionary<string, object>();
                }

                return _JData;            }
        }

        public static string CGet(string name)
        {
            ConnectionStringSettings value = ConfigurationManager.ConnectionStrings[name];
            if(value == null)
            {
                return "";
            }

            return value.ConnectionString;
        }
        public static string Get(string name)
        {
            string value = ConfigurationManager.AppSettings.Get(name);

            return value;
        }

        public static object JGet(string name)
        {
            if (!JData.ContainsKey(name)){
                return null;
            }

            return JData[name];
        }
    }
}
