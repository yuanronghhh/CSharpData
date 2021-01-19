﻿using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.IO;

namespace CommonLib.Service
{
    public class ConfigClass
    {
        static string JConfig = string.Format("config.json");
        {
            get
            {
                if (_JData != null)
                {
                    return _JData;
                }

                if(!File.Exists(JConfig)) 
                {
                    return new Dictionary<string, object>();
                }

                _JData = JsonConvert.DeserializeObject<Dictionary<string, object>>( File.ReadAllText(JConfig));
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