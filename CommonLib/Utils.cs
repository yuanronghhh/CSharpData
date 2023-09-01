using System.Collections;
using System;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Reflection;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace CommonLib.Utils
{
    public class TaskQueueData
    {
        public Action<object[]> method { get; set; }
        public object[] param { get; set; }
    }

    public class TaskQueue : ConcurrentQueue<TaskQueueData>
    {
        public void EnqueueTask(Action<object[]> method, params object[] param)
        {
            TaskQueueData qd = new TaskQueueData()
            {
                method = method,
                param = param
            };

            base.Enqueue(qd);
        }
    }

    public class DataConvert
    {
        public static string ObjectToString(object obj)
        {
            if(obj == null) { return ""; }

            return JsonConvert.SerializeObject(obj);
        }

        public static T StringToObject<T>(string str)
        {
            return JsonConvert.DeserializeObject<T>(str);
        }

        public static byte[] ObjectToBytes(object obj)
        {
            if (obj == null) { return new byte[] { }; }

            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }

        public static T BytesToObject<T>(byte[] bts)
        {
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream(bts))
            {
                return (T)bf.Deserialize(ms);
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class DAUtils
    {
        /// <summary>
        /// 
        /// </summary>
        public static string[] hanName = new string[] { "零", "一", "二", "三", "四", "五", "六", "七", "八", "九" };
        /// <summary>
        /// 
        /// </summary>
        public static string[] hanFName = new string[] { "零", "壹", "叁", "肆", "伍", "陆", "柒", "捌", "玖" };
        /// <summary>
        /// 
        /// </summary>
        public static string[] hanUnit = new string[] { "", "十", "百", "千", "万", "十", "百", "千", "亿", "十", "百", "千"};
        /// <summary>
        /// 
        /// </summary>
        public static string[] hanFUnit = new string[] { "", "拾", "佰", "仟", "万", "拾", "佰", "仟", "亿", "拾", "佰", "仟" };
        /// <summary>
        /// 获取单个数字中文
        /// </summary>
        /// <param name="index"></param>
        /// <param name="fan"></param>
        /// <returns></returns>
        public static string Digit2Han(int index, bool fan = false)
        {
            if (index > 9 || index < 0) { return ""; }

            if (fan)
            {
                return hanFName[index];
            }

            return hanName[index];
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="numStr"></param>
        /// <param name="fan"></param>
        /// <returns></returns>
        public static string Number2HanParseInt(string numStr, bool fan = false)
        {
            string tstr = "";
            char c = '\0';
            int ui;
            string u, d;

            for (int i = 0; i < numStr.Length; i++)
            {
                c = numStr[i];
                ui = numStr.Length - i - 1;

                d = Digit2Han(c - '0', fan);
                u = fan ? hanFUnit[ui] : hanUnit[ui];

                tstr += d + u;
            }

            tstr = Regex.Replace(tstr, "零(千|百|十)", "零");
            tstr = Regex.Replace(tstr, "零(仟|佰|拾)", "零");
            tstr = Regex.Replace(tstr, "零+", "零");
            tstr = Regex.Replace(tstr, "零(万|亿)", "$1");
            tstr = Regex.Replace(tstr, "零(万|亿)", "$1");

            tstr = Regex.Replace(tstr, "(亿)万|一(十)", "$1$2");
            tstr = Regex.Replace(tstr, "(亿)万|壹(拾)", "$1$2");
            
            return tstr;
        }

        /// <summary>
        /// 获取数字中文
        /// </summary>
        /// <param name="number"></param>
        /// <param name="fan"></param>
        /// <returns></returns>
        public static string Int2Han(int number, bool fan = false)
        {
            if (number < 0) { return ""; }
            string numStr = number.ToString();

            return Number2HanParseInt(numStr, fan);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int NullToZero(int? value)
        {
            return value.HasValue ? value.Value : 0;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static decimal NullToZero(decimal? value)
        {
            return value.HasValue ? value.Value : 0;
        }

        static Dictionary<char, int> RomanW = new Dictionary<char, int>()
        {
            { 'I', 1 },
            { 'V', 5 },
            { 'X', 10 },
            { 'L', 50 },
            { 'C', 100 },
            { 'D', 500 },
            { 'M', 1000 },
        };

        static Dictionary<char, int> RomanP = new Dictionary<char, int>()
        {
            { 'I', 0 },
            { 'V', 1 },
            { 'X', 1 },
            { 'L', 2 },
            { 'C', 2 },
            { 'D', 3 },
            { 'M', 3 },
        };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public int RomanToInt(string s)
        {
            int sum = 0;

            for (int i = 0; i < s.Length; i++)
            {
                if (i + 1 < s.Length && RomanP[s[i]] == RomanP[s[i + 1]] - 1)
                {
                    sum += RomanW[s[i + 1]] - RomanW[s[i]];
                    i++;
                }
                else
                {
                    sum += RomanW[s[i]];
                }
            }

            return sum;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dst"></param>
        /// <param name="src"></param>
        /// <returns></returns>
        public static decimal? NullAddTo(decimal? dst, decimal? src)
        {
            if (dst.HasValue)
            {
                if (src.HasValue)
                {
                    return dst.Value + src.Value;
                }

                return dst.Value;
            }
            else
            {
                if (src.HasValue)
                {
                    return src.Value;
                }

                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dst"></param>
        /// <param name="src"></param>
        /// <returns></returns>
        public static int? NullAddTo(int? dst, int? src)
        {
            if (dst.HasValue)
            {
                if (src.HasValue)
                {
                    return dst.Value + src.Value;
                }

                return dst.Value;
            }
            else
            {
                if (src.HasValue)
                {
                    return src.Value;
                }

                return null;
            }
        }

        /// <summary>
        /// 获取 计算环比 / 同比 百分比
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static double? GetRate(decimal? a, decimal? b)
        {
            if (a.HasValue && b.HasValue && b > 0)
            {
                return (double)(a.Value / b.Value - 1) * 100;
            }

            return null;
        }

        /// <summary>
        /// 获取 计算环比 / 同比 小数点
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static double? GetDoubleRate(decimal? a, decimal? b)
        {
            if (a.HasValue && b.HasValue && b > 0)
            {
                return (double)(a.Value / b.Value - 1);
            }

            return null;
        }

        /// <summary>
        /// 获取完成率 百分比
        /// </summary>
        /// <param name="a">当前</param>
        /// <param name="b">以前</param>
        /// <returns></returns>
        public static double? GetFinishRate(decimal? a, decimal? b)
        {
            if (!a.HasValue || !b.HasValue) { return null; }

            if (b > 0)
            {
                return (double)(a / b) * 100;
            }

            return null;
        }

        /// <summary>
        /// 获取完成率 小数点
        /// </summary>
        /// <param name="a">当前</param>
        /// <param name="b">以前</param>
        /// <returns></returns>
        public static double? GetDoubleFinishRate(decimal? a, decimal? b)
        {
            if (!a.HasValue || !b.HasValue) { return null; }

            if (b > 0)
            {
                return (double)(a / b);
            }

            return null;
        }

        /// <summary>
        /// 获取完成率 小数点
        /// </summary>
        /// <param name="a">当前</param>
        /// <param name="b">以前</param>
        /// <returns></returns>
        public static decimal GetDecimalFinishRate(decimal a, decimal b)
        {
            if (b > 0)
            {
                return (a / b);
            }

            return 0;
        }

        /// <summary>
        /// 获取计数
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static decimal? GetDivideCount(decimal? a, decimal? b)
        {
            if (b > 0)
            {
                return (a / b);
            }

            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ev"></param>
        /// <param name="p"></param>
        /// <param name="v"></param>
        /// <returns></returns>
        public static bool TryConvert(string ev, PropertyInfo p, out object v)
        {
            TypeConverter tc = TypeDescriptor.GetConverter(p.PropertyType);
            v = null;

            if (tc == null)
            {
                return false;
            }

            try
            {
                v = tc.ConvertFromInvariantString(ev);
                return true;
            }
            catch (Exception err)
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="v1"></param>
        /// <param name="v2"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        public static int TimeAnsciiCompare(DateTime v1, DateTime v2, string format)
        {
            return v1.ToString(format).CompareTo(v2.ToString(format));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class DateTimeExtend
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mDate"></param>
        /// <returns></returns>
        public static int GetQuarter(this DateTime mDate)
        {
            if (mDate.Month >= 1 && mDate.Month < 4)
            {
                return 1;
            }
            else if (mDate.Month >= 4 && mDate.Month < 7)
            {
                return 2;
            }
            else if (mDate.Month >= 7 && mDate.Month < 10)
            {
                return 3;
            }
            else
            {
                return 4;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="year"></param>
        /// <param name="quarter"></param>
        /// <returns></returns>
        public static DateTime GetStartTimeByQuarter(int year, int quarter)
        {
            if (quarter == 1)
            {
                return new DateTime(year, 01, 01);
            }
            else if (quarter == 2)
            {
                return new DateTime(year, 04, 01);
            }
            else if (quarter == 3)
            {
                return new DateTime(year, 07, 01);
            }
            else
            {
                return new DateTime(year, 010, 01);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mDate"></param>
        /// <returns></returns>
        public static DateTime GetPeriodStartTime(this DateTime mDate)
        {
            if (mDate.Day >= 1 && mDate.Day < 11)
            {
                return new DateTime(mDate.Year, mDate.Month, 1);
            }
            else if (mDate.Day >= 11 && mDate.Day < 21)
            {
                return new DateTime(mDate.Year, mDate.Month, 11);
            }
            else
            {
                return new DateTime(mDate.Year, mDate.Month, 21);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        public static string[] PeriodNames = new string[] { "上", "中", "下" };
        /// <summary>
        /// 
        /// </summary>
        public static string[] PeriodENames = new string[] { "F", "S", "T" };
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mDate"></param>
        /// <returns></returns>
        public static string GetPeriodName(this DateTime mDate)
        {
            int i = mDate.GetPeriod();
            if (i == -1) { return ""; }

            return PeriodNames[i - 1];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="period"></param>
        /// <returns></returns>
        public static string GetPeriodEName(int period)
        {
            return PeriodENames[period - 1];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mDate"></param>
        /// <returns></returns>
        public static int GetPeriod(this DateTime mDate)
        {
            if (mDate.Day >= 1 && mDate.Day < 11)
            {
                return 1;
            }
            else if (mDate.Day >= 11 && mDate.Day < 21)
            {
                return 2;
            }
            else if (mDate.Day >= 21)
            {
                return 3;
            }
            else
            {
                return -1;
            }
        }
    }
}