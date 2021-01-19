using System;

namespace CommonLib.Utils
{
    public static class CliTools
    {
        public static bool ArgEqual(string arg, string para)
        {
            return (arg == ("-" + para)) || (arg == ("--" + para));
        }

        public static bool GetArgValue(string[] args, string[] flags, bool isFlag, ref string outPutString)
        {
            int i;
            string arg;

            for (i = 0; i < args.Length; i++)
            {
                arg = args[i];

                if(!Array.TrueForAll(flags, flag => !ArgEqual(arg, flag)))
                {
                    continue;
                }

                if (isFlag)
                {
                    return true;
                }

                if (i + 1 >= args.Length)
                {
                    return false;
                }

                if (args[i + 1].StartsWith("-"))
                {
                    // Parameter value should not starts with '-'
                    return false;
                }

                outPutString = args[i + 1];
                return true;
            }

            return false;
        }

        public static bool GetArgValue(string[] args, string flag, bool isFlag, ref string outPutString)
        {
            int i;
            string arg;

            for (i = 0; i < args.Length; i++)
            {
                arg = args[i];

                if(!ArgEqual(arg, flag))
                {
                    continue;
                }

                if(isFlag)
                {
                    return true;
                }

                if (i + 1 >= args.Length)
                {
                    return false;
                }

                if (args[i + 1].StartsWith("-")) {
                    // Parameter value should not starts with '-'
                    return false;
                }

                outPutString = args[i + 1];
                return true;
            }

            return false;
        }
    }
}
