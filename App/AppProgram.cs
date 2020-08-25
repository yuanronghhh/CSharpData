using System;
using App.Test;

namespace App
{
    public class AppProgram
    {
        private static double Rad(double d)
        {
            return (double)d * Math.PI / 180d;
        }

        public static double GetDistance(double lng1, double lat1, double lng2, double lat2)
        {
            const double EARTH_RADIUS = 6378137;

            double radLat1 = Rad(lat1);
            double radLng1 = Rad(lng1);
            double radLat2 = Rad(lat2);
            double radLng2 = Rad(lng2);
            double a = radLat1 - radLat2;
            double b = radLng1 - radLng2;
            double result = 2 * Math.Asin(Math.Sqrt(Math.Pow(Math.Sin(a / 2), 2) + Math.Cos(radLat1) * Math.Cos(radLat2) * Math.Pow(Math.Sin(b / 2), 2))) * EARTH_RADIUS;
            return result;
        }

        static void Main(string[] args)
        {
            TestCase.Run();

            Console.WriteLine("[Done]");
            Console.ReadKey();
        }
    }
}
