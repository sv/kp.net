

using System;
using System.Data;
using KpNet.KdbPlusClient;

namespace Example
{
    class Program
    {
        static void Main()
        {
            //1. download trial kdb+ here http://kx.com/Developers/software.php
            //2. start q process on 1001 port by running  'q -p 1001'
                      
            try
            {
                using (IDatabaseConnection connection = new KdbDatabaseConnection("localhost", 1001))
                {
                    // get current time
                    TimeSpan time = connection.ExecuteScalar<TimeSpan>(".z.T");
                    Console.WriteLine("Current time {0}",time);

                    // create trade table
                    connection.ExecuteNonQuery("trade:([]time:();sym:();price:();size:())");

                    // get list of tables in db
                    // should output 'trade'
                    IDataReader reader = connection.ExecuteQuery("\\a");
                    Console.WriteLine("Tables in database:");

                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
