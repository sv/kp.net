

using System;
using System.Data;
using System.Data.Common;
using KpNet.KdbPlusClient;

namespace Example
{
    class Program
    {
        static void Main()
        {
            //1. download trial kdb+ here http://kx.com/Developers/software.php
            //2. start q process on 1001 port by running  'q -p 1001'
                
            // Simplified API
            try
            {
                using (IDatabaseClient client = new KdbPlusDatabaseClient("localhost", 1001))
                {
                    // get current time
                    TimeSpan time = client.ExecuteScalar<TimeSpan>(".z.T");
                    Console.WriteLine("Current time {0}",time);

                    // create trade table
                    client.ExecuteNonQuery("trade:([]time:();sym:();price:();size:())");

                    // get list of tables in db
                    // should output 'trade'
                    IDataReader reader = client.ExecuteQuery("\\a");
                    Console.WriteLine("Tables in database:");

                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
            }
            catch (KdbPlusException ex)
            {
                Console.WriteLine(ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            // ADO.Net provider
            try
            {
                using (DbConnection connection = new KdbPlusConnection("server=localhost;port=1001;user id=me;password=my;buffersize=16384"))
                {
                    // open connection

                    connection.Open();

                    DbCommand command = connection.CreateCommand();

                    // get current time
                    command.CommandText = ".z.T";
                    TimeSpan time = (TimeSpan)command.ExecuteScalar();
                    Console.WriteLine("Current time {0}", time);

                    // create trade table
                    command.CommandText = "trade:([]time:();sym:();price:();size:())";
                    command.ExecuteNonQuery();


                    // get list of tables in db
                    // should output 'trade'
                    command.CommandText = "\\a";
                    IDataReader reader = command.ExecuteReader();
                    Console.WriteLine("Tables in database:");

                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(0));
                    }
                }
            }
            catch (KdbPlusException ex)
            {
                Console.WriteLine(ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
