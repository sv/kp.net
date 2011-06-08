using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using KpNet.Hosting;
using KpNet.KdbPlusClient;


namespace Example
{
    class Program
    {
        static void Main()
        {
            //1. download trial kdb+ here http://kx.com/Developers/software.php
            // there should be path to q process in the path env variable


            try
            {
                using(IDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNonPooledClient(new KdbPlusConnectionStringBuilder(){Server = "localhost",Port = 1003}))
                {
                    client.ExecuteScalar("0");
                    Console.WriteLine("Successful ping 1003");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            //Console.WriteLine("Shellexecute");

            //for (int iteration = 0; iteration < 100; iteration++)
            //{
            //    try
            //    {
            //        Console.WriteLine("Iteration {0}", iteration);
            //        KdbPlusProcess process = KdbPlusProcess.Builder.
            //            SetPort(1002)
            //            .SetWorkingDirectory(@"d:\kdbl\scripts").EnableMultiThreading()
            //            //.AddSetupCommand(SetupCommand)
            //            .UseShellExecute()
            //            .StartNew();

            //        Query(process);
            //    }
            //    catch (Exception ex)
            //    {
            //        Console.WriteLine(ex);
            //    }
                    
            //}
            
            

            
            //    Console.WriteLine("Without Shellexecute");
            //    for (int iteration = 0; iteration < 100; iteration++)
            //    {
            //        try
            //        {
            //            Console.WriteLine("Iteration {0}", iteration);
            //            KdbPlusProcess process = KdbPlusProcess.Builder
            //                .SetPort(1003)
            //                .SetWorkingDirectory(@"d:\kdbl\scripts").EnableMultiThreading()
            //                //.AddSetupCommand(SetupCommand)
            //                .HideWindow().StartNew();

            //            Query(process);

            //        }
            //        catch (Exception ex)
            //        {
            //            Console.WriteLine(ex);
            //        }
            //    }
            

            //Console.ReadLine();

            //try
            //{
            //    // Simplified API
            //    RunSimplifiedAPIExample();

            //    // Implicit connection pooling
            //    RunSimplifiedConnectionPoolingExample();

            //    // ADO.Net provider
            //    RunADONetExample();
            //}
            //finally
            //{
            //    if(process.IsAlive)
            //        process.Kill();
            //}
        }

        private static void SetupCommand(IDatabaseClient client)
        {
            using (client)
            {
                client.ReceiveTimeout = TimeSpan.FromHours(1);

                // load script with functions into kdb+
                const string preloadScript = "\\l startup_kiwi.q";
                Console.WriteLine("Executing: {0}.", preloadScript);

                client.ExecuteNonQuery(preloadScript);

                // call init function to load db and all scripts
                string command = String.Format(CultureInfo.InvariantCulture,
                     ".kiwi.init[`{0};`{1};{2}]",
                    CorrectPath(@"D:\kdbl\db_a"), // db location
                    CorrectPath(@"d:\kdbl\scripts"), // path to scripts
                    1, // preload consensus into memory
                    1, // load pva script
                    0); // load ibes scripts

                Console.WriteLine("Executing: {0}.", command);

                client.ExecuteNonQuery(command);

                // change port to negative to start multithreading
                string mtCommand = String.Format(CultureInfo.InvariantCulture, @"\p -{0}", client.Port);
                Console.WriteLine("Executing: {0}.", mtCommand);
                client.ExecuteNonQuery(mtCommand);
            }
        }

        // correct path for kdb+ process - replace \ to /
        private static string CorrectPath(string path)
        {
            const string invalidSeparator = "\\";
            const string validSeparator = "/";

            path = path.Trim().Replace(invalidSeparator, validSeparator);
            if (path.EndsWith(validSeparator, StringComparison.OrdinalIgnoreCase))
                path = path.Substring(0, path.Length - 1);

            return path;
        }

        private static void Query(KdbPlusProcess process)
        {
            try
            {
                for (int i = 0; i < 10; i++)
                {
                    using (IDatabaseClient client = process.GetConnection())
                    {
                        client.SendTimeout = TimeSpan.FromSeconds(10);
                        client.ReceiveTimeout = TimeSpan.FromMinutes(1);
                        client.ExecuteScalar("while[1; ]");
                    }
                }
            }
            finally
            {
                if (process.IsAlive)
                    process.Kill();
            }
        }

        private static void RunSimplifiedConnectionPoolingExample()
        {
            try
            {
                for (int i = 0; i < 10; i++)
                {
                    //Connections are pooled since pooling is not disabled explicitly.
                    using (IDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNewClient("server=localhost;port=1001;"))
                    {
                        // get current time
                        TimeSpan time = client.ExecuteScalar<TimeSpan>(".z.T");
                        Console.WriteLine("Current time {0}", time);
                    }

                    // Connections are recreated since pooling is disabled explicitly.
                    using (IDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNewClient("server=localhost;port=1001;Pooling=false;"))
                    {
                        // get current time
                        TimeSpan time = client.ExecuteScalar<TimeSpan>(".z.T");
                        Console.WriteLine("Current time {0}", time);
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

        private static void RunADONetExample()
        {
            try
            {
                using (DbConnection connection = new KdbPlusConnection("server=localhost;port=1001;user id=me;password=my;buffer size=16384"))
                {
                    // open connection - connection pooling is enabled
                    // to disable pooling - use Pooling=false in connection string.
                    connection.Open();

                    DbCommand command = connection.CreateCommand();

                    // get current time
                    command.CommandText = ".z.T";
                    TimeSpan time = (TimeSpan)command.ExecuteScalar();
                    Console.WriteLine("Current time {0}", time);

                    // create trade table
                    command.CommandText = "trade:([]sym:();price:();size:())";
                    command.ExecuteNonQuery();

                    command.CommandText = "`trade insert(`AIG;10.75;200)";
                    command.ExecuteNonQuery();

                    object[] x = { "xx", 93.5, 300 };
                    command.CommandText = "insert";

                    DbParameter table = command.CreateParameter();
                    table.Value = "trade";
                    command.Parameters.Add(table);

                    DbParameter row = command.CreateParameter();
                    row.Value = x;
                    command.Parameters.Add(row);
                    command.ExecuteNonQuery();

                    command.ExecuteNonQuery();

                    command.CommandText = "select from trade where sym=@sym";
                    command.Parameters.Clear();
                    DbParameter sym = command.CreateParameter();
                    sym.ParameterName = "@sym";
                    sym.Value = "AIG";
                    command.Parameters.Add(sym);

                    // get data from trade
                    using (IDataReader reader = command.ExecuteReader())
                    {
                        DataTable dtable = new DataTable();
                        dtable.Load(reader);

                        Console.WriteLine("Trade:");

                        Console.WriteLine("{0}\t{1}\t{2}", dtable.Columns[0].ColumnName, dtable.Columns[1].ColumnName, dtable.Columns[2].ColumnName);

                        for (int i = 0; i < dtable.Rows.Count; i++)
                        {
                            Console.WriteLine("{0}\t{1}\t{2}", dtable.Rows[i][0], dtable.Rows[i][1], dtable.Rows[i][2]);
                        }
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

        private static void RunSimplifiedAPIExample()
        {
            try
            {
                using (IDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNonPooledClient("server=localhost;port=1001"))
                {
                    // get current time
                    TimeSpan time = client.ExecuteScalar<TimeSpan>(".z.T");
                    Console.WriteLine("Current time {0}",time);

                    // create trade table
                    client.ExecuteNonQuery("trade:([]sym:();price:();size:())");
                    client.ExecuteNonQuery("`trade insert(`AIG;10.75;200)");
                    client.ExecuteOneWayNonQuery("`trade insert(`AIG1;10.75;200)");
                    
                    // parameters handling example
                    object[] x = { "xx", 93.5, 300 };
                    client.ExecuteNonQuery("insert", "trade", x);

                    // one-way call example
                    object[] y = { "yy", 93.5, 300 };
                    client.ExecuteOneWayNonQuery("insert", "trade", y);

                    // get data from trade
                    IDataReader reader = client.ExecuteQuery("select from trade");
                    Console.WriteLine("Trade:");

                    Console.WriteLine("{0}\t{1}\t{2}", reader.GetName(0), reader.GetName(1), reader.GetName(2));
                    while (reader.Read())
                    {
                        Console.WriteLine("{0}\t{1}\t{2}", reader.GetString(0), reader.GetDouble(1), reader.GetInt32(2));
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
