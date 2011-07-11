using System;
using System.Data;
using System.Data.Common;
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
            KdbPlusProcess process = KdbPlusProcess.Builder.UseShellExecute().LimitNumberOfCoresTo(1).StartNew();            

            try
            {
                // Simplified API
                RunSimplifiedAPIExample();

                // Implicit connection pooling
                RunSimplifiedConnectionPoolingExample();

                // ADO.Net provider
                RunADONetExample();
            }
            finally
            {
                if(process.IsAlive)
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
