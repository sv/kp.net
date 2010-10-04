using System;
using System.Data;
using System.Threading;
using NUnit.Framework;

namespace KpNet.KdbPlusClient.IntegrationTests.Simplified
{
    [TestFixture]
    public class KdbPlusDatabaseClientTests
    {
        
        [Test(Description = Constants.DescriptionMessage)]
        public void SuccessfulConnectTest()
        {
            using (CreateDatabaseClient())
            {
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void SuccessfulConnectViaConnectionStringTest()
        {
            using (CreateDatabaseClientFromConString(Constants.ConnectionString))
            {
            }
        }

        [Test]
        [ExpectedException(typeof(KdbPlusException))]
        public void FailedConnectTest()
        {
            using (CreateFakeDatabaseClient())
            {
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void GetCurrentTimeTest()
        {
            using (IDatabaseClient client  = CreateDatabaseClient())
            {
                Assert.IsInstanceOfType(typeof(TimeSpan), client.ExecuteScalar(".z.T"));
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void GetCurrentTypedTimeTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                Assert.IsInstanceOfType(typeof(TimeSpan), client.ExecuteScalar<TimeSpan>(".z.T"));
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void GetScalar0Test()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                Assert.IsInstanceOfType(typeof(int), client.ExecuteScalar<int>("0"));
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void GetScalar0AsyncTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                IAsyncResult result = client.BeginExecuteScalar("0", null, null, null);
                object received = client.EndExecuteScalar(result);

                Assert.IsInstanceOfType(typeof(int), received);
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteNonQueryTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTradeAndInsertRow(client);

                Assert.IsTrue(client.ExecuteQuery("select from trade").HasRows);
            }
        }

        

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteOneWayNonQueryTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTradeAndInsertRowOneWay(client);

                Thread.Sleep(1000);

                Assert.IsTrue(client.ExecuteQuery("select from trade").HasRows);
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteQueryTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTradeAndInsertRow(client);

                using(IDataReader reader = client.ExecuteQuery("select from trade"))
                {
                    CheckTrade(reader);
                }
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteQueryAsDataTableTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTradeAndInsertRow(client);

                DataTable table = client.ExecuteQueryAsDataTable("select from trade");
                CheckTrade(table);
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteNonQueryWithParamTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTrade(client);
                
                object[] x = { "AIG", 10.75, 200 };
                client.ExecuteNonQuery("insert", "trade", x);

                Assert.IsTrue(client.ExecuteQuery("select from trade").HasRows);
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteOneWayNonQueryWithParamTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTrade(client);

                object[] x = { "AIG", 10.75, 200 };
                client.ExecuteOneWayNonQuery("insert", "trade", x);

                Thread.Sleep(1000);

                Assert.IsTrue(client.ExecuteQuery("select from trade").HasRows);
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteQueryWithParamTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTrade(client);
                client.ExecuteNonQuery("f:{[table;symbol]select from table where sym=symbol}");

                InsertRow(client);

                using (IDataReader reader = client.ExecuteQuery("f","trade","AIG"))
                {
                    CheckTrade(reader);
                }
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteQueryWithParamMultipleResultRowsTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTrade(client);
                client.ExecuteNonQuery("f:{[table]select from table}");

                InsertRow(client);
                client.ExecuteNonQuery("`trade insert(`MSFT;11.0;300)");

                using (IDataReader reader = client.ExecuteQuery("f", "trade"))
                {
                    CheckTwoRows(reader);
                }
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteQueryWithParamMultipleResultsTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                CreateTrade(client);
                client.ExecuteNonQuery("f:{[table]d:`result1`result2!(); d[`result1]:select from table; d[`result2]:select from table; :d}");

                InsertRow(client);
                client.ExecuteNonQuery("`trade insert(`MSFT;11.0;300)");

                IMultipleResult result = client.ExecuteQueryWithMultipleResult("f", "trade");

                Assert.IsTrue(result.Count == 2);
                CheckTwoRows(result.GetResult(0));
                CheckTwoRows(result.GetResult(1));
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        [ExpectedException(typeof(KdbPlusException))]
        public void ExceptionAtScalarTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                client.ExecuteScalar("exception");
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        [ExpectedException(typeof(KdbPlusException))]
        public void ExceptionAtNonQueryTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                client.ExecuteNonQuery("exception");
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        [ExpectedException(typeof(KdbPlusException))]
        public void ExceptionAtQueryTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                client.ExecuteQuery("exception");
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        [ExpectedException(typeof(KdbPlusException))]
        public void ExceptionAtMultipleResultTest()
        {
            using (IDatabaseClient client = CreateDatabaseClient())
            {
                client.ExecuteQueryWithMultipleResult("exception");
            }
        }

        private static void CreateTradeAndInsertRow(IDatabaseClient client)
        {
            CreateTrade(client);
            InsertRow(client);
        }

        private static void CreateTrade(IDatabaseClient client)
        {
            client.ExecuteNonQuery("trade:([]sym:();price:();size:())");
        }

        private static void InsertRow(IDatabaseClient client)
        {
            client.ExecuteNonQuery("`trade insert(`AIG;10.75;200)");
        }

        private static void CreateTradeAndInsertRowOneWay(IDatabaseClient client)
        {
            CreateTrade(client);
            InsertRowOneWay(client);
        }

        private static void InsertRowOneWay(IDatabaseClient client)
        {
            client.ExecuteOneWayNonQuery("`trade insert(`AIG;10.75;200)");
        }

        private static void CheckTrade(IDataReader reader)
        {
            Assert.IsTrue(reader.Read());

            Assert.AreEqual("sym", reader.GetName(0));
            Assert.AreEqual("price", reader.GetName(1));
            Assert.AreEqual("size", reader.GetName(2));

            Assert.AreEqual("AIG", reader.GetString(0));
            Assert.AreEqual(10.75, reader.GetDouble(1));
            Assert.AreEqual(200, reader.GetInt32(2));
        }

        private static void CheckTrade(DataTable table)
        {
            Assert.AreEqual(3, table.Columns.Count);
            Assert.AreEqual(1, table.Rows.Count);

            Assert.AreEqual(typeof(string), table.Columns[0].DataType);
            Assert.AreEqual(typeof(Double), table.Columns[1].DataType);
            Assert.AreEqual(typeof(int), table.Columns[2].DataType);

            Assert.AreEqual("sym", table.Columns[0].ColumnName);
            Assert.AreEqual("price", table.Columns[1].ColumnName);
            Assert.AreEqual("size", table.Columns[2].ColumnName);

            Assert.AreEqual("AIG", table.Rows[0][0]);
            Assert.AreEqual(10.75, table.Rows[0][1]);
            Assert.AreEqual(200, table.Rows[0][2]);
        }

        private static void CheckTwoRows(IDataReader reader)
        {
            CheckTrade(reader);

            Assert.IsTrue(reader.Read());

            Assert.AreEqual("sym", reader.GetName(0));
            Assert.AreEqual("price", reader.GetName(1));
            Assert.AreEqual("size", reader.GetName(2));

            Assert.AreEqual("MSFT", reader.GetString(0));
            Assert.AreEqual(11, reader.GetDouble(1));
            Assert.AreEqual(300, reader.GetInt32(2));

            Assert.IsFalse(reader.Read());
        }

        protected IDatabaseClient CreateDatabaseClient()
        {
            return CreateDatabaseClient(Constants.Host, Constants.Port);
        }

        protected virtual IDatabaseClient CreateDatabaseClientFromConString(string connectionString)
        {
            return new NonPooledKdbPlusDatabaseClient(connectionString);
        }

        protected IDatabaseClient CreateFakeDatabaseClient()
        {
            return CreateDatabaseClient("fakehost", 1);
        }

        protected virtual IDatabaseClient CreateDatabaseClient(string host, int port)
        {
            return new NonPooledKdbPlusDatabaseClient(host, port);
        }
    }
}
