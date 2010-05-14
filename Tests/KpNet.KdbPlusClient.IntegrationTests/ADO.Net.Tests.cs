using System;
using System.Data;
using System.Data.Common;
using NUnit.Framework;

namespace KpNet.KdbPlusClient.IntegrationTests
{
    [TestFixture]
    public sealed class ADONetTests
    {
        [Test]
        public void SuccessfulConnectTest()
        {
            DbConnection connection;

            using (connection = CreateConnection())
            {
                Assert.AreEqual(ConnectionState.Closed, connection.State);
                connection.Open();
                Assert.AreEqual(ConnectionState.Open, connection.State);
            }

            Assert.AreEqual(ConnectionState.Closed, connection.State);
        }

        [Test]
        [ExpectedException(typeof(KdbPlusException))]
        public void FailedConnectTest()
        {
            using (DbConnection connection = new KdbPlusConnection("server=localhost;port=11"))
            {
                connection.Open();
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void GetCurrentTimeTest()
        {
            using (DbConnection connection = CreateConnection())
            {
                connection.Open();

                DbCommand command = connection.CreateCommand();
                command.CommandText = ".z.T";

                Assert.IsInstanceOfType(typeof(TimeSpan), command.ExecuteScalar());
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteNonQueryTest()
        {
            using (DbConnection connection = CreateConnection())
            {
                connection.Open();

                CreateTradeAndInsertRow(connection);
                DbCommand command = connection.CreateCommand();
                command.CommandText = "select from trade";

                Assert.IsTrue(command.ExecuteReader().HasRows);
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteNonQueryWitParamTest()
        {
            using (DbConnection connection = CreateConnection())
            {
                connection.Open();

                CreateTrade(connection);

                DbCommand command = connection.CreateCommand();

                object[] x = { "AIG", 10.75, 200 };
                command.CommandText = "insert";

                DbParameter table = command.CreateParameter();
                table.Value = "trade";
                command.Parameters.Add(table);

                DbParameter row = command.CreateParameter();
                row.Value = x;
                command.Parameters.Add(row);
                command.ExecuteNonQuery();

                command.CommandText = "select from trade where sym=@sym";
                command.Parameters.Clear();
                DbParameter sym = command.CreateParameter();
                sym.ParameterName = "@sym";
                sym.Value = "AIG";
                command.Parameters.Add(sym);

                CheckTrade(command.ExecuteReader());
            }
        }

        [Test(Description = Constants.DescriptionMessage)]
        public void ExecuteQueryTest()
        {
            using (DbConnection connection = CreateConnection())
            {
                connection.Open();

                CreateTradeAndInsertRow(connection);
                DbCommand command = connection.CreateCommand();
                command.CommandText = "select from trade";

                CheckTrade(command.ExecuteReader());
            }
        }

        private static void CreateTradeAndInsertRow(DbConnection connection)
        {
            CreateTrade(connection);
            InsertRow(connection);
        }

        private static void CreateTrade(DbConnection connection)
        {
            DbCommand command = connection.CreateCommand();
            command.CommandText = "trade:([]sym:();price:();size:())";
            command.ExecuteNonQuery();
        }

        private static void InsertRow(DbConnection connection)
        {
            DbCommand command = connection.CreateCommand();
            command.CommandText = "`trade insert(`AIG;10.75;200)";
            command.ExecuteNonQuery();
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

        private static KdbPlusConnection CreateConnection()
        {
            return new KdbPlusConnection(Constants.ConnectionStringNoPooling);
        }
    }
}
