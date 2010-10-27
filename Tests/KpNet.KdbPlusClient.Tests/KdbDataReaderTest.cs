using System;
using System.Data;
using kx;
using NUnit.Framework;

namespace KpNet.KdbPlusClient.Tests
{
    [TestFixture]
    public sealed class KdbDataReaderTest
    {
        private static readonly c.Flip _result =
            new c.Flip(new c.Dict(new[] {"id", "name"}, new object[] {new[] {1, 2, 3}, new[] {"sasha", "masha", "zina"}}));

        private static KdbPlusDataReader GetReader()
        {
            return new KdbPlusDataReader(_result);
        }

        [Test]
        [ExpectedException(typeof (ObjectDisposedException))]
        public void CannotReadAfterDisposeTest()
        {
            KdbPlusDataReader reader = GetReader();
            reader.Dispose();

            reader.GetInt32(0);
        }

        [Test]
        [ExpectedException(typeof (ObjectDisposedException))]
        public void CannotReadAfterEndTest()
        {
            KdbPlusDataReader reader = GetReader();
            while (reader.Read()) ;

            reader.GetInt32(0);
        }

        [Test]
        public void ColumnAndRowCountTest()
        {
            KdbPlusDataReader reader = GetReader();
            Assert.AreEqual(2, reader.FieldCount);

            int rows = 0;

            while (reader.Read())
                rows++;

            Assert.AreEqual(3, rows);
        }

        [Test]
        public void ColumnTypeTest()
        {
            KdbPlusDataReader reader = GetReader();
            Assert.AreEqual(typeof(int), reader.GetFieldType(0));
            Assert.AreEqual(typeof(string), reader.GetFieldType(1));
        }

        [Test]
        public void ColumnTypeNameTest()
        {
            KdbPlusDataReader reader = GetReader();
            Assert.AreEqual("Int32", reader.GetDataTypeName(0));
            Assert.AreEqual("String", reader.GetDataTypeName(1));
        }

        [Test]
        public void ColumnNamesTest()
        {
            KdbPlusDataReader reader = GetReader();
            Assert.AreEqual("id", reader.GetName(0));
            Assert.AreEqual("name", reader.GetName(1));
        }

        [Test]
        public void RowsetGetValueTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("sasha", reader.GetValue(1).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("masha", reader.GetValue(1).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("zina", reader.GetValue(1).ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void DataTableGetValueTest()
        {
            using(KdbPlusDataReader reader = GetReader())
            {
                DataTable table = new DataTable();
                table.Load(reader);

                Assert.AreEqual(2, table.Columns.Count);
                Assert.AreEqual(3, table.Rows.Count);

                Assert.AreEqual("id", table.Columns[0].ColumnName);
                Assert.AreEqual("name", table.Columns[1].ColumnName);

                Assert.AreEqual(typeof(int), table.Columns[0].DataType);
                Assert.AreEqual(typeof(string), table.Columns[1].DataType);

                Assert.AreEqual(1, table.Rows[0][0]);
                Assert.AreEqual("sasha", table.Rows[0][1]);

                Assert.AreEqual(2, table.Rows[1][0]);
                Assert.AreEqual("masha", table.Rows[1][1]);

                Assert.AreEqual(3, table.Rows[2][0]);
                Assert.AreEqual("zina", table.Rows[2][1]);
                
            }
        }

        [Test]
        public void RowsetGetValuesTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            var values = new object[2];

            reader.GetValues(values);

            Assert.AreEqual(1, Int32.Parse(values[0].ToString()));
            Assert.AreEqual("sasha", values[1].ToString());

            Assert.AreEqual(true, reader.Read());
            reader.GetValues(values);

            Assert.AreEqual(2, Int32.Parse(values[0].ToString()));
            Assert.AreEqual("masha", values[1].ToString());

            Assert.AreEqual(true, reader.Read());
            reader.GetValues(values);

            Assert.AreEqual(3, Int32.Parse(values[0].ToString()));
            Assert.AreEqual("zina", values[1].ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetNameIndexerTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, reader["id"]);
            Assert.AreEqual("sasha", reader["name"]);

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, reader["id"]);
            Assert.AreEqual("masha", reader["name"]);

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, reader["id"]);
            Assert.AreEqual("zina", reader["name"]);

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetNumberIndexerTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, reader[0]);
            Assert.AreEqual("sasha", reader[1]);

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, reader[0]);
            Assert.AreEqual("masha", reader[1]);

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, reader[0]);
            Assert.AreEqual("zina", reader[1]);

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetSpecificGetMethodsTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, reader.GetInt32(0));
            Assert.AreEqual("sasha", reader.GetString(1));

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, reader.GetInt32(0));
            Assert.AreEqual("masha", reader.GetString(1));

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, reader.GetInt32(0));
            Assert.AreEqual("zina", reader.GetString(1));

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void NullsAreReadAsDbNullsTest()
        {
            c.Flip result =
            new c.Flip(new c.Dict(new[] { "id", "name" }, new object[] { new[] { 1, (int)c.NULL(typeof(int))}, new[] { "sasha", String.Empty} }));

            KdbPlusDataReader reader = new KdbPlusDataReader(result);

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("sasha", reader.GetValue(1).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(DBNull.Value, reader.GetValue(0));
            Assert.AreEqual(DBNull.Value, reader.GetValue(1));
            
            Assert.AreEqual(false, reader.Read());
        }
    }
}