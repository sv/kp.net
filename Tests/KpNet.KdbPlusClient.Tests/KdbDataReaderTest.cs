using System;
using Kdbplus;
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

            Assert.AreEqual(1, Int32.Parse(reader["id"].ToString()));
            Assert.AreEqual("sasha", reader["name"].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse(reader["id"].ToString()));
            Assert.AreEqual("masha", reader["name"].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse((reader)["id"].ToString()));
            Assert.AreEqual("zina", (reader)["name"].ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetNumberIndexerTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse((reader)[0].ToString()));
            Assert.AreEqual("sasha", (reader)[1].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse((reader)[0].ToString()));
            Assert.AreEqual("masha", (reader)[1].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse((reader)[0].ToString()));
            Assert.AreEqual("zina", (reader)[1].ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetSpecificGetMethodsTest()
        {
            KdbPlusDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, reader.GetInt32(0), reader.GetInt64(0));
            Assert.AreEqual("sasha", reader.GetString(1));

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, reader.GetInt32(0), reader.GetInt64(0));
            Assert.AreEqual("masha", reader.GetString(1));

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, reader.GetInt32(0), reader.GetInt64(0));
            Assert.AreEqual("zina", reader.GetString(1));

            Assert.AreEqual(false, reader.Read());
        }
    }
}