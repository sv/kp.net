using System;
using System.Data;
using NUnit.Framework;
using Flip = Kdbplus.c.Flip;
using Dict = Kdbplus.c.Dict;

namespace KpNet.KdbPlusClient.Tests
{
    [TestFixture]
    public sealed class KdbDataReaderTest
    {
        private static readonly Flip _result = new Flip(new Dict(new string[] { "id", "name" }, new object[] { new int[] { 1, 2, 3 }, new string[] { "sasha", "masha", "zina" } }));

        [Test]
        public void ColumnAndRowCountTest()
        {
            KdbDataReader reader = GetReader();
            Assert.AreEqual(2, reader.FieldCount);

            int rows = 0;

            while (reader.Read())
                rows++;

            Assert.AreEqual(3, rows);
        }

        [Test]
        public void ColumnNamesTest()
        {
            KdbDataReader reader = GetReader();
            Assert.AreEqual("id", reader.GetName(0));
            Assert.AreEqual("name", reader.GetName(1));
        }

        [Test]
        public void RowsetGetValueTest()
        {
            KdbDataReader reader = GetReader();

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
        public void RowsetNameIndexerTest()
        {
            KdbDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse(((IDataRecord)reader)["id"].ToString()));
            Assert.AreEqual("sasha", ((IDataRecord)reader)["name"].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse(((IDataRecord)reader)["id"].ToString()));
            Assert.AreEqual("masha", ((IDataRecord)reader)["name"].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse(((IDataRecord)reader)["id"].ToString()));
            Assert.AreEqual("zina", ((IDataRecord)reader)["name"].ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetNumberIndexerTest()
        {
            KdbDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse(((IDataRecord)reader)[0].ToString()));
            Assert.AreEqual("sasha", ((IDataRecord)reader)[1].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse(((IDataRecord)reader)[0].ToString()));
            Assert.AreEqual("masha", ((IDataRecord)reader)[1].ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse(((IDataRecord)reader)[0].ToString()));
            Assert.AreEqual("zina", ((IDataRecord)reader)[1].ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void RowsetSpecificGetMethodsTest()
        {
            KdbDataReader reader = GetReader();

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

        [Test]
        public void RowsetGetValuesTest()
        {
            KdbDataReader reader = GetReader();

            Assert.AreEqual(true, reader.Read());

            object[] values = new object[2];

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
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CannotReadAfterEndTest()
        {
            KdbDataReader reader = GetReader();
            while (reader.Read());

            reader.GetInt32(0);
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CannotReadAfterDisposeTest()
        {
            KdbDataReader reader = GetReader();
            reader.Dispose();

            reader.GetInt32(0);
        }

        private static KdbDataReader GetReader()
        {
            return new KdbDataReader(_result);
        }
    }
}