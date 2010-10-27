using kx;
using NUnit.Framework;
using System.Data;
using System;

namespace KpNet.KdbPlusClient.Tests
{
    [TestFixture]
    public sealed class KdbMultipleResultTest
    {   
        private KdbMultipleResult _result;

        [SetUp]
        public void SetUp()
        {
            _result = CreateMultipleResult();
        }

        [Test]
        public void TableCountTest()
        {
            Assert.AreEqual(_result.Count, 2);
        }

        [Test]
        [ExpectedException(typeof(ApplicationException), "Cannot find reader 'table3'.")]
        public void GetReaderForNonexistentTableTest()
        {
            IDataReader reader = _result.Results["table3"];            
        }

        [Test]
        [ExpectedException(typeof(ApplicationException))]
        public void GetReaderForNonexistentIndexTest()
        {
            IDataReader reader = _result.GetResult(3);
        }

        [Test]
        public void GetValueFromTheFirstResultTest()
        {
            IDataReader reader = _result.Results["table1"];

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("A1", reader.GetValue(1).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("A2", reader.GetValue(1).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("A3", reader.GetValue(1).ToString());

            Assert.AreEqual(false, reader.Read());
        }

        [Test]
        public void GetValueFromTheSecondResultTest()
        {
            IDataReader reader = _result.GetResult(1);

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(1, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("B1", reader.GetValue(1).ToString());            
            Assert.AreEqual("C1", reader.GetValue(2).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(2, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("B2", reader.GetValue(1).ToString());
            Assert.AreEqual("C2", reader.GetValue(2).ToString());

            Assert.AreEqual(true, reader.Read());

            Assert.AreEqual(3, Int32.Parse(reader.GetValue(0).ToString()));
            Assert.AreEqual("B3", reader.GetValue(1).ToString());
            Assert.AreEqual("C3", reader.GetValue(2).ToString());

            Assert.AreEqual(false, reader.Read());
        }

        private static KdbMultipleResult CreateMultipleResult()
        {
            c.Dict table1 = CreateTable(new string[] { "id1" },                             // Key column names. 
                                        new object[] { new object[] { 1, 2, 3 } },          // Key column values.
                                        new string[] { "columnA" },                         // Column names.  
                                        new object[] { new object[] { "A1", "A2", "A3" } });// Column calues.  
            c.Dict table2 = CreateTable(new string[] { "id1" },
                                        new object[] { new object[] { 1, 2, 3 } },
                                        new string[] { "columnB", "columnC" },
                                        new object[] { new object[] { "B1", "B2", "B3" }, new object[] { "C1", "C2", "C3" } });
            return new KdbMultipleResult(new c.Dict(new string[] { "table1", "table2" }, new c.Dict[] { table1, table2 }));
        }

        private static c.Dict CreateTable(object keyColumnNames, object keyColumnValues, object columnNames, object columnValues)
        {
            c.Flip keyColumnFlip = new c.Flip(new c.Dict(keyColumnNames, keyColumnValues));
            c.Flip columnFlip = new c.Flip(new c.Dict(columnNames, columnValues));
            return new c.Dict(keyColumnFlip, columnFlip);
        }
    }
}
