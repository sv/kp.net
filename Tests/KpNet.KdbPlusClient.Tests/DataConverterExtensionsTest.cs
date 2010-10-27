using System;
using System.Data;
using kx;
using NUnit.Framework;

namespace KpNet.KdbPlusClient.Tests
{
    [TestFixture]
    public sealed class DataConverterExtensionsTest
    {
        [Test]
        public void ConvertSimpleDictToDataTableTest()
        {
            DateTime firstDate = DateTime.Now;
            DateTime secondDate = firstDate.AddDays(1);
            const string tableName = "MyTable";

            c.Dict dict = CreateDict(firstDate, secondDate);
            DataTable dt = dict.ToDataTable(tableName);

            CheckDataTable(firstDate, secondDate, dt, tableName);
        }

        [Test]
        public void ConvertSimpleFlipToDataTableTest()
        {
            DateTime firstDate = DateTime.Now;
            DateTime secondDate = firstDate.AddDays(1);
            const string tableName = "MyTable";

            c.Dict dict = CreateDict(firstDate, secondDate);
            c.Flip flip = new c.Flip(dict);
            DataTable dt = flip.ToDataTable(tableName);

            CheckDataTable(firstDate, secondDate, dt, tableName);
        }

        [Test]
        public void ConvertEmptyDataTableToDictTest()
        {
            DataTable dt = new DataTable();

            c.Dict dict = dt.ToDict();
            string[] columnNames = (string[])dict.x;
            object[] columnValues = (object[])dict.y;

            Assert.AreEqual(0, columnNames.Length);
            Assert.AreEqual(0, columnValues.Length);
        }

        [Test]
        public void ConvertEmptyDataTableToFlipTest()
        {
            DataTable dt = new DataTable();

            c.Flip dict = dt.ToFlip();
            string[] columnNames = dict.x;
            object[] columnValues = dict.y;

            Assert.AreEqual(0, columnNames.Length);
            Assert.AreEqual(0, columnValues.Length);
        }

        [Test]
        public void ConvertSimpleDataTableToDict()
        {
            DateTime now = DateTime.Now;
            DateTime secondDate = now.AddDays(1);
            const string tableName = "MyTable";
            DataTable dt = CreateTable(tableName, now, secondDate);

            c.Dict dict = dt.ToDict();

            DataTable tableToCheck = dict.ToDataTable(tableName);
            CheckDataTable(now, secondDate, tableToCheck, tableName);

        }

        [Test]
        public void ConvertSimpleDataTableToFlip()
        {
            DateTime now = DateTime.Now;
            DateTime secondDate = now.AddDays(1);
            const string tableName = "MyTable";
            DataTable dt = CreateTable(tableName, now, secondDate);

            c.Flip flip = dt.ToFlip();

            DataTable tableToCheck = flip.ToDataTable(tableName);
            CheckDataTable(now, secondDate, tableToCheck, tableName);

        }

        [Test]
        public void ConvertDataSetToDict()
        {
            DateTime now = DateTime.Now;
            DateTime secondDate = now.AddDays(1);
            const string tableName = "MyTable";
            DataTable dt1 = CreateTable(tableName+"1", now, secondDate);
            DataTable dt2 = CreateTable(tableName + "2", now, secondDate);
            DataSet ds = new DataSet();
            ds.Tables.Add(dt1);
            ds.Tables.Add(dt2);

            c.Dict dict = ds.ToDict();
            string[] columnNames = (string[]) dict.x;
            object[] columnValues = (object[]) dict.y;
            
            Assert.AreEqual(2, columnNames.Length);
            Assert.AreEqual(2, columnValues.Length);

            Assert.AreEqual(tableName + "1", columnNames[0]);
            Assert.AreEqual(tableName + "2", columnNames[1]);

            c.Dict d1 = ((c.Dict[]) columnValues[0])[0];
            DataTable tableToCheck1 = d1.ToDataTable(tableName);
            CheckDataTable(now, secondDate, tableToCheck1, tableName);

            c.Dict d2 = ((c.Dict[])columnValues[1])[0];
            DataTable tableToCheck2 = d2.ToDataTable(tableName);
            CheckDataTable(now, secondDate, tableToCheck2, tableName);
        }

        [Test]
        public void ConvertDictToDataSet()
        {
            DateTime firstDate = DateTime.Now;
            DateTime secondDate = firstDate.AddDays(1);
            const string tableName = "MyTable";

            c.Dict dict1 = CreateDict(firstDate, secondDate);
            c.Dict dict2 = CreateDict(firstDate, secondDate);
            c.Dict composite = new c.Dict(new string[] { tableName + "1", tableName + "2" }, new object[] { new c.Dict[] { dict1 }, new c.Dict[] { dict2 } });

            DataSet ds = composite.ToDataSet();
            Assert.AreEqual(2,ds.Tables.Count);

            DataTable dt1 = ds.Tables[0];
            CheckDataTable(firstDate, secondDate, dt1, tableName + "1");

            DataTable dt2 = ds.Tables[1];
            CheckDataTable(firstDate, secondDate, dt2, tableName + "2");

        }

        private static DataTable CreateTable(string tableName, DateTime now, DateTime secondDate)
        {
            DataTable dt = new DataTable(tableName);
            dt.Columns.Add("id", typeof (int));
            dt.Columns[0].AllowDBNull = true;

            dt.Columns.Add("name", typeof(string));
            dt.Columns.Add("date", typeof (DateTime));


            DataRow row = dt.NewRow();
            row[0] = 1;
            row[1] = "Alex";
            row[2] = now;

            dt.Rows.Add(row);

            DataRow row1 = dt.NewRow();
            row1[0] = 2;
            row1[1] = "K+";
            row1[2] = secondDate;

            dt.Rows.Add(row1);

            DataRow row2 = dt.NewRow();
            row2[0] = DBNull.Value;
            row2[1] = DBNull.Value;
            row2[2] = DBNull.Value;

            dt.Rows.Add(row2);
            return dt;
        }

        private static void CheckDataTable(DateTime firstDate, DateTime secondDate, DataTable dt, string tableName)
        {
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(3, dt.Columns.Count);
            Assert.AreEqual(tableName, dt.TableName);

            Assert.AreEqual(1, dt.Rows[0][0]);
            Assert.AreEqual(2, dt.Rows[1][0]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2][0]);

            Assert.AreEqual("Alex", dt.Rows[0][1]);
            Assert.AreEqual("K+", dt.Rows[1][1]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2][1]);

            Assert.AreEqual(firstDate, dt.Rows[0][2]);
            Assert.AreEqual(secondDate, dt.Rows[1][2]);
            Assert.AreEqual(DBNull.Value, dt.Rows[2][2]);
        }

        private static c.Dict CreateDict(DateTime firstDate, DateTime secondDate)
        {
            string[] columnNames = new string[] { "id", "name", "date" };
            object[] columns = new object[3];
            columns[0] = new int[] { 1, 2, (int)c.NULL(typeof(int)) };
            columns[1] = new string[] { "Alex", "K+", (string)c.NULL(typeof(string)) };
            columns[2] = new DateTime[] { firstDate, secondDate, (DateTime)c.NULL(typeof(DateTime)) };

            return new c.Dict(columnNames, columns);
        }
    }
}
