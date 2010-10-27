using System;
using System.Data;
using KpNet.Common;
using kx;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// This class contains set of extension methods to convert .Net DataSet and DataTable into
    /// KDB+ c.Dict class. 
    /// </summary>
    public static class DataConverterExtensions
    {
        /// <summary>
        /// Converts data table into c.Dict.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>c.Dict</returns>
        public static c.Dict ToDict(this DataTable table)
        {
            Guard.ThrowIfNull(table, "table");

            return CreateDict(table);
        }


        /// <summary>
        /// Converts data table into c.Flip.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>c.Flip</returns>
        public static c.Flip ToFlip(this DataTable table)
        {
            Guard.ThrowIfNull(table, "table");

            if(table.Columns.Count == 0)
                return new c.Flip(CreateEmptyDict());

            return new c.Flip(CreateDict(table));
        }

        /// <summary>
        /// Converts the DataSet into the graph of c.Dict objects.
        /// </summary>
        /// <param name="dataSet">The data set.</param>
        /// <returns></returns>
        public static c.Dict ToDict(this DataSet dataSet)
        {
            Guard.ThrowIfNull(dataSet, "dataSet");
            int tableCount = dataSet.Tables.Count;

            if (tableCount == 0)
                return CreateEmptyDict();

            string[] columnNames = new string[tableCount];
            object[] columns = new object[tableCount];

            c.Dict result = new c.Dict(columnNames, columns);

            for (int i = 0; i < tableCount; i++)
            {
                DataTable table = dataSet.Tables[i];
                columnNames[i] = table.TableName??String.Empty;
                columns[i] = new c.Dict[] { table.ToDict() };
            }

            return result;
        }

        /// <summary>
        /// Converts c.Dict into DataTable.
        /// </summary>
        /// <param name="dict">The dict.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static DataTable ToDataTable(this c.Dict dict, string tableName)
        {
            Guard.ThrowIfNull(dict, "dict");
            if (String.IsNullOrEmpty(tableName))
                tableName = "Table1";

            KdbPlusDataReader reader = new KdbPlusDataReader(new c.Flip(dict));
            
            DataTable table = new DataTable(tableName);
            table.Load(reader);
            
            return table;
        }

        /// <summary>
        /// Converts c.Flip into DataTable.
        /// </summary>
        /// <param name="flip">The flip.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static DataTable ToDataTable(this c.Flip flip, string tableName)
        {
            Guard.ThrowIfNull(flip, "flip");

            return ToDataTable(new c.Dict(flip.x, flip.y), tableName);
        }

        /// <summary>
        /// Converts c.Dict into DataTable.
        /// </summary>
        /// <param name="dict">The dict.</param>
        /// <returns></returns>
        public static DataTable ToDataTable(this c.Dict dict)
        {
            return ToDataTable(dict, null);
        }

        /// <summary>
        /// Converts c.Flip into DataTable.
        /// </summary>
        /// <param name="flip">The flip.</param>
        /// <returns></returns>
        public static DataTable ToDataTable(this c.Flip flip)
        {
            Guard.ThrowIfNull(flip, "flip");

            return ToDataTable(new c.Dict(flip.x,flip.y));
        }

        /// <summary>
        /// Converts the graph of c.Dict into DataSet
        /// </summary>
        /// <param name="dict">The dict.</param>
        /// <returns></returns>
        public static DataSet ToDataSet(this c.Dict dict)
        {
            Guard.ThrowIfNull(dict, "dict");

            string[] tableNames = (string[]) dict.x;
            object[] dicts = (object[]) dict.y;

            DataSet ds = new DataSet();

            int tableCount = tableNames.Length;
            if (tableCount == 0)
                return ds;

            for (int i = 0; i < tableCount; i++)
            {
                c.Dict tableDict = (c.Dict)((Array)dicts[i]).GetValue(0);
                ds.Tables.Add(tableDict.ToDataTable(tableNames[i]));
            }

            return ds;
        }

        #region Private Members

        private static c.Dict CreateDict(DataTable table)
        {
            int columnCount = table.Columns.Count;
            if(columnCount == 0)
                return CreateEmptyDict();

            string[] columnNames = new string[table.Columns.Count];

            for (int i = 0; i < columnCount; i++ )
            {
                columnNames[i] = table.Columns[i].ColumnName;
            }

            int rowCount = table.Rows.Count;

            object[] columns = new object[columnCount];


            for (int i = 0; i < columnCount; i++)
            {
                Type type = table.Columns[i].DataType;
                object defaultValue = GetDefaultValue(type);

                Array column = Array.CreateInstance(type, rowCount);

                for (int j = 0; j < rowCount; j++)
                {
                    object value = table.Rows[j][i];

                    if (value == DBNull.Value)
                        value = defaultValue;

                    column.SetValue(value, j);
                }

                columns[i] = column;
            }

            return new c.Dict(columnNames, columns);
        }

        private static object GetDefaultValue(Type type)
        {
            return c.NULL(type);
        }

        private static c.Dict CreateEmptyDict()
        {
            return new c.Dict(new string[]{}, new object[]{});
        }

        #endregion
    }
}
