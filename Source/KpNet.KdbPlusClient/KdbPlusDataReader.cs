using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using Kdbplus;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Reader implementation for kdb+ result set.
    /// </summary>
    internal sealed class KdbPlusDataReader : DbDataReader
    {
        private static readonly CultureInfo DefaultCulture = CultureInfo.InvariantCulture;
        private readonly c.Flip _result;
        private int _columnCount;
        private int _currentRowIndex;
        private bool _isDisposed;
        private int _rowCount;
        private List<Type> _types;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusDataReader"/> class.
        /// </summary>
        /// <param name="result">The original kdb+ query result.</param>
        public KdbPlusDataReader(c.Flip result)
        {
            Guard.ThrowIfNull(result, "result");

            _result = result;

            InitIndexes();
        }

        /// <summary>
        /// Gets a value that indicates whether this <see cref="T:System.Data.Common.DbDataReader"/> contains one or more rows.
        /// </summary>
        /// <value></value>
        /// <returns>true if the <see cref="T:System.Data.Common.DbDataReader"/> contains one or more rows; otherwise false.
        /// </returns>
        public override bool HasRows
        {
            get
            {
                ThrowIfDisposed();
                return _rowCount > 0;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Data.Common.DbDataReader"/> is closed.
        /// </summary>
        /// <value></value>
        /// <returns>true if the <see cref="T:System.Data.Common.DbDataReader"/> is closed; otherwise false.
        /// </returns>
        public override bool IsClosed
        {
            get { return _isDisposed; }
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The number of columns in the current row.
        /// </returns>
        public override int FieldCount
        {
            get { return _columnCount; }
        }

        /// <summary>
        /// Gets the <see cref="System.Object"/> with the specified i.
        /// </summary>
        /// <value></value>
        public override object this[int i]
        {
            get
            {
                ThrowIfDisposed();
                return GetValue(i);
            }
        }

        /// <summary>
        /// Gets the <see cref="System.Object"/> with the specified name.
        /// </summary>
        /// <value></value>
        public override object this[string name]
        {
            get
            {
                ThrowIfDisposed();

                int index = GetIndexByName(name);
                if (index == -1)
                {
                    throw new ArgumentException("Can't locate provided column name.", "name");
                }
                return (this)[index];
            }
        }

        public override int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public override int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns>
        /// true if there are more rows; otherwise false.
        /// </returns>
        public override bool Read()
        {
            ThrowIfDisposed();

            if (_currentRowIndex < _rowCount)
                _currentRowIndex++;

            // close reader if we have read all items
            bool result = _currentRowIndex < _rowCount;
            if (!result)
                Close();

            return result;
        }

        /// <summary>
        /// Gets the name for the field to find.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The name of the field or the empty string (""), if there is no value to return.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override string GetName(int i)
        {
            ThrowIfDisposed();

            ValidateIndex(i);

            return GetNameInternal(i);
        }

        /// <summary>
        /// Return the value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The <see cref="T:System.Object"/> which will contain the field value upon return.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override object GetValue(int i)
        {
            ThrowIfDisposed();

            return GetCurrentRowValue(i);
        }

        /// <summary>
        /// Gets all attribute columns in the collection for the current row.
        /// </summary>
        /// <param name="values">An array of <see cref="T:System.Object"/> into which to copy the attribute columns.</param>
        /// <returns>
        /// The number of instances of <see cref="T:System.Object"/> in the array.
        /// </returns>
        public override int GetValues(object[] values)
        {
            ThrowIfDisposed();

            if (values == null || values.Length < _columnCount)
                throw new ArgumentException("Values parameter is incorrect.", "values");

            int minVal = Math.Min(values.Length, _columnCount);

            for (int i = 0; i < minVal; i++)
            {
                values[i] = GetCurrentRowValue(i);
            }
            return minVal;
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The 32-bit signed integer value of the specified field.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override int GetInt32(int i)
        {
            return (int) GetValue(i);
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The 64-bit signed integer value of the specified field.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override long GetInt64(int i)
        {
            return (Int64) GetValue(i);
        }

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>The string value of the specified field.</returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override string GetString(int i)
        {
            return (string)GetValue(i);
        }

        /// <summary>
        /// Closes the <see cref="T:System.Data.Common.DbDataReader"/> object.
        /// </summary>
        public override void Close()
        {
            _isDisposed = true;
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The name specified is not a valid column name.
        /// </exception>
        public override int GetOrdinal(string name)
        {
            ThrowIfDisposed();

            return GetIndexByName(name);
        }

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the column.</returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override bool GetBoolean(int i)
        {
            return (bool) GetValue(i);
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>
        /// The 8-bit unsigned integer value of the specified column.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override byte GetByte(int i)
        {
            return (byte) GetValue(i);
        }

        public override string GetDataTypeName(int i)
        {
            ThrowIfDisposed();

            return GetFieldType(i).Name;
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int i)
        {
            ThrowIfDisposed();

            ValidateIndex(i);

            return GetFieldTypeInternal(i);
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>
        /// The character value of the specified column.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override char GetChar(int i)
        {
            return (char) GetValue(i);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int i)
        {
            throw new NotSupportedException(Resources.NotSupportedInKdbPlus);
        }

        public override short GetInt16(int i)
        {
            return (Int16)GetValue(i);
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The single-precision floating point number of the specified field.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override float GetFloat(int i)
        {
            return (float) GetValue(i);
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The double-precision floating point number of the specified field.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override double GetDouble(int i)
        {
            return (double) GetValue(i);
        }

        public override decimal GetDecimal(int i)
        {
            throw new NotSupportedException(Resources.NotSupportedInKdbPlus);
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// The date and time data value of the specified field.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override DateTime GetDateTime(int i)
        {
            return (DateTime) GetValue(i);
        }

        /// <summary>
        /// Return whether the specified field is set to null.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>
        /// true if the specified field is set to null; otherwise, false.
        /// </returns>
        /// <exception cref="T:System.IndexOutOfRangeException">
        /// The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>.
        /// </exception>
        public override bool IsDBNull(int i)
        {
            return GetValue(i) == null;
        }

        public override DataTable GetSchemaTable()
        {
            ThrowIfDisposed();

            DataTable table = new DataTable();

            for(int i=0; i< _columnCount; i++)
            {
                table.Columns.Add(new DataColumn(GetNameInternal(i), GetFieldTypeInternal(i)));
            }

            return table.CreateDataReader().GetSchemaTable();
        }

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        private int GetIndexByName(string name)
        {
            Guard.ThrowIfNullOrEmpty(name, "name");

            return Array.FindIndex(_result.x, s => s == name);
        }

        private string GetNameInternal(int i)
        {
            return _result.x[i];
        }

        private Type GetFieldTypeInternal(int i)
        {
            return _types[i];
        }

        private void ValidateIndex(int i)
        {
            if (i < 0 || i >= FieldCount)
            {
                throw new ArgumentOutOfRangeException("i", "Provided argument is out of range.");
            }
        }

        private object GetCurrentRowValue(int i)
        {
            ValidateIndex(i);

            object value = c.at(_result.y[i], _currentRowIndex);

            return value;
        }

        private void InitIndexes()
        {
            _columnCount = _result.x.Length;
            _rowCount = ((Array) (_result.y[0])).Length;
            _currentRowIndex = -1;

            _types = new List<Type>(_columnCount);

            foreach (Array columnValues in _result.y)
            {
                _types.Add(columnValues.GetType().GetElementType());
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Provided object was disposed");
            }
        }

        /// <summary>
        /// Creates the empty reader.
        /// </summary>
        /// <returns></returns>
        internal static DbDataReader CreateEmptyReader()
        {
            return CreateReaderFromPrimitive(null);
        }

        /// <summary>
        /// Creates the reader from primitive.
        /// </summary>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        internal static DbDataReader CreateReaderFromPrimitive(object result)
        {
            var d = new c.Dict(new[] {"Result"}, new object[] {new[] {result}});
            return new KdbPlusDataReader(new c.Flip(d));
        }

        /// <summary>
        /// Creates the reader from collection.
        /// </summary>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        internal static DbDataReader CreateReaderFromCollection(object result)
        {
            var d = new c.Dict(new[] {"Result"}, new[] {result});
            return new KdbPlusDataReader(new c.Flip(d));
        }
    }
}