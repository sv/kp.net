using System;
using System.Collections;
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

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbDataReader"/> class.
        /// </summary>
        /// <param name="result">The original kdb+ query result.</param>
        public KdbPlusDataReader(c.Flip result)
        {
            Guard.ThrowIfNull(result, "result");

            _result = result;

            InitIndexes();
        }

        public override bool HasRows
        {
            get { return _rowCount > 0; }
        }

        public override bool IsClosed
        {
            get { return _isDisposed; }
        }

        public override int FieldCount
        {
            get { return _columnCount; }
        }

        public override object this[int i]
        {
            get { return GetValue(i); }
        }

        public override object this[string name]
        {
            get
            {
                ThrowIfDisposed();

                int index = GetIndexByName(name);
                if (index == -1)
                {
                    throw new ArgumentException(" Can't locate provided column name.", "name");
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

        public override string GetName(int i)
        {
            ThrowIfDisposed();

            ValidateIndex(i);

            return _result.x[i];
        }

        public override object GetValue(int i)
        {
            return GetCurrentRowValue(i);
        }

        public override int GetValues(object[] values)
        {
            if (values == null || values.Length < _columnCount)
                throw new ArgumentException("Values parameter is incorrect.", "values");

            int minVal = Math.Min(values.Length, _columnCount);

            for (int i = 0; i < minVal; i++)
            {
                values[i] = GetCurrentRowValue(i);
            }
            return minVal;
        }

        public override int GetInt32(int i)
        {
            ThrowIfDisposed();

            return (int) GetValue(i);
        }

        public override long GetInt64(int i)
        {
            ThrowIfDisposed();

            return (Int64) Convert.ChangeType(GetValue(i), typeof (Int64), DefaultCulture);
        }

        public override string GetString(int i)
        {
            return Convert.ToString(GetValue(i), DefaultCulture);
        }

        public override void Close()
        {
            _isDisposed = true;
        }

        public override int GetOrdinal(string name)
        {
            ThrowIfDisposed();

            return GetIndexByName(name);
        }

        public override bool GetBoolean(int i)
        {
            ThrowIfDisposed();

            return (bool) GetValue(i);
        }

        public override byte GetByte(int i)
        {
            ThrowIfDisposed();

            return (byte) GetValue(i);
        }

        public override string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int i)
        {
            ThrowIfDisposed();

            return (char) GetValue(i);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int i)
        {
            throw new NotSupportedException(Resources.NotSupportedInKDBPlus);
        }

        public override short GetInt16(int i)
        {
            throw new NotSupportedException(Resources.NotSupportedInKDBPlus);
        }

        public override float GetFloat(int i)
        {
            ThrowIfDisposed();

            return (float) GetValue(i);
        }

        public override double GetDouble(int i)
        {
            ThrowIfDisposed();

            return (double) GetValue(i);
        }

        public override decimal GetDecimal(int i)
        {
            throw new NotSupportedException(Resources.NotSupportedInKDBPlus);
        }

        public override DateTime GetDateTime(int i)
        {
            return (DateTime) GetValue(i);
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int i)
        {
            ThrowIfDisposed();

            return GetValue(i) == null;
        }

        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
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

        private void ValidateIndex(int i)
        {
            if (i < 0 || i >= FieldCount)
            {
                throw new ArgumentOutOfRangeException("i", "Provided argument is out of range.");
            }
        }

        private object GetCurrentRowValue(int i)
        {
            ThrowIfDisposed();
            ValidateIndex(i);

            object value = c.at(_result.y[i], _currentRowIndex);

            return value;
        }

        private void InitIndexes()
        {
            _columnCount = _result.x.Length;
            _rowCount = ((Array) (_result.y[0])).Length;
            _currentRowIndex = -1;
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

        public static DbDataReader CreateReaderFromCollection(object result)
        {
            var d = new c.Dict(new[] {"Result"}, new[] {result});
            return new KdbPlusDataReader(new c.Flip(d));
        }
    }
}