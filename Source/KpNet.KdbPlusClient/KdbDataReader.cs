using System;
using System.Data;
using System.Globalization;
using Kdbplus;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Reader implementation for kdb+ result set.
    /// </summary>
    internal sealed class KdbDataReader : IDataReader
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
        public KdbDataReader(c.Flip result)
        {
            Guard.ThrowIfNull(result, "result");

            _result = result;

            InitIndexes();
        }

        #region IDataReader Members

        public bool IsClosed
        {
            get { return _isDisposed; }
        }

        public int FieldCount
        {
            get { return _columnCount; }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

        public object this[string name]
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

        public bool Read()
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

        public string GetName(int i)
        {
            ThrowIfDisposed();

            ValidateIndex(i);

            return _result.x[i];
        }

        public object GetValue(int i)
        {
            return GetCurrentRowValue(i);
        }

        public int GetValues(object[] values)
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

        public int GetInt32(int i)
        {
            ThrowIfDisposed();

            return (int)GetValue(i);
        }

        public long GetInt64(int i)
        {
            ThrowIfDisposed();

            return (Int64) Convert.ChangeType(GetValue(i), typeof (Int64), DefaultCulture);
        }

        public string GetString(int i)
        {
            return Convert.ToString(GetValue(i), DefaultCulture);
        }

        public void Close()
        {
            _isDisposed = true;
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        #endregion

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
        internal static IDataReader CreateEmptyReader()
        {
            return CreateReaderFromPrimitive(null);
        }

        /// <summary>
        /// Creates the reader from primitive.
        /// </summary>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        internal static IDataReader CreateReaderFromPrimitive(object result)
        {
            c.Dict d = new c.Dict(new[] {"Result"}, new object[] {new[] {result}});
            return new KdbDataReader(new c.Flip(d));
        }

        #region IDisposable implementation

        public void Dispose()
        {
            if (!_isDisposed)
            {
                Close();
            }
        }

        #endregion

        public static IDataReader CreateReaderFromCollection(object result)
        {
            c.Dict d = new c.Dict(new[] {"Result"}, new[] {result});
            return new KdbDataReader(new c.Flip(d));
        }
    }
}