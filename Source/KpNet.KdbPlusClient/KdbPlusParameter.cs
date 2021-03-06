﻿using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusParameter : DbParameter
    {
        public const string ParameterNamePrefix = "@";

        private DbType _dbType = DbType.Object;
        
        private string _name;
        private object _value;
        public event EventHandler ParameterChanged;

        
        public KdbPlusParameter()
        {
        }

        public KdbPlusParameter(string parameterName, DbType type, object value)
        {
            CheckParameterName(parameterName);
            _name = parameterName;
            _dbType = type;
            _value = value;
        }

        public KdbPlusParameter(string parameterName, object value)
        {
            CheckParameterName(parameterName);
            _name = parameterName;
            Value = value;
            // Setting the value also infers the type.
        }

        public override void ResetDbType()
        {
            ThrowHelper.ThrowNotSupported();
        }

        public override DbType DbType
        {
            get { return _dbType; }
            set
            {
                _dbType = value;
                InvokeParameterChanged();
            }
        }

        public override ParameterDirection Direction
        {
            get { return ParameterDirection.Input; }
            set { ThrowHelper.ThrowNotSupported(); }
        }

        public override Boolean IsNullable
        {
            get { return ThrowHelper.ThrowNotSupported<bool>(); }
            set { ThrowHelper.ThrowNotSupported(); }
        }

        public override String ParameterName
        {
            get { return _name; }
            set {
                    _name = value;
                    InvokeParameterChanged();
                }
        }

        public override int Size
        {
            get { return ThrowHelper.ThrowNotSupported<int>(); }
            set { ThrowHelper.ThrowNotSupported(); }
        }

        public override String SourceColumn
        {
            get { return ThrowHelper.ThrowNotSupported<string>(); }
            set { ThrowHelper.ThrowNotSupported(); }
        }

        public override bool SourceColumnNullMapping
        {
            get { return ThrowHelper.ThrowNotSupported<bool>(); }
            set { ThrowHelper.ThrowNotSupported(); }
        }

        public override DataRowVersion SourceVersion
        {
            get { return ThrowHelper.ThrowNotSupported<DataRowVersion>(); }
            set { ThrowHelper.ThrowNotSupported(); }
        }

        public override object Value
        {
            get { return _value; }
            set
            {
                _value = value;
                _dbType = InferType(value);
                InvokeParameterChanged();
            }
        }

        public string FormatValue
        {
            get
            {
                if (_dbType == DbType.String)
                {
                    if (String.IsNullOrEmpty((string)_value))
                        return "`";

                    return String.Format(CultureInfo.InvariantCulture, "`$\"{0}\"", _value);
                }

                if (_dbType == DbType.Int16 && _value == null)
                    return "0Nh";

                if (_dbType == DbType.Int32 && _value == null)
                    return "0N";

                if (_dbType == DbType.Int64 && _value == null)
                    return "0Nj";

                if (_dbType == DbType.Single && _value == null)
                    return "0Ne";

                if (_dbType == DbType.Double && _value == null)
                    return "0n";

                if (_dbType == DbType.StringFixedLength)
                {
                    if(_value == null)
                        return String.Empty;
                    return String.Format(CultureInfo.InvariantCulture, "\"{0}\"", _value);
                }

                if (_dbType == DbType.DateTime)
                {
                    if(_value == null)
                        return "0Nz";
                    return ((DateTime)_value).ToString("yyyy.MM.ddTHH:mm:ss.fff");
                }

                if (_dbType == DbType.Time)
                {
                    if (_value == null)
                        return "0Nt";
                    return ((TimeSpan)_value).ToString("HH:mm:ss.fff");
                }

                return _value.ToString();
            }
        }

        private static void CheckParameterName(string parameterName)
        {
            Guard.ThrowIfNullOrEmpty(parameterName, "parameterName");

            if (!parameterName.StartsWith(ParameterNamePrefix))
                throw new ArgumentException(String.Format(CultureInfo.InvariantCulture, "ParameterName  should start with {0}.",ParameterNamePrefix));
        }

        private static DbType InferType(Object value)
        {
            if (value == null)
                return DbType.Object;

            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Empty:
                    throw new SystemException("Invalid data type");

                case TypeCode.Object:
                    return DbType.Object;
                case TypeCode.Char:
                    return DbType.StringFixedLength;

                case TypeCode.DBNull:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    // Throw a SystemException for unsupported data types.
                    throw new SystemException("Invalid data type");

                case TypeCode.Boolean:
                    return DbType.Boolean;

                case TypeCode.Byte:
                    return DbType.Byte;

                case TypeCode.Int16:
                    return DbType.Int16;

                case TypeCode.Int32:
                    return DbType.Int32;

                case TypeCode.Int64:
                    return DbType.Int64;

                case TypeCode.Single:
                    return DbType.Single;

                case TypeCode.Double:
                    return DbType.Double;

                case TypeCode.Decimal:
                    return DbType.Decimal;

                case TypeCode.DateTime:
                    return DbType.DateTime;

                case TypeCode.String:
                    return DbType.String;

                default:
                    throw new SystemException("Value is of unknown data type");
            }
        }

        private void InvokeParameterChanged()
        {
            EventHandler handler = ParameterChanged;
            if (handler != null) handler(this, EventArgs.Empty);
        }
    }
}