using System;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusParameter : DbParameter
    {
        public const string ParameterNamePrifix = "@";

        private DbType _dbType = DbType.Object;
        private string _name;
        private object _value;

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
            set { _dbType = value; }
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
            set { _name = value; }
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
            }
        }

        public string FormatValue
        {
            get
            {
                if (_dbType == DbType.String)
                    return String.Concat("`", _value);
                return _value.ToString();
            }
        }

        private static void CheckParameterName(string parameterName)
        {
            Guard.ThrowIfNullOrEmpty(parameterName, "parameterName");

            if (!parameterName.StartsWith(ParameterNamePrifix))
                throw new ArgumentException(String.Format(CultureInfo.InvariantCulture, "ParameterName  should start with {0}.",ParameterNamePrifix));
        }

        private static DbType InferType(Object value)
        {
            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Empty:
                    throw new SystemException("Invalid data type");

                case TypeCode.Object:
                    return DbType.Object;

                case TypeCode.DBNull:
                case TypeCode.Char:
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
    }
}