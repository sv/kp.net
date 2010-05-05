using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusCommand : DbCommand
    {
        private readonly KdbPlusConnection _connection;
        private string _commandText;

        public KdbPlusCommand(string commandText, KdbPlusConnection connection)
        {
            Guard.ThrowIfNull(connection, "connection");
            _commandText = commandText;
            _connection = connection;
        }

        public KdbPlusCommand(KdbPlusConnection connection) : this(null, connection)
        {
        }

        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                Guard.ThrowIfNullOrEmpty(value, "CommandText");
                _commandText = value;
            }
        }

        public override int CommandTimeout
        {
            get
            {
                return Convert.ToInt32(_connection.Client.ReceiveTimeout.TotalMilliseconds);
            }
            set
            {
                _connection.Client.ReceiveTimeout = TimeSpan.FromMilliseconds(value);
            }
        }

        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set { throw new NotSupportedException(Resources.NotSupportedInKDBPlus); }
        }

        
        protected override DbConnection DbConnection
        {
            get { return _connection; }
            set { Guard.ThrowIfNull(value, "DbConnection"); }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            EnsureCommandText();
            return _connection.Client.ExecuteQuery(CommandText);
        }

        public override int ExecuteNonQuery()
        {
            EnsureCommandText();

            _connection.Client.ExecuteNonQuery(CommandText);

            return 0;
        }

        public override object ExecuteScalar()
        {
            EnsureCommandText();

            return _connection.Client.ExecuteScalar(CommandText);
        }

        private void EnsureCommandText()
        {
            if (String.IsNullOrEmpty(_commandText))
                throw new InvalidOperationException("CommandText is empty or not initialized.");
        }

        #region Not implemented or Not Supported members 

        protected override DbParameterCollection DbParameterCollection
        {
            get { throw new NotImplementedException(); }
        }

        protected override DbTransaction DbTransaction
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public override bool DesignTimeVisible
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public override void Prepare()
        {
            throw new NotSupportedException(Resources.NotSupportedInKDBPlus);
        }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}