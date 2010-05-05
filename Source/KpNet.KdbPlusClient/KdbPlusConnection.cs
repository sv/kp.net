using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusConnection : DbConnection
    {
        private readonly KdbPlusConnectionStringBuilder _builder;
        private IDatabaseClient _client;
        private ConnectionState _state = ConnectionState.Closed;

        public KdbPlusConnection(string connectionString)
        {
            _builder = new KdbPlusConnectionStringBuilder(connectionString);
        }

        public override string ConnectionString
        {
            get { return _builder.ConnectionString; }
            set
            {
                if (_state != ConnectionState.Closed)
                    throw new InvalidOperationException(
                        "Not allowed to schange connection string after the connection was opened.");

                _builder.ConnectionString = value;
            }
        }

        public override ConnectionState State
        {
            get { return _state; }
        }

        public override string DataSource
        {
            get { return _builder.Server; }
        }

        public int Port
        {
            get { return _builder.Port; }
        }

        public override void Close()
        {
            if (_client != null)
                _client.Dispose();

            _state = ConnectionState.Closed;
        }

        internal IDatabaseClient Client
        {
            get
            {
                if (State != ConnectionState.Open)
                    throw new InvalidOperationException("Connection is not open.");

                return _client;
            }
        }

        public override void Open()
        {
            try
            {
                _state = ConnectionState.Connecting;
                _client = new KdbPlusDatabaseClient(_builder.Server, _builder.Port, _builder.UserID, _builder.Password,
                                                    _builder.BufferSize);
                _state = ConnectionState.Open;
            }
            catch (KdbPlusException)
            {
                _state = ConnectionState.Broken;
                throw;
            }
        }

        protected override DbCommand CreateDbCommand()
        {
            return new KdbPlusCommand(this);
        }

        #region NotSupported members

        public override string Database
        {
            get { throw new NotSupportedException(Resources.NotSupportedInKDBPlus); }
        }

        public override string ServerVersion
        {
            get { throw new NotSupportedException(Resources.NotSupportedInKDBPlus); }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException(Resources.NotSupportedInKDBPlus);
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException(Resources.NotSupportedInKDBPlus);
        }

        #endregion
    }
}