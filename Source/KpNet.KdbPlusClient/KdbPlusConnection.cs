using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    // ADO.Net connection implementation for KDB+ provider.
    public sealed class KdbPlusConnection : DbConnection
    {
        private readonly KdbPlusConnectionStringBuilder _builder;
        private IDatabaseClient _client;
        private ConnectionState _state = ConnectionState.Closed;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusConnection"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public KdbPlusConnection(string connectionString)
        {
            _builder = new KdbPlusConnectionStringBuilder(connectionString);
        }

        /// <summary>
        /// Gets or sets the string used to open the connection.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The connection string used to establish the initial connection. The exact contents of the connection string depend on the specific data source for this connection. The default value is an empty string.
        /// </returns>
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

        /// <summary>
        /// Gets a string that describes the state of the connection.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The state of the connection. The format of the string returned depends on the specific type of connection you are using.
        /// </returns>
        public override ConnectionState State
        {
            get { return _state; }
        }

        /// <summary>
        /// Gets the name of the database server to which to connect.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The name of the database server to which to connect. The default value is an empty string.
        /// </returns>
        public override string DataSource
        {
            get { return _builder.Server; }
        }

        /// <summary>
        /// Gets the port.
        /// </summary>
        /// <value>The port.</value>
        public int Port
        {
            get { return _builder.Port; }
        }

        /// <summary>
        /// Closes the connection to the database. This is the preferred method of closing any open connection.
        /// </summary>
        /// <exception cref="T:System.Data.Common.DbException">
        /// The connection-level error that occurred while opening the connection.
        /// </exception>
        public override void Close()
        {
            if (State == ConnectionState.Closed)
                return;

            if (_client != null)
                _client.Dispose();

            _state = ConnectionState.Closed;
        }

        /// <summary>
        /// Gets the client.
        /// </summary>
        /// <value>The client.</value>
        internal IDatabaseClient Client
        {
            get
            {
                if (State != ConnectionState.Open)
                    throw new InvalidOperationException("Connection is not open.");

                return _client;
            }
        }

        /// <summary>
        /// Opens a database connection with the settings specified by the <see cref="P:System.Data.Common.DbConnection.ConnectionString"/>.
        /// </summary>
        public override void Open()
        {
            try
            {
                _state = ConnectionState.Connecting;
                _client = KdbPlusDatabaseClient.Factory.CreateNewClient(_builder);
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
            get {
                    return ThrowHelper.ThrowNotSupported<string>();
                }
        }

        public override string ServerVersion
        {
            get { return ThrowHelper.ThrowNotSupported<string>(); }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return ThrowHelper.ThrowNotSupported<DbTransaction>();
        }

        public override void ChangeDatabase(string databaseName)
        {
            ThrowHelper.ThrowNotSupported();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            
            Close();
        }

        #endregion
    }
}