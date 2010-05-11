using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// ADO.Net command implementation for KDB+ provider.
    /// </summary>
    public sealed class KdbPlusCommand : DbCommand
    {
        private readonly KdbPlusConnection _connection;
        private string _commandText;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusCommand"/> class.
        /// </summary>
        /// <param name="commandText">The command text.</param>
        /// <param name="connection">The connection.</param>
        public KdbPlusCommand(string commandText, KdbPlusConnection connection)
        {
            Guard.ThrowIfNull(connection, "connection");
            _commandText = commandText;
            _connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusCommand"/> class.
        /// </summary>
        /// <param name="connection">The connection.</param>
        public KdbPlusCommand(KdbPlusConnection connection) : this(null, connection)
        {
        }

        /// <summary>
        /// Gets or sets the text command to run against the data source.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The text command to execute. The default value is an empty string ("").
        /// </returns>
        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                Guard.ThrowIfNullOrEmpty(value, "CommandText");
                _commandText = value;
            }
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The time in seconds to wait for the command to execute.
        /// </returns>
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

        /// <summary>
        /// Indicates or specifies how the <see cref="P:System.Data.Common.DbCommand.CommandText"/> property is interpreted.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// One of the <see cref="T:System.Data.CommandType"/> values. The default is Text.
        /// </returns>
        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set { throw new NotSupportedException(Resources.NotSupportedInKDBPlus); }
        }


        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.Common.DbConnection"/> used by this <see cref="T:System.Data.Common.DbCommand"/>.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The connection to the data source.
        /// </returns>
        protected override DbConnection DbConnection
        {
            get { return _connection; }
            set { Guard.ThrowIfNull(value, "DbConnection"); }
        }

        /// <summary>
        /// Executes the command text against the connection.
        /// </summary>
        /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior"/>.</param>
        /// <returns>
        /// A <see cref="T:System.Data.Common.DbDataReader"/>.
        /// </returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            EnsureCommandText();
            
            DbDataReader reader = _connection.Client.ExecuteQuery(CommandText);

            if ((behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
                _connection.Close();

            return reader;
        }

        /// <summary>
        /// Executes a SQL statement against a connection object.
        /// </summary>
        /// <returns>The number of rows affected.</returns>
        public override int ExecuteNonQuery()
        {
            EnsureCommandText();

            _connection.Client.ExecuteNonQuery(CommandText);

            return 0;
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query. All other columns and rows are ignored.
        /// </summary>
        /// <returns>
        /// The first column of the first row in the result set.
        /// </returns>
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