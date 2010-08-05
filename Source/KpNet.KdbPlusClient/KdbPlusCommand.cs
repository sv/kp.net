using System;
using System.Data;
using System.Data.Common;
using System.Text;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// ADO.Net command implementation for KDB+ provider.
    /// </summary>
    public sealed class KdbPlusCommand : DbCommand
    {
        private readonly KdbPlusConnection _connection;
        private string _commandText;
        private string _preparedText;
        private object[] _preparedParameters;

        private readonly KdbPlusParameterCollection _parameters = new KdbPlusParameterCollection();

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
            _parameters.ParametersChanged += ClearPreparedValues;
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
                _preparedText = null;
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
            set { ThrowHelper.ThrowNotSupported(); }
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

            DbDataReader reader = _connection.Client.ExecuteQuery(_preparedText, _preparedParameters);

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

            _connection.Client.ExecuteNonQuery(_preparedText, _preparedParameters);

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
            
            return _connection.Client.ExecuteScalar(_preparedText, _preparedParameters);
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
            PrepareQuery();
        }

       
        private void EnsureCommandText()
        {
            if (String.IsNullOrEmpty(_commandText))
                throw new InvalidOperationException("CommandText is empty or not initialized.");

            if (String.IsNullOrEmpty(_preparedText))
                PrepareQuery();
        }

        private void PrepareQuery()
        {
            object[] preparedParameters = null;
            _preparedText = _commandText;

            bool anyParamsInQuery = _preparedText.Contains(KdbPlusParameter.ParameterNamePrefix);
            int parameterCount = _parameters.Count;

            if (anyParamsInQuery)
            {
                _preparedText = GetPreparedText(parameterCount, ref preparedParameters);
            }
            else
            {
                if (parameterCount > 0)
                {
                    preparedParameters = new object[parameterCount];

                    for (int i = 0; i < parameterCount; i++)
                        preparedParameters[i] = _parameters[i].Value;
                }
            }

            _preparedParameters = preparedParameters;
        }

        private string GetPreparedText(int parameterCount, ref object[] preparedParameters)
        {
            string preparedText;
            int replacedCount = 0;

            if (parameterCount > 0)
            {
                KdbPlusParameter[] cachedParameters = new KdbPlusParameter[parameterCount];
                _parameters.CopyTo(cachedParameters, 0);

                StringBuilder builder = new StringBuilder(_commandText);

                for (int i = 0; i < parameterCount; i++)
                {
                    KdbPlusParameter param = cachedParameters[i];

                    if (!String.IsNullOrEmpty(param.ParameterName) && _commandText.Contains(param.ParameterName))
                    {
                        replacedCount++;
                        builder.Replace(param.ParameterName, param.FormatValue);
                        cachedParameters[i] = null;
                    }
                }

                preparedText = builder.ToString();
                if (parameterCount - replacedCount > 0)
                {
                    preparedParameters = new object[parameterCount - replacedCount];

                    int added = 0;
                    for (int i = 0; i < parameterCount; i++)
                    {
                        KdbPlusParameter param = cachedParameters[i];

                        if (param != null)
                        {
                            preparedParameters[added] = param.Value;
                            added++;
                        }
                    }
                }

            }
            else throw new InvalidOperationException("No parameters were specified for command.");
            return preparedText;
        }

        private void ClearPreparedValues(object sender, EventArgs e)
        {
            _preparedText = null;
            _preparedParameters = null;
        }

        #region Not implemented or Not Supported members 

        protected override DbParameterCollection DbParameterCollection
        {
            get { return _parameters; }
        }

        protected override DbTransaction DbTransaction
        {
            get { return ThrowHelper.ThrowNotImplemented<DbTransaction>(); }
            set { ThrowHelper.ThrowNotImplemented(); }
        }

        public override bool DesignTimeVisible
        {
            get { return false; }
            set { ThrowHelper.ThrowNotImplemented(); }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { return ThrowHelper.ThrowNotImplemented<UpdateRowSource>(); }
            set { ThrowHelper.ThrowNotImplemented(); }
        }

        public override void Cancel()
        {
            ThrowHelper.ThrowNotSupported();
        }

        protected override DbParameter CreateDbParameter()
        {
            return new KdbPlusParameter();
        }

        #endregion
    }
}