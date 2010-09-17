using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using Kdbplus;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Client implementation for kdb+ related operations.
    /// </summary>
    internal sealed class NonPooledKdbPlusDatabaseClient : KdbPlusDatabaseClient
    {
        private static readonly Type _dictType = typeof (c.Dict);
        private static readonly Type _flipType = typeof(c.Flip);
        private c _client;
        private TimeSpan _receiveTimeout = TimeSpan.FromMinutes(1);
        private TimeSpan _sendTimeout = TimeSpan.FromMinutes(1);
        private DateTime _created;
        private readonly KdbPlusConnectionStringBuilder _builder;
        private bool _canBeReused = true;

        public override string ConnectionString
        {
            get { return _builder.ConnectionString; }
        }

        static NonPooledKdbPlusDatabaseClient()
        {
            c.e = Encoding.UTF8;
        }

        public NonPooledKdbPlusDatabaseClient(string connectionString)
            : this(new KdbPlusConnectionStringBuilder(connectionString))
        {
        }

        public NonPooledKdbPlusDatabaseClient(KdbPlusConnectionStringBuilder builder)
        {
            Guard.ThrowIfNull(builder, "builder");

            _builder = builder;

            Initialize(builder.Server, builder.Port, builder.UserID, builder.Password, builder.BufferSize);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NonPooledKdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="port">The port.</param>
        public NonPooledKdbPlusDatabaseClient(string server, int port)
            : this(server, port, null, null, KdbPlusConnectionStringBuilder.DefaultBufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NonPooledKdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="port">The port.</param>
        /// <param name="userId">The user name.</param>
        /// <param name="password">The password.</param>
        /// <param name="bufferSize">The buffer size.</param>
        public NonPooledKdbPlusDatabaseClient(string server, int port, string userId, string password, int bufferSize)
        {
            Initialize(server, port, userId, password, bufferSize);
        }

        #region IDatabaseClient Members


        /// <summary>
        /// Executes the instruction that does not return results.
        /// Does not wait for the response - just puts the message into the tcp stack
        /// and exits.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public override void ExecuteOneWayNonQuery(string query, params object[] parameters)
        {
            CheckInnerState();

            Guard.ThrowIfNullOrEmpty(query, "query");

            DoNativeOneWayQuery(query, parameters);
        }


        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public override T ExecuteScalar<T>(string query, params object[] parameters)
        {
            CheckInnerState();

            object result = DoNativeQuery(query, parameters);
            return GetTypedResult<T>(result);
        }

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public override object ExecuteScalar(string query, params object[] parameters)
        {
            CheckInnerState();

            return DoNativeQuery(query, parameters);
        }

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public override TimeSpan SendTimeout
        {
            get { return _sendTimeout; }
            set
            {
                CheckInnerState();
                _client.SendTimeout = ToMilliSeconds(value);
                _sendTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public override TimeSpan ReceiveTimeout
        {
            get { return _receiveTimeout; }
            set
            {
                CheckInnerState();
                _client.ReceiveTimeout = ToMilliSeconds(value);
                _receiveTimeout = value;
            }
        }

        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public override IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            CheckInnerState();

            object queryResult = DoNativeQuery(query, parameters);

            return new KdbMultipleResult((c.Dict) queryResult);
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <returns></returns>
        public override object Receive()
        {
            CheckInnerState();

            try
            {
                return _client.k();
            }
            catch (ThreadAbortException)
            {
                MarkAsNotReusable();
                throw;
            }
            catch (KException ex)
            {
                throw new KdbPlusException("Query execution exception.", ex);
            }
            catch (Exception ex)
            {
                return HandleFatalException(ex);
            }
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public override T Receive<T>()
        {
            return GetTypedResult<T>(Receive());
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public override DbDataReader ReceiveQueryResult()
        {
            return CreateReader(Receive());
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public override DataTable ReceiveQueryResultAsDataTable()
        {
            DbDataReader reader = ReceiveQueryResult();

            return ConvertReaderToTable(reader);
        }


        /// <summary>
        /// Receives multiple query results from server.
        /// </summary>
        /// <returns></returns>
        public override IMultipleResult ReceiveMultipleQueryResult()
        {
            object queryResult = Receive();

            return new KdbMultipleResult((c.Dict)queryResult);
        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public override DateTime Created
        {
            get { return _created; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        public override bool IsConnected
        {
            get { return _canBeReused && !IsDisposed && _client.Connected; }
            internal set { _canBeReused = value; }
        }

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        public override DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            CheckInnerState();

            object result = DoNativeQuery(query, parameters);

            return CreateReader(result);
        }

        /// <summary>
        /// Executes the query returns data table.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public override DataTable ExecuteQueryAsDataTable(string query, params object[] parameters)
        {
            CheckInnerState();

            DbDataReader reader = ExecuteQuery(query, parameters);

            return ConvertReaderToTable(reader);
        }

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public override void ExecuteNonQuery(string query, params object[] parameters)
        {
            CheckInnerState();

            DoNativeQuery(query, parameters);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            if (!IsDisposed)
            {
                if(_client != null)
                    _client.Close();
                IsDisposed = true;
            }
        }

        #endregion



        private void Initialize(string server, int port, string userId, string password, int bufferSize)
        {
            Guard.ThrowIfNullOrEmpty(server, "server");

            if (port <= 0)
                throw new ArgumentException(String.Concat("Invalid port:", port));

            try
            {
                _client = new c(server, port, FormatUserName(userId, password), bufferSize)
                {
                    SendTimeout = ToMilliSeconds(_sendTimeout),
                    ReceiveTimeout = ToMilliSeconds(_receiveTimeout)
                };
                _created = DateTime.Now;
            }
            catch (Exception ex)
            {
                throw new KdbPlusFatalException("Failed to connect to server.", ex);
            }
        }

        private static T GetTypedResult<T>(object result)
        {
            if (result is T)
                return (T)result;

            return (T)GetResultFromFlip(result);
        }

        private static DbDataReader CreateReader(object result)
        {
            if (result == null)
                return KdbPlusDataReader.CreateEmptyReader();

            Type resultType = result.GetType();

            // table is returned from k+ as flip
            if (resultType == _flipType)
                return new KdbPlusDataReader((c.Flip)result);

            // result returned as dict
            if (resultType == _dictType)
                return new KdbPlusDataReader(c.td(result));

            // collection is returned
            if (result as IEnumerable != null)
                return KdbPlusDataReader.CreateReaderFromCollection(result);

            // primitive e.g. count is returned
            return KdbPlusDataReader.CreateReaderFromPrimitive(result);
        }

        private object DoNativeQuery(string query, params object[] parameters)
        {
            try
            {
                int length;

                if (parameters == null || (length = parameters.Length) == 0)
                    return _client.k(query);
                
                if (length == 1)
                    return _client.k(query, parameters[0]);

                if (length == 2)
                    return _client.k(query, parameters[0], parameters[1]);

                if (length == 3)
                    return _client.k(query, parameters[0], parameters[1], parameters[2]);

                return _client.k(query, parameters);
            }
            catch (ThreadAbortException)
            {
                MarkAsNotReusable();
                throw;
            }
            catch (KException ex)
            {
                throw new KdbPlusException("Query execution exception.", query, ex);
            }
            catch (Exception ex)
            {
                return HandleFatalException(ex);
            }
        }

        private void DoNativeOneWayQuery(string query, params object[] parameters)
        {
            try
            {
                int length;

                if (parameters == null || (length = parameters.Length) == 0)
                {
                    _client.ks(query);
                    return;
                }

                if (length == 1)
                {
                    _client.ks(query, parameters[0]);
                    return;
                }

                if (length == 2)
                {
                    _client.ks(query, parameters[0], parameters[1]);
                    return;
                }

                _client.ks(query, parameters);
            }
            catch (ThreadAbortException)
            {
                MarkAsNotReusable();
                throw;
            }
            catch (KException ex)
            {
                throw new KdbPlusException("Query execution exception.", query, ex);
            }
            catch (Exception ex)
            {
                HandleFatalException(ex);
            }
        }

        private void CheckInnerState()
        {
            if(!_canBeReused)
                throw new InvalidOperationException("Connection can't be reused.");

            ThrowIfDisposed();
        }

        private object HandleFatalException(Exception ex)
        {
            MarkAsNotReusable();
            throw new KdbPlusFatalException("Failed to receive results from server.", ex);
        }

        private void MarkAsNotReusable()
        {
            _canBeReused = false;
            Dispose();
        }

        private static object GetResultFromFlip(object value)
        {
            c.Flip result = c.td(value);
            object[] values = result.y;

            return ((Array) values[0]).GetValue(0);
        }

        private static int ToMilliSeconds(TimeSpan interval)
        {
            return Convert.ToInt32(interval.TotalMilliseconds);
        }

        private static string FormatUserName(string userName, string password)
        {
            if (String.IsNullOrEmpty(userName))
                return String.Empty;

            return String.Format(CultureInfo.InvariantCulture, "{0}:{1}", userName, password);
        }

        private static DataTable ConvertReaderToTable(DbDataReader reader)
        {
            using (reader)
            {
                DataTable table = new DataTable();

                table.Load(reader);
                return table;
            }
        }
    }
}