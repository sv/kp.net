using System;
using System.Collections;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Kdbplus;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Client implementation for kdb+ related operations.
    /// </summary>
    public sealed class KdbPlusDatabaseClient : IDatabaseClient
    {
        private c _client;
        private TimeSpan _receiveTimeout = TimeSpan.FromMinutes(1);
        private TimeSpan _sendTimeout = TimeSpan.FromMinutes(1);
        private DateTime _created;

        static KdbPlusDatabaseClient()
        {
            c.e = Encoding.UTF8;
        }

        public KdbPlusDatabaseClient(string connectionString)
            : this(new KdbPlusConnectionStringBuilder(connectionString))
        {
        }

        public KdbPlusDatabaseClient(KdbPlusConnectionStringBuilder builder)
        {
            Guard.ThrowIfNull(builder, "builder");

            Initialize(builder.Server, builder.Port, builder.UserID, builder.Password, builder.BufferSize);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="port">The port.</param>
        public KdbPlusDatabaseClient(string server, int port)
            : this(server, port, null, null, KdbPlusConnectionStringBuilder.DefaultBufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="port">The port.</param>
        /// <param name="userId">The user name.</param>
        /// <param name="password">The password.</param>
        /// <param name="bufferSize">The buffer size.</param>
        public KdbPlusDatabaseClient(string server, int port, string userId, string password, int bufferSize)
        {
            Initialize(server, port, userId, password, bufferSize);
        }

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
                throw new KdbPlusException("Failed to connect to server.", ex);
            }
        }

        #region IDatabaseClient Members

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string query) where T : struct
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            object result = DoNativeQuery(query);
            if (result is T)
                return (T) result;

            return (T) GetResultFromFlip(result);
        }

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public object ExecuteScalar(string query)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            return DoNativeQuery(query);
        }

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public TimeSpan SendTimeout
        {
            get { return _sendTimeout; }
            set
            {
                _client.SendTimeout = ToMilliSeconds(value);
                _sendTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public TimeSpan ReceiveTimeout
        {
            get { return _receiveTimeout; }
            set
            {
                _client.ReceiveTimeout = ToMilliSeconds(value);
                _receiveTimeout = value;
            }
        }

        public IMultipleResult ExecuteQueryWithMultipleResult(string query)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            object queryResult = DoNativeQuery(query);

            return new KdbMultipleResult((c.Dict) queryResult);
        }

        public DateTime Created
        {
            get { return _created; }
        }

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public DbDataReader ExecuteQuery(string query)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            object result = DoNativeQuery(query);

            if (result == null)
                return KdbPlusDataReader.CreateEmptyReader();

            Type resultType = result.GetType();

            // table is returned from k+
            if (resultType.IsAssignableFrom(typeof (c.Dict)) || resultType.IsAssignableFrom(typeof (c.Flip)))
                return new KdbPlusDataReader(c.td(result));

            // collection is returned
            if (result as IEnumerable != null)
                return KdbPlusDataReader.CreateReaderFromCollection(result);

            // primitive e.g. count is returned
            return KdbPlusDataReader.CreateReaderFromPrimitive(result);
        }

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        public void ExecuteNonQuery(string query)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            DoNativeQuery(query);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (_client != null)
                _client.Close();
        }

        #endregion

        private object DoNativeQuery(string query)
        {
            try
            {
                return _client.k(query);
            }
            catch (Exception exc)
            {
                throw new KdbPlusException("Query execution exception.", query, exc);
            }
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
    }
}