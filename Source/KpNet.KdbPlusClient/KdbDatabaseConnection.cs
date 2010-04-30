using System;
using System.Collections;
using System.Data;
using System.Text;
using Kdbplus;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Implementation class for kdb+ related operations.
    /// </summary>
    public sealed class KdbDatabaseConnection : IDatabaseConnection
    {
        private readonly c _client;
        private TimeSpan _receiveTimeout = TimeSpan.FromMinutes(1);
        private TimeSpan _sendTimeout = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbDatabaseConnection"/> class.
        /// </summary>
        /// <param name="host">The host.</param>
        /// <param name="port">The port.</param>
        public KdbDatabaseConnection(string host, int port)
        {
            Guard.ThrowIfNullOrEmpty(host, "host");
            
            if (port <= 0)
                throw new ArgumentException(String.Concat("Invalid port:", port));

            _client = new c(host, port)
                          {
                              SendTimeout = ToMilliSeconds(_sendTimeout),
                              ReceiveTimeout = ToMilliSeconds(_receiveTimeout)
                          };
            c.e = Encoding.UTF8;
        }

        #region IDatabaseConnection Members

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

            return new KdbMultipleResult((c.Dict)queryResult);
        }        

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public IDataReader ExecuteQuery(string query)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            object result = DoNativeQuery(query);

            if (result == null)
                return KdbDataReader.CreateEmptyReader();

            Type resultType = result.GetType();

            // table is returned from k+
            if (resultType.IsAssignableFrom(typeof (c.Dict)) || resultType.IsAssignableFrom(typeof (c.Flip)))
                return new KdbDataReader(c.td(result));

            // collection is returned
            if (result as IEnumerable != null)
                return KdbDataReader.CreateReaderFromCollection(result);

            // primitive e.g. count is returned
            return KdbDataReader.CreateReaderFromPrimitive(result);
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
                throw new DatabaseException("Query failed.", query, exc);
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
    }
}