using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// KdbPlus client implementation that supportes pooling.
    /// </summary>
    public sealed class PooledKdbPlusDatabaseClient : IDatabaseClient
    {
        private static readonly SortedDictionary<string, KdbPlusDatabaseClientPool> _pools =
            new SortedDictionary<string, KdbPlusDatabaseClientPool>(StringComparer.OrdinalIgnoreCase);

        private static readonly ReaderWriterLock _locker = new ReaderWriterLock();

        private KdbPlusDatabaseClientPool _pool;
        private IDatabaseClient _innerClient;

        /// <summary>
        /// Gets the inner client.
        /// </summary>
        /// <value>The inner client.</value>
        internal IDatabaseClient InnerClient
        {
            get
            {
                return _innerClient;
            }
        }

        /// <summary>
        /// Gets the connection pools.
        /// </summary>
        /// <value>The pools.</value>
        internal static IDictionary<string, KdbPlusDatabaseClientPool> Pools
        {
            get
            {
                return _pools;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledKdbPlusDatabaseClient"/> class.
        /// To disable connection pooling - use Pooling=false in the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public PooledKdbPlusDatabaseClient(string connectionString):this(new KdbPlusConnectionStringBuilder(connectionString))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledKdbPlusDatabaseClient"/> class.
        /// To disable connection pooling - use Pooling=false in the connection string.
        /// </summary>
        /// <param name="builder">The builder.</param>
        public PooledKdbPlusDatabaseClient(KdbPlusConnectionStringBuilder builder)
        {
            Guard.ThrowIfNull(builder, "builder");

            if (builder.Pooling)
            {
                GetClientFromPool(builder);
            }

            else _innerClient = new KdbPlusDatabaseClient(builder);
        }

        /// <summary>
        /// Clears the connection pool.
        /// </summary>
        public void ClearPool()
        {
            try
            {
                _locker.AcquireWriterLock(-1);
                _pool.Clear();
            }
            finally
            {
                _locker.ReleaseWriterLock();
            }
        }

        /// <summary>
        /// Gets the connection pool.
        /// </summary>
        /// <value>The pool.</value>
        internal KdbPlusDatabaseClientPool Pool
        {
            get
            {
                return _pool;
            }
        }

        private void GetClientFromPool(KdbPlusConnectionStringBuilder builder)
        {
            string connectionString = builder.ConnectionString;

            _locker.AcquireReaderLock(-1);

            try
            {
                if (!_pools.TryGetValue(connectionString, out _pool))
                {
                    LockCookie lockCookie = _locker.UpgradeToWriterLock(-1);

                    try
                    {
                        if (!_pools.TryGetValue(connectionString, out _pool))
                        {
                            _pool = new KdbPlusDatabaseClientPool(builder);
                            _pools.Add(connectionString, _pool);
                        }
                    }
                    finally
                    {
                        _locker.DowngradeFromWriterLock(ref lockCookie);
                    }
                }

                _innerClient = _pool.GetConnection();
            }
            finally
            {
                _locker.ReleaseReaderLock();
            }
        }

        #region IDatabaseClient Members

        public void Dispose()
        {
            if (_pool != null)
                _pool.ReturnConnectionToPool(_innerClient);

            else _innerClient.Dispose();
        }

        /// <summary>
        /// Gets a value indicating whether this <see cref="PooledKdbPlusDatabaseClient"/> is pooled.
        /// </summary>
        /// <value><c>true</c> if pooled; otherwise, <c>false</c>.</value>
        public bool Pooled
        {
            get
            {
                return _pool != null;
            }
        }

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public TimeSpan SendTimeout
        {
            get { return _innerClient.SendTimeout; }
            set { _innerClient.SendTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public TimeSpan ReceiveTimeout
        {
            get { return _innerClient.ReceiveTimeout; }
            set { _innerClient.ReceiveTimeout = value; }
        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public DateTime Created
        {
            get { return _innerClient.Created; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        public bool IsConnected
        {
            get
            {
                return _innerClient.IsConnected;
            }
        }


        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        public DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQuery(query, parameters);
        }


        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public void ExecuteNonQuery(string query, params object[] parameters)
        {
            _innerClient.ExecuteNonQuery(query, parameters);
        }


        /// <summary>
        /// Executes the instruction that does not return results.
        /// Does not wait for the response - just puts the message into the tcp stack
        /// and exits.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public void ExecuteOneWayNonQuery(string query, params object[] parameters)
        {
            _innerClient.ExecuteOneWayNonQuery(query, parameters);
        }


        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string query, params object[] parameters) where T : struct
        {
            return _innerClient.ExecuteScalar<T>(query, parameters);
        }


        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public object ExecuteScalar(string query, params object[] parameters)
        {
            return _innerClient.ExecuteScalar(query, parameters);
        }


        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQueryWithMultipleResult(query, parameters);
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <returns></returns>
        public object Receive()
        {
            return _innerClient.Receive();
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T Receive<T>()
        {
            return _innerClient.Receive<T>();
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public DbDataReader ReceiveQueryResult()
        {
            return _innerClient.ReceiveQueryResult();
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public IMultipleResult ReceiveMultipleQueryResult()
        {
            return _innerClient.ReceiveMultipleQueryResult();
        }

        #endregion
    }
}