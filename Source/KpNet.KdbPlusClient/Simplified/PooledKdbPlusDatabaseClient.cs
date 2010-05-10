using System;
using System.Collections.Generic;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// KdbPlus client implementation that supportes pooling.
    /// </summary>
    public sealed class PooledKdbPlusDatabaseClient : IDatabaseClient
    {
        private static readonly SortedDictionary<string, KdbPlusDatabaseClientPool> _pools =
            new SortedDictionary<string, KdbPlusDatabaseClientPool>(StringComparer.OrdinalIgnoreCase);

        private static readonly object _locker = new object();
        private KdbPlusDatabaseClientPool _pool;
        private IDatabaseClient _innerClient;


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

        private void GetClientFromPool(KdbPlusConnectionStringBuilder builder)
        {
            string connectionString = builder.ConnectionString;

            if (!_pools.TryGetValue(connectionString, out _pool))
            {
                lock (_locker)
                {
                    if (!_pools.TryGetValue(connectionString, out _pool))
                    {
                        _pool = new KdbPlusDatabaseClientPool(builder);
                        _pools.Add(connectionString, _pool);
                    }
                }
            }

            _innerClient = _pool.GetConnection();
        }

        #region IDatabaseClient Members

        public void Dispose()
        {
            if (_pool != null)
                _pool.ReturnConnectionToPool(_innerClient);

            else _innerClient.Dispose();
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
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public DbDataReader ExecuteQuery(string query)
        {
            return _innerClient.ExecuteQuery(query);
        }

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        public void ExecuteNonQuery(string query)
        {
            _innerClient.ExecuteNonQuery(query);
        }

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string query) where T : struct
        {
            return _innerClient.ExecuteScalar<T>(query);
        }

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public object ExecuteScalar(string query)
        {
            return _innerClient.ExecuteScalar(query);
        }

        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        public IMultipleResult ExecuteQueryWithMultipleResult(string query)
        {
            return _innerClient.ExecuteQueryWithMultipleResult(query);
        }

        #endregion
    }
}