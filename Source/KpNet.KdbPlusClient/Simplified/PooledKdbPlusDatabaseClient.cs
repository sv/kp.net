using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// KdbPlus client implementation that supportes pooling.
    /// </summary>
    internal sealed class PooledKdbPlusDatabaseClient : KdbPlusDatabaseClient
    {
        private static readonly Dictionary<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool> _pools =
            new Dictionary<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool>();

        private static readonly ReaderWriterLock _locker = new ReaderWriterLock();

        private KdbPlusDatabaseClientPool _pool;
        private KdbPlusDatabaseClient _innerClient;

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
        internal static IDictionary<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool> Pools
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

            GetClientFromPool(builder);
        }

        public PooledKdbPlusDatabaseClient(KdbPlusDatabaseClientPool pool)
        {
            Guard.ThrowIfNull(pool, "pool");
            _pool = pool;
            _innerClient = _pool.GetConnection();
        }


        /// <summary>
        /// Refreshes the connection pool if the instance is pooled.
        /// </summary>
        public override void RefreshPool()
        {
            _pool.Refresh();
        }

        /// <summary>
        /// Gets a value indicating whether this instance is pooled.
        /// </summary>
        /// <value><c>true</c> if this instance is pooled; otherwise, <c>false</c>.</value>
        public override bool IsPooled
        {
            get
            {
                return true;
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
            // get existing pool
            try
            {
                _locker.AcquireReaderLock(-1);
                if (_pools.TryGetValue(builder, out _pool))
                {
                    _innerClient = _pool.GetConnection();
                    return;
                }
            }
            finally
            {
                _locker.ReleaseReaderLock();
            }
                
            // create new pool
            try
            {
                _locker.AcquireWriterLock(-1);

                if (!_pools.TryGetValue(builder, out _pool))
                {
                    _pool = Factory.CreatePool(builder);
                    _pools.Add(builder, _pool);
                }
            }
            finally
            {
                _locker.ReleaseWriterLock();
            }
            
                     
            _innerClient = _pool.GetConnection();
        }

        #region IDatabaseClient Members

        public override void Dispose()
        {
            _pool.ReturnConnectionToPool(_innerClient);
        }

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public override TimeSpan SendTimeout
        {
            get { return _innerClient.SendTimeout; }
            set { _innerClient.SendTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public override TimeSpan ReceiveTimeout
        {
            get { return _innerClient.ReceiveTimeout; }
            set { _innerClient.ReceiveTimeout = value; }
        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public override DateTime Created
        {
            get { return _innerClient.Created; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        public override bool IsConnected
        {
            get
            {
                return _innerClient.IsConnected;
            }
            internal set { _innerClient.IsConnected = value; }
        }


        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        public override DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQuery(query, parameters);
        }

        /// <summary>
        /// Executes the query returns data table.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>DataTable object</returns>
        public override DataTable ExecuteQueryAsDataTable(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQueryAsDataTable(query, parameters);
        }


        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public override void ExecuteNonQuery(string query, params object[] parameters)
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
        public override void ExecuteOneWayNonQuery(string query, params object[] parameters)
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
        public override T ExecuteScalar<T>(string query, params object[] parameters)
        {
            return _innerClient.ExecuteScalar<T>(query, parameters);
        }


        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public override object ExecuteScalar(string query, params object[] parameters)
        {
            return _innerClient.ExecuteScalar(query, parameters);
        }


        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public override IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQueryWithMultipleResult(query, parameters);
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <returns></returns>
        public override object Receive()
        {
            return _innerClient.Receive();
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public override T Receive<T>()
        {
            return _innerClient.Receive<T>();
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public override DbDataReader ReceiveQueryResult()
        {
            return _innerClient.ReceiveQueryResult();
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public override DataTable ReceiveQueryResultAsDataTable()
        {
            return _innerClient.ReceiveQueryResultAsDataTable();
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public override IMultipleResult ReceiveMultipleQueryResult()
        {
            return _innerClient.ReceiveMultipleQueryResult();
        }

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public override string ConnectionString
        {
            get { return _innerClient.ConnectionString; }
        }

        #endregion
    }
}