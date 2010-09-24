using System.Collections.Generic;
using System.Threading;
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// KdbPlus client implementation that supportes pooling.
    /// </summary>
    internal sealed class PooledKdbPlusDatabaseClient : DelegatingKdbPlusDatabaseClient
    {
        private static readonly Dictionary<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool> _pools =
            new Dictionary<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool>();

        private static readonly ReaderWriterLock _locker = new ReaderWriterLock();

        private KdbPlusDatabaseClientPool _pool;
        
        
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

        private static KdbPlusDatabaseClient GetConnection(KdbPlusDatabaseClientPool pool)
        {
            Guard.ThrowIfNull(pool, "pool");
            return pool.GetConnection();
        }

        public PooledKdbPlusDatabaseClient(KdbPlusDatabaseClientPool pool):base(GetConnection(pool))
        {
            _pool = pool;
        }


        /// <summary>
        /// Refreshes the connection pool if the instance is pooled.
        /// </summary>
        public override void RefreshPool()
        {
            ThrowIfDisposed();
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
                ThrowIfDisposed();
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
                ThrowIfDisposed();
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
                    InnerClient = _pool.GetConnection();
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
            
                     
            InnerClient = _pool.GetConnection();
        }

        
        public override void Dispose()
        {
            if (!IsDisposed)
            {
                _pool.ReturnConnectionToPool(InnerClient);
                InnerClient = null;
                _pool = null;
                IsDisposed = true;
            }
        }
    }
}