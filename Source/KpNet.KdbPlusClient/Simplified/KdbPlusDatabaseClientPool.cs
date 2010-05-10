using System;
using System.Collections.Generic;
using System.Threading;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Implementation of connection pool.
    /// </summary>
    internal sealed class KdbPlusDatabaseClientPool : IDisposable
    {
        private readonly KdbPlusConnectionStringBuilder _builder;
        private readonly object _locker = new object();
        private readonly int _maxPoolSize;
        private readonly int _minPoolSize;
        private readonly int _loadBalanceTimeout;
        private List<IDatabaseClient> _connectionPool;
        private List<IDatabaseClient> _createdConnections;
        private int _connectionsCount;
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusDatabaseClientPool"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public KdbPlusDatabaseClientPool(string connectionString) :this(new KdbPlusConnectionStringBuilder(connectionString))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusDatabaseClientPool"/> class.
        /// </summary>
        /// <param name="builder">The builder.</param>
        public KdbPlusDatabaseClientPool(KdbPlusConnectionStringBuilder builder)
        {
            Guard.ThrowIfNull(builder, "builder");

            int minPoolSize = builder.MinPoolSize;

            if (minPoolSize < 0)
                throw new ArgumentException("Value of minPoolSize should be >= 0.");

            int maxPoolSize = builder.MaxPoolSize;

            if (maxPoolSize < minPoolSize || maxPoolSize <= 0)
                throw new ArgumentException("Value of maxPoolSize should be >= 0 and >= minPoolSize.");

            int loadBalanceTimeout = builder.LoadBalanceTimeout;

            if (loadBalanceTimeout < 0)
                throw new ArgumentException("Value of loadBalanceTimeout should be >=0.");

            _builder = builder;
            _minPoolSize = minPoolSize;
            _maxPoolSize = maxPoolSize;
            _loadBalanceTimeout = loadBalanceTimeout;

            InitializeConnectionPool();

            AppDomain.CurrentDomain.UnhandledException += CurrentDomainUnhandledException;
            AppDomain.CurrentDomain.ProcessExit += CurrentDomainProcessExit;
        }

        /// <summary>
        /// Gets the connection from pool.
        /// </summary>
        /// <returns></returns>
        public IDatabaseClient GetConnection()
        {
            ThrowIfDisposed();

            lock (_locker)
            {
                if (_connectionPool.Count != 0)
                    return GetFreeConnection();

                if (_connectionsCount < _maxPoolSize)
                    return CreateNewConnection();

                while (_connectionPool.Count == 0)
                {
                    Monitor.Wait(_locker);

                    if (_connectionPool.Count != 0)
                        return GetFreeConnection();

                    if (_connectionsCount < _maxPoolSize)
                        return CreateNewConnection();

                }

                return GetFreeConnection();

            }
        }

        /// <summary>
        /// Returns the connection to pool.
        /// </summary>
        /// <param name="connection">The connection.</param>
        public void ReturnConnectionToPool(IDatabaseClient connection)
        {
            ThrowIfDisposed();

            lock (_locker)
            {
                // dispose connection if _loadBalanceTimeout is set and is exceeded
                if (_loadBalanceTimeout > 0 && (DateTime.Now - connection.Created).TotalSeconds > _loadBalanceTimeout)
                {
                    _connectionsCount--;
                    connection.Dispose();
                    _createdConnections.Remove(connection);
                }
                else _connectionPool.Add(connection);

                Monitor.Pulse(_locker);
            }
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("Already disposed.");
        }

        void CurrentDomainProcessExit(object sender, EventArgs e)
        {
            Dispose();
        }

        void CurrentDomainUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Dispose();
        }

        #region IDisposable Members

        public void Dispose()
        {
            if (!_isDisposed && _createdConnections != null)
            {
                foreach (IDatabaseClient connection in _createdConnections)
                {
                    connection.Dispose();
                }

                _isDisposed = true;
            }
        }

        #endregion

        private void InitializeConnectionPool()
        {
            _connectionPool = new List<IDatabaseClient>(_maxPoolSize);
            _createdConnections = new List<IDatabaseClient>(_maxPoolSize);

            for (int i = 0; i < _minPoolSize; i++)
            {
                CreateNewConnection();
            }
        }

        private IDatabaseClient CreateNewConnection()
        {
            IDatabaseClient connection = new KdbPlusDatabaseClient(_builder);
            _connectionsCount++;
            _createdConnections.Add(connection);
            return connection;
        }

        private IDatabaseClient GetFreeConnection()
        {
            IDatabaseClient connection = _connectionPool[0];
            _connectionPool.RemoveAt(0);
            return connection;
        }
        
    }
}