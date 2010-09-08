using System;
using System.Collections.Generic;
using System.Threading;
using KpNet.KdbPlusClient.Simplified;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Implementation of connection pool.
    /// </summary>
    public sealed class KdbPlusDatabaseClientPool : IKdbPlusDatabaseClientPool
    {
        private readonly KdbPlusConnectionStringBuilder _builder;
        private readonly object _locker = new object();
        private readonly int _maxPoolSize;
        private readonly int _minPoolSize;
        private readonly int _loadBalanceTimeout;
        private List<KdbPlusDatabaseClient> _connectionPool;
        private List<KdbPlusDatabaseClient> _createdConnections;
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

            AppDomain.CurrentDomain.DomainUnload += CurrentDomainUnload;

            InitializeConnectionPool();
        }

        void CurrentDomainUnload(object sender, EventArgs e)
        {
            Dispose();
        }

        /// <summary>
        /// Gets the connections count.
        /// </summary>
        /// <value>The connections count.</value>
        public int ConnectionsCount
        {
            get
            {
                lock(_locker)
                {
                    return _connectionsCount;
                }
            }
        }

        /// <summary>
        /// Gets the connection from pool.
        /// </summary>
        /// <returns></returns>
        public KdbPlusDatabaseClient GetConnection()
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
        public void ReturnConnectionToPool(KdbPlusDatabaseClient connection)
        {
            ThrowIfDisposed();

            Guard.ThrowIfNull(connection, "connection");

            lock (_locker)
            {
                _connectionPool.Add(connection);

                Monitor.Pulse(_locker);
            }
        }

        private void DisposeConnection(KdbPlusDatabaseClient connection)
        {
            _connectionsCount--;
            _createdConnections.Remove(connection);
            connection.Dispose();
        }

        private bool ConnectionShouldBeDisposed(KdbPlusDatabaseClient connection)
        {
            // dispose connection if it's broken
            if(!connection.IsConnected)
                return true;

            return _loadBalanceTimeout > 0 && (DateTime.Now - connection.Created).TotalSeconds > _loadBalanceTimeout;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("Already disposed.");
        }

        #region IDisposable Members

        public void Dispose()
        {
            lock (_locker)
            {
                if (!_isDisposed && _createdConnections != null)
                {
                    foreach (KdbPlusDatabaseClient connection in _createdConnections)
                    {
                        connection.Dispose();
                    }

                    _isDisposed = true;
                }
            }
        }

        /// <summary>
        /// Clears this connection pool.
        /// </summary>
        public void Clear()
        {
            ThrowIfDisposed();

            lock (_locker)
            {
                foreach (KdbPlusDatabaseClient connection in _createdConnections)
                {
                    connection.IsConnected = false;
                }
            }
        }

        #endregion

        private void InitializeConnectionPool()
        {
            _connectionPool = new List<KdbPlusDatabaseClient>(_maxPoolSize);
            _createdConnections = new List<KdbPlusDatabaseClient>(_maxPoolSize);

            for (int i = 0; i < _minPoolSize; i++)
            {
                CreateNewConnection();
            }
        }

        private KdbPlusDatabaseClient CreateNewConnection()
        {
            KdbPlusDatabaseClient connection = new KdbPlusDatabaseClient(_builder);
            _connectionsCount++;
            _createdConnections.Add(connection);
            return connection;
        }

        private KdbPlusDatabaseClient GetFreeConnection()
        {
            KdbPlusDatabaseClient connection = _connectionPool[0];
            _connectionPool.RemoveAt(0);

            if (ConnectionShouldBeDisposed(connection))
            {
                DisposeConnection(connection);

                return CreateNewConnection();
            }

            return connection;
        }
        
    }
}