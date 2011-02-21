﻿using System;
using System.Collections.Generic;
using System.Threading;
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Default implementation of connection pool.
    /// </summary>
    internal sealed class DefaultKdbPlusDatabaseClientPool : KdbPlusDatabaseClientPool
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
        private TimeSpan _connectionCleaningInterval;
        private Timer _cleaningTimer;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultKdbPlusDatabaseClientPool"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public DefaultKdbPlusDatabaseClientPool(string connectionString) :this(new KdbPlusConnectionStringBuilder(connectionString))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultKdbPlusDatabaseClientPool"/> class.
        /// </summary>
        /// <param name="builder">The builder.</param>
        public DefaultKdbPlusDatabaseClientPool(KdbPlusConnectionStringBuilder builder)
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

            InitializeConnectionCleaning();
        }

        /// <summary>
        /// Gets the connections count.
        /// </summary>
        /// <value>The connections count.</value>
        public override int ConnectionsCount
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
        public override KdbPlusDatabaseClient GetConnection()
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
        public override void ReturnConnectionToPool(KdbPlusDatabaseClient connection)
        {
            ThrowIfDisposed();

            Guard.ThrowIfNull(connection, "connection");

            lock (_locker)
            {
                if (ConnectionShouldBeDisposed(connection))
                    DisposeConnection(connection);
                else _connectionPool.Add(connection);

                Monitor.Pulse(_locker);
            }
        }

        public override void Dispose()
        {
            lock (_locker)
            {
                if (!_isDisposed && _createdConnections != null)
                {
                    foreach (NonPooledKdbPlusDatabaseClient connection in _createdConnections)
                    {
                        connection.Dispose();
                    }

                    if (_cleaningTimer != null)
                        _cleaningTimer.Dispose();

                    _isDisposed = true;
                }
            }
        }

        /// <summary>
        /// Gets the connection information for current pool.
        /// </summary>
        /// <value>The connection information.</value>
        public override KdbPlusConnectionStringBuilder ConnectionInformation
        {
            get { return _builder; }
        }

        public override bool IsBusy
        {
            get
            {
                ThrowIfDisposed();

                lock (_locker)
                {
                    return _connectionPool.Count == 0;
                }
            }
        }

        /// <summary>
        /// Clears this connection pool.
        /// </summary>
        public override void Refresh()
        {
            ThrowIfDisposed();

            lock (_locker)
            {
                foreach (NonPooledKdbPlusDatabaseClient connection in _createdConnections)
                {
                    connection.IsConnected = false;
                }
            }
        }

        #region Private Members

        private void InitializeConnectionCleaning()
        {
            int lazyTimeoutSeconds = _builder.InactivityTimeout;

            if (lazyTimeoutSeconds > 0)
            {
                TimeSpan connectionCleaningInterval = TimeSpan.FromSeconds(_builder.InactivityTimeout);
                _connectionCleaningInterval = connectionCleaningInterval;
                _cleaningTimer = new Timer(CleanNotUsedConnections, null, TimeSpan.Zero, _connectionCleaningInterval);
            }
        }


        private void CurrentDomainUnload(object sender, EventArgs e)
        {
            Dispose();
        }

        private void CleanNotUsedConnections(object state)
        {
            lock(_locker)
            {
                if (_createdConnections.Count == 0)
                    return;

                DateTime now = DateTime.Now;

                foreach (NonPooledKdbPlusDatabaseClient connection in _createdConnections)
                {
                    if(now - connection.LastUsed > _connectionCleaningInterval)
                        DisposeConnection(connection);
                }
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
            NonPooledKdbPlusDatabaseClient connection = new NonPooledKdbPlusDatabaseClient(_builder);
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

        #endregion

    }
}