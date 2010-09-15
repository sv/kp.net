using System;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// This class represents a contract for connection pool implementations.
    /// </summary>
    public abstract class KdbPlusDatabaseClientPool : IDisposable
    {
        protected KdbPlusDatabaseClientPool()
        {
        }

        /// <summary>
        /// Gets the count of connections in the pool.
        /// </summary>
        /// <value>The connections count.</value>
        public abstract int ConnectionsCount { get; }

        /// <summary>
        /// Gets the connection from pool.
        /// </summary>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient GetConnection();

        /// <summary>
        /// Returns the connection to pool.
        /// </summary>
        /// <param name="connection">The connection.</param>
        public abstract void ReturnConnectionToPool(KdbPlusDatabaseClient connection);

        /// <summary>
        /// Clears this connection pool. Each connection in the pool will be recreated.
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// Disposes the pool. After the pool is disposed, it can't be used anymore.
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// Gets the connection information for current pool.
        /// </summary>
        /// <value>The connection information.</value>
        public abstract KdbPlusConnectionStringBuilder ConnectionInformation { get; }
    }
}
