using System;

namespace KpNet.KdbPlusClient.Simplified
{
    public interface IKdbPlusDatabaseClientPool : IDisposable
    {
        /// <summary>
        /// Gets the connections count.
        /// </summary>
        /// <value>The connections count.</value>
        int ConnectionsCount { get; }

        /// <summary>
        /// Gets the connection from pool.
        /// </summary>
        /// <returns></returns>
        KdbPlusDatabaseClient GetConnection();

        /// <summary>
        /// Returns the connection to pool.
        /// </summary>
        /// <param name="connection">The connection.</param>
        void ReturnConnectionToPool(KdbPlusDatabaseClient connection);

        /// <summary>
        /// Clears this connection pool.
        /// </summary>
        void Clear();
    }
}
