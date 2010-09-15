
namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Contract class for client factory.
    /// </summary>
    public abstract class  KdbPlusDatabaseClientFactory
    {
        protected KdbPlusDatabaseClientFactory()
        {
        }

        /// <summary>
        /// Creates the new kdb+ client. Pooled or non pooled connection can be used
        /// depending on the Pooling parameter in the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreateNewClient(string connectionString);
        
        /// <summary>
        /// Creates the new kdb+ client. Pooled or non pooled connection can be used
        /// depending on the Pooling parameter in the connection string.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreateNewClient(KdbPlusConnectionStringBuilder builder);

        /// <summary>
        /// Creates the non pooled kdb+ client. TCP connection is created and closed for each client.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreateNonPooledClient(string connectionString);

        /// <summary>
        /// Creates the non pooled kdb+ client. TCP connection is created and closed for each client.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreateNonPooledClient(KdbPlusConnectionStringBuilder builder);

        /// <summary>
        /// Creates the pooled kdb+ client. TCP connection is taken from connection pool. Connection is returned to the pool when client is disposed.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreatePooledClient(string connectionString);

        /// <summary>
        /// Creates the pooled kdb+ client. TCP connection is taken from connection pool. Connection is returned to the pool when client is disposed.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreatePooledClient(KdbPlusConnectionStringBuilder builder);

        /// <summary>
        /// Creates the pooled kdb+ client. TCP connection is taken from connection pool. Connection is returned to the pool when client is disposed.
        /// </summary>
        /// <param name="pool">The pool.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreatePooledClient(KdbPlusDatabaseClientPool pool);

        /// <summary>
        /// Creates the balancing kdb+ client. Connection information is randomly chosen by connection dispatcher. 
        /// </summary>
        /// <param name="dispatcher">The dispatcher.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClient CreateBalancingClient(ConnectionDispatcher dispatcher);

        /// <summary>
        /// Creates the connection pool.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClientPool CreatePool(string connectionString);


        /// <summary>
        /// Creates the connection pool.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <returns></returns>
        public abstract KdbPlusDatabaseClientPool CreatePool(KdbPlusConnectionStringBuilder builder);
    }
}
