
namespace KpNet.KdbPlusClient.Simplified
{
    /// <summary>
    /// Factory for load-ballanced access to multiple db instances.
    /// </summary>
    public sealed class BalancingKdbPlusDatabaseClientFactory
    {
        private readonly KdbPlusConnectionStringBuilder[] _builders;

        /// <summary>
        /// Initializes a new instance of the <see cref="BalancingKdbPlusDatabaseClientFactory"/> class.
        /// </summary>
        /// <param name="builders">The connection string builders.</param>
        public BalancingKdbPlusDatabaseClientFactory(KdbPlusConnectionStringBuilder[] builders)
        {
            Guard.ThrowIfNull(builders, "builders");

            _builders = builders;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BalancingKdbPlusDatabaseClientFactory"/> class.
        /// </summary>
        /// <param name="connectionStrings">The connection strings.</param>
        public BalancingKdbPlusDatabaseClientFactory(string[] connectionStrings)
            : this(CreateConnectionBuilders(connectionStrings))
        {            
        } 

        private static KdbPlusConnectionStringBuilder[] CreateConnectionBuilders(string[] connectionStrings)
        {
            Guard.ThrowIfNull(connectionStrings, "connectionStrings");

            KdbPlusConnectionStringBuilder[] result = new KdbPlusConnectionStringBuilder[connectionStrings.Length];

            for (int i = 0; i < connectionStrings.Length; i++ )
            {
                result[i] = new KdbPlusConnectionStringBuilder(connectionStrings[i]);
            }
            
            return result;
        }

        /// <summary>
        /// Creates the database client randomly choosing connection info.
        /// </summary>
        /// <returns></returns>
        public IDatabaseClient CreateDatabaseClient()
        {
            return new BalancingKdbPlusDatabaseClient(_builders);
        }
    }
}
