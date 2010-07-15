
namespace KpNet.KdbPlusClient.Simplified
{
    public sealed class BalancingKdbPlusDatabaseClientFactory
    {
        private readonly KdbPlusConnectionStringBuilder[] _builders;

        public BalancingKdbPlusDatabaseClientFactory(KdbPlusConnectionStringBuilder[] builders)
        {
            Guard.ThrowIfNull(builders, "builders");

            _builders = builders;
        }

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

        public IDatabaseClient CreateDatabaseClient()
        {
            return new BalancingKdbPlusDatabaseClient(_builders);
        }
    }
}
