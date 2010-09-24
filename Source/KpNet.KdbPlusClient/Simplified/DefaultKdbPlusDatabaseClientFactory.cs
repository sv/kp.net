
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    internal sealed class DefaultKdbPlusDatabaseClientFactory : KdbPlusDatabaseClientFactory
    {
        public override KdbPlusDatabaseClient CreateNewClient(string connectionString)
        {
            return CreateNewClient(StringToBuilder(connectionString));
        }

        public override KdbPlusDatabaseClient CreateNewClient(KdbPlusConnectionStringBuilder builder)
        {
            Guard.ThrowIfNull(builder, "builder");

            if (builder.Pooling)
                return new PooledKdbPlusDatabaseClient(builder);
            return new NonPooledKdbPlusDatabaseClient(builder);
        }

        public override KdbPlusDatabaseClient CreateNonPooledClient(string connectionString)
        {
            return CreateNonPooledClient(StringToBuilder(connectionString));
        }

        public override KdbPlusDatabaseClient CreateNonPooledClient(KdbPlusConnectionStringBuilder builder)
        {
            return new NonPooledKdbPlusDatabaseClient(builder);
        }

        public override KdbPlusDatabaseClient CreatePooledClient(string connectionString)
        {
            return CreatePooledClient(StringToBuilder(connectionString));
        }

        public override KdbPlusDatabaseClient CreatePooledClient(KdbPlusConnectionStringBuilder builder)
        {
            return new PooledKdbPlusDatabaseClient(builder);
        }

        public override KdbPlusDatabaseClient CreatePooledClient(KdbPlusDatabaseClientPool pool)
        {
            return new PooledKdbPlusDatabaseClient(pool);
        }

        public override KdbPlusDatabaseClient CreateBalancingClient(ConnectionDispatcher dispatcher)
        {
            return new BalancingKdbPlusDatabaseClient(dispatcher);
        }

        public override KdbPlusDatabaseClientPool CreatePool(string connectionString)
        {
            return CreatePool(StringToBuilder(connectionString));
        }

        public override KdbPlusDatabaseClientPool CreatePool(KdbPlusConnectionStringBuilder builder)
        {
            return new DefaultKdbPlusDatabaseClientPool(builder);
        }

        private static KdbPlusConnectionStringBuilder StringToBuilder(string connectionString)
        {
            return new KdbPlusConnectionStringBuilder(connectionString);
        }
    }
}
