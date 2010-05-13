using NUnit.Framework;

namespace KpNet.KdbPlusClient.IntegrationTests.Simplified
{
    [TestFixture]
    public sealed class PooledKdbPlusDatabaseClientTests : KdbPlusDatabaseClientTests
    {
        protected override IDatabaseClient CreateDatabaseClient(string host, int port)
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder {Server = host, Port = port};

            return new PooledKdbPlusDatabaseClient(builder.ConnectionString);
        }

        protected override IDatabaseClient CreateDatabaseClientFromConString(string connectionString)
        {
            return new PooledKdbPlusDatabaseClient(connectionString);
        }
    }
}
