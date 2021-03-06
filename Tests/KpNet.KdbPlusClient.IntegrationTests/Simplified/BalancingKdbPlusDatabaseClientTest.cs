﻿using System.Collections.Generic;
using NUnit.Framework;

namespace KpNet.KdbPlusClient.IntegrationTests.Simplified
{
    [TestFixture]
    public class BalancingKdbPlusDatabaseClientTest : KdbPlusDatabaseClientTests
    {
        protected override IDatabaseClient CreateDatabaseClient(string host, int port)
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder { Server = host, Port = port };

            return new BalancingKdbPlusDatabaseClient(new ConnectionDispatcher(new KdbPlusConnectionStringBuilder[] { builder }));
        }

        protected override IDatabaseClient CreateDatabaseClientFromConString(string connectionString)
        {
            return new BalancingKdbPlusDatabaseClient(new ConnectionDispatcher(new string[] { connectionString }));
        }

        private static KdbPlusConnectionStringBuilder[] CreateBuilders(int count)
        {
            KdbPlusConnectionStringBuilder[] result = new KdbPlusConnectionStringBuilder[count];

            for (int i = 0; i < count; i++)
            {
                result[i] = new KdbPlusConnectionStringBuilder { Server = Constants.Host, Port = Constants.Port, MinPoolSize = i + 1 };
            }

            return result;
        }

        [Test]
        public void TestBalancing()
        {
            int count = 20;
            KdbPlusConnectionStringBuilder[] builders = CreateBuilders(count);

            List<string> connectionStrings = new List<string>(count * 3);

            for (int i = 0; i < 3 * count; i++)
            {
                connectionStrings.Add(new KdbPlusConnectionStringBuilder { Server = Constants.Host, Port = Constants.Port, MinPoolSize = (i % count) + 1 }.ConnectionString);
            }

            Assert.AreEqual(connectionStrings.Count, 3 * count);

            ConnectionDispatcher dispatcher = new ConnectionDispatcher(builders);

            for (int i = 0; i < 3 * count; i++)
            {
                using (BalancingKdbPlusDatabaseClient client = new BalancingKdbPlusDatabaseClient(dispatcher))
                {
                    client.ExecuteNonQuery("0");
                    connectionStrings.Remove(client.ConnectionString);
                }
            }

            Assert.AreEqual(0, connectionStrings.Count);
        }
    }
}
