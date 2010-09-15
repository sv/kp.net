using System.Collections.Generic;
using System.Threading;
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
            return KdbPlusDatabaseClient.Factory.CreateNewClient(connectionString);
        }

        [Test]
        public void SimplePoolingTest()
        {
            PooledKdbPlusDatabaseClient client = (PooledKdbPlusDatabaseClient)CreateDatabaseClient();
            Assert.IsNotNull(client.Pool);
            client.Dispose();
        }

        [Test]
        public void DisabledPoolingTest()
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder { Server = Constants.Host, Port = Constants.Port, Pooling = false};
            NonPooledKdbPlusDatabaseClient client = (NonPooledKdbPlusDatabaseClient)CreateDatabaseClientFromConString(builder.ConnectionString);
            client.Dispose();
        }

        [Test]
        public void MultiThreadedSinglePoolTest()
        {
            const int threadCount = 5;
            Thread[] threads = new Thread[threadCount];

            for (int i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(
                    delegate()
                    {
                        for (int j = 0; j < threadCount; j++)
                        {
                            using (IDatabaseClient client = CreateDatabaseClient())
                            {
                                client.ExecuteNonQuery("0");
                                Thread.Sleep(100);
                            }
                        }
                    }
                    );
            }

            for (int i = 0; i < threadCount; i++)
            {
                threads[i].Start();
            }

            for (int i = 0; i < threadCount; i++)
            {
                threads[i].Join();
            }

            PooledKdbPlusDatabaseClient testClient = (PooledKdbPlusDatabaseClient)CreateDatabaseClient();
            Assert.IsNotNull(testClient.Pool);
            Assert.AreEqual(threadCount, testClient.Pool.ConnectionsCount);
        }

        [Test]
        public void MultiThreadedTwoPoolsTest()
        {
            const int threadCount = 1;
            Thread[] threads = new Thread[threadCount];

            foreach (KeyValuePair<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool> entry in PooledKdbPlusDatabaseClient.Pools)
            {
                entry.Value.Dispose();
            }
            PooledKdbPlusDatabaseClient.Pools.Clear();

            KdbPlusConnectionStringBuilder builder1 = new KdbPlusConnectionStringBuilder { Server = Constants.Host, Port = Constants.Port};
            KdbPlusConnectionStringBuilder builder2 = new KdbPlusConnectionStringBuilder { Server = Constants.Host, Port = Constants.Port, MaxPoolSize = 15};
            
            for (int i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(
                    delegate()
                    {
                        for (int j = 0; j < threadCount; j++)
                        {
                            using (IDatabaseClient client = CreateDatabaseClientFromConString(builder1.ConnectionString)
                                )
                            {
                                client.ExecuteNonQuery("0");
                                Thread.Sleep(100);
                            }

                            using (IDatabaseClient client = CreateDatabaseClientFromConString(builder2.ConnectionString)
                                )
                            {
                                client.ExecuteNonQuery("0");
                                Thread.Sleep(100);
                            }
                        }
                    }
                    );
            }

            for (int i = 0; i < threadCount; i++)
            {
                threads[i].Start();
            }

            for (int i = 0; i < threadCount; i++)
            {
                threads[i].Join();
            }

            Assert.AreEqual(2, PooledKdbPlusDatabaseClient.Pools.Keys.Count);

            foreach (KeyValuePair<KdbPlusConnectionStringBuilder, KdbPlusDatabaseClientPool> entry in PooledKdbPlusDatabaseClient.Pools)
            {
                Assert.AreEqual(threadCount, entry.Value.ConnectionsCount);
            }
        }

        [Test]
        public void LoadBalanceTimeoutTest()
        {
            KdbPlusConnectionStringBuilder builder1 = new KdbPlusConnectionStringBuilder { Server = Constants.Host, Port = Constants.Port, LoadBalanceTimeout = 3};

            PooledKdbPlusDatabaseClient client1;
            using (client1 = (PooledKdbPlusDatabaseClient)CreateDatabaseClientFromConString(builder1.ConnectionString)
                                )
            {
                client1.ExecuteNonQuery("0");
                Thread.Sleep(5000);
            }


            // with delay there should be new connection
            PooledKdbPlusDatabaseClient client2;
            using (client2 = (PooledKdbPlusDatabaseClient)CreateDatabaseClientFromConString(builder1.ConnectionString)
                                )
            {
                Assert.AreNotSame(client1.InnerClient, client2.InnerClient);
                
            }
            using (PooledKdbPlusDatabaseClient client3 = (PooledKdbPlusDatabaseClient)CreateDatabaseClientFromConString(builder1.ConnectionString)
                                )
            {
                // without delay connection should be the same
                Assert.AreSame(client2.InnerClient, client3.InnerClient);
            }

        }
    }
}
