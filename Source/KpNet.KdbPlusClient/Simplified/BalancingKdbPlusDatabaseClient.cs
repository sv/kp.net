
namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Round-robin client implementation.
    /// This client choose one random connection location and work with it. 
    /// </summary>
    internal sealed class BalancingKdbPlusDatabaseClient : DelegatingKdbPlusDatabaseClient
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BalancingKdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="dispatcher">The dispatcher.</param>
        public BalancingKdbPlusDatabaseClient(ConnectionDispatcher dispatcher):base(GetConnection(dispatcher))
        {
        }

        private static KdbPlusDatabaseClient GetConnection(ConnectionDispatcher dispatcher)
        {
            Guard.ThrowIfNull(dispatcher, "dispatcher");
            return Factory.CreateNewClient(dispatcher.GetRandomConnection());
        }

        public override bool IsPooled
        {
            get
            {
                ThrowIfDisposed();
                return InnerClient.IsPooled;
            }
        }

        public override void RefreshPool()
        {
            ThrowIfDisposed();
            InnerClient.RefreshPool();
        }

        #region IDisposable Members

        public override void Dispose()
        {
            if (!IsDisposed)
            {
                if (InnerClient != null)
                {
                    InnerClient.Dispose();
                    InnerClient = null;
                }

                IsDisposed = true;
            }
        }

        #endregion
    }
}
