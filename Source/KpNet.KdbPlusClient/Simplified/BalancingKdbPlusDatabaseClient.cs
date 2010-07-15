using System;
using System.Data.Common;

namespace KpNet.KdbPlusClient.Simplified
{
    /// <summary>
    /// Round-robin client implementation.
    /// This client choose one random connection location and work with it. 
    /// </summary>
    public sealed class BalancingKdbPlusDatabaseClient : IDatabaseClient
    {
        private readonly IDatabaseClient _innerClient;
        private static int _index = -1;
        private static readonly object _locker = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="BalancingKdbPlusDatabaseClient"/> class.
        /// Only one of connection strings will be used.
        /// </summary>
        /// <param name="builders">The connection string builders.</param>
        public BalancingKdbPlusDatabaseClient(KdbPlusConnectionStringBuilder[] builders)
        {
            Guard.ThrowIfNull(builders, "builders");
            _innerClient = CreateDatabaseClient(builders);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BalancingKdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="connectionStrings">The connection strings.</param>
        public BalancingKdbPlusDatabaseClient(string[] connectionStrings)
        {
            Guard.ThrowIfNull(connectionStrings, "connectionStrings");
            _innerClient = CreateDatabaseClient(connectionStrings);
        }

        private static IDatabaseClient CreateDatabaseClient(KdbPlusConnectionStringBuilder[] builders)
        {
            lock(_locker)
            {
                return new PooledKdbPlusDatabaseClient(GetNext(builders));
            }
        }

        private static IDatabaseClient CreateDatabaseClient(string[] connectionStrings)
        {
            lock (_locker)
            {
                return new PooledKdbPlusDatabaseClient(GetNext(connectionStrings));
            }
        }

        private static T GetNext<T>(T[] elements)
        {
            _index++;

            return elements[_index % elements.Length];
        }

        #region IDatabaseClient Members

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public TimeSpan SendTimeout
        {
            get { return _innerClient.SendTimeout; }
            set { _innerClient.SendTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public TimeSpan ReceiveTimeout
        {
            get { return _innerClient.ReceiveTimeout; }
            set { _innerClient.ReceiveTimeout = value; }
        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public DateTime Created
        {
            get { return _innerClient.Created; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        public bool IsConnected
        {
            get
            {
                return _innerClient.IsConnected;
            }
        }


        public DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQuery(query, parameters);
        }

        public void ExecuteNonQuery(string query, params object[] parameters)
        {
            _innerClient.ExecuteNonQuery(query, parameters);
        }

        public void ExecuteOneWayNonQuery(string query, params object[] parameters)
        {
            _innerClient.ExecuteOneWayNonQuery(query, parameters);
        }

        public T ExecuteScalar<T>(string query, params object[] parameters) where T : struct
        {
            return _innerClient.ExecuteScalar<T>(query, parameters);
        }

        public object ExecuteScalar(string query, params object[] parameters)
        {
            return _innerClient.ExecuteScalar(query, parameters);
        }

        public IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQueryWithMultipleResult(query, parameters);
        }

        public object Receive()
        {
            return _innerClient.Receive();
        }

        public T Receive<T>()
        {
            return _innerClient.Receive<T>();
        }

        public DbDataReader ReceiveQueryResult()
        {
            return _innerClient.ReceiveQueryResult();
        }

        public IMultipleResult ReceiveMultipleQueryResult()
        {
            return _innerClient.ReceiveMultipleQueryResult();
        }

        public string ConnectionString
        {
            get { return _innerClient.ConnectionString; }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            if(_innerClient != null)
            {
                _innerClient.Dispose();
            }
        }

        #endregion
    }
}
