using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Round-robin client implementation.
    /// This client choose one random connection location and work with it. 
    /// </summary>
    internal sealed class BalancingKdbPlusDatabaseClient : KdbPlusDatabaseClient
    {
        private readonly KdbPlusDatabaseClient _innerClient;


        /// <summary>
        /// Initializes a new instance of the <see cref="BalancingKdbPlusDatabaseClient"/> class.
        /// </summary>
        /// <param name="dispatcher">The dispatcher.</param>
        public BalancingKdbPlusDatabaseClient(ConnectionDispatcher dispatcher)
        {
            Guard.ThrowIfNull(dispatcher, "dispatcher");
            _innerClient = Factory.CreateNewClient(dispatcher.GetRandomConnection());
        }
        

        #region IDatabaseClient Members

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public override TimeSpan SendTimeout
        {
            get { return _innerClient.SendTimeout; }
            set { _innerClient.SendTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public override TimeSpan ReceiveTimeout
        {
            get { return _innerClient.ReceiveTimeout; }
            set { _innerClient.ReceiveTimeout = value; }
        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public override DateTime Created
        {
            get { return _innerClient.Created; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        public override bool IsConnected
        {
            get
            {
                return _innerClient.IsConnected;
            }
            internal set { _innerClient.IsConnected = value; }
        }


        public override DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQuery(query, parameters);
        }

        public override DataTable ExecuteQueryAsDataTable(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQueryAsDataTable(query, parameters);
        }

        public override void ExecuteNonQuery(string query, params object[] parameters)
        {
            _innerClient.ExecuteNonQuery(query, parameters);
        }

        public override void ExecuteOneWayNonQuery(string query, params object[] parameters)
        {
            _innerClient.ExecuteOneWayNonQuery(query, parameters);
        }

        public override T ExecuteScalar<T>(string query, params object[] parameters)
        {
            return _innerClient.ExecuteScalar<T>(query, parameters);
        }

        public override object ExecuteScalar(string query, params object[] parameters)
        {
            return _innerClient.ExecuteScalar(query, parameters);
        }

        public override IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            return _innerClient.ExecuteQueryWithMultipleResult(query, parameters);
        }

        public override object Receive()
        {
            return _innerClient.Receive();
        }

        public override T Receive<T>()
        {
            return _innerClient.Receive<T>();
        }

        public override DbDataReader ReceiveQueryResult()
        {
            return _innerClient.ReceiveQueryResult();
        }

        public override DataTable ReceiveQueryResultAsDataTable()
        {
            return _innerClient.ReceiveQueryResultAsDataTable();
        }

        public override IMultipleResult ReceiveMultipleQueryResult()
        {
            return _innerClient.ReceiveMultipleQueryResult();
        }

        public override string ConnectionString
        {
            get { return _innerClient.ConnectionString; }
        }

        #endregion

        #region IDisposable Members

        public override void Dispose()
        {
            if(_innerClient != null)
            {
                _innerClient.Dispose();
            }
        }

        #endregion
    }
}
