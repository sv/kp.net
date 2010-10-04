
using System;
using System.Data;
using System.Data.Common;
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    internal abstract class DelegatingKdbPlusDatabaseClient : KdbPlusDatabaseClient
    {
        private KdbPlusDatabaseClient _innerClient;

        protected DelegatingKdbPlusDatabaseClient(KdbPlusDatabaseClient innerClient)
        {
            Guard.ThrowIfNull(innerClient, "innerClient");
            _innerClient = innerClient;
        }

        protected DelegatingKdbPlusDatabaseClient()
        {
        }

        internal KdbPlusDatabaseClient InnerClient
        {
            get { return _innerClient; }
            set { _innerClient = value; }
        }

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public override TimeSpan SendTimeout
        {
            get
            {
                ThrowIfDisposed();
                return _innerClient.SendTimeout;
            }
            set
            {
                ThrowIfDisposed();
                _innerClient.SendTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public override TimeSpan ReceiveTimeout
        {
            get
            {
                ThrowIfDisposed();
                return _innerClient.ReceiveTimeout;
            }
            set
            {
                ThrowIfDisposed();
                _innerClient.ReceiveTimeout = value;
            }
        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public override DateTime Created
        {
            get
            {
                ThrowIfDisposed();
                return _innerClient.Created;
            }
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
                ThrowIfDisposed();
                return _innerClient.IsConnected;
            }
            internal set
            {
                ThrowIfDisposed();
                _innerClient.IsConnected = value;
            }
        }


        public override DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            return _innerClient.ExecuteQuery(query, parameters);
        }

        public override DataTable ExecuteQueryAsDataTable(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            return _innerClient.ExecuteQueryAsDataTable(query, parameters);
        }

        public override void ExecuteNonQuery(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            _innerClient.ExecuteNonQuery(query, parameters);
        }

        public override void ExecuteOneWayNonQuery(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            _innerClient.ExecuteOneWayNonQuery(query, parameters);
        }

        public override T ExecuteScalar<T>(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            return _innerClient.ExecuteScalar<T>(query, parameters);
        }

        public override object ExecuteScalar(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            return _innerClient.ExecuteScalar(query, parameters);
        }

        public override IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            ThrowIfDisposed();
            return _innerClient.ExecuteQueryWithMultipleResult(query, parameters);
        }

        public override object Receive()
        {
            ThrowIfDisposed();
            return _innerClient.Receive();
        }

        public override T Receive<T>()
        {
            ThrowIfDisposed();
            return _innerClient.Receive<T>();
        }

        public override DbDataReader ReceiveQueryResult()
        {
            ThrowIfDisposed();
            return _innerClient.ReceiveQueryResult();
        }

        public override DataTable ReceiveQueryResultAsDataTable()
        {
            ThrowIfDisposed();
            return _innerClient.ReceiveQueryResultAsDataTable();
        }

        public override IMultipleResult ReceiveMultipleQueryResult()
        {
            ThrowIfDisposed();
            return _innerClient.ReceiveMultipleQueryResult();
        }

        public override string ConnectionString
        {
            get
            {
                ThrowIfDisposed();
                return _innerClient.ConnectionString;
            }
        }

        public override IAsyncResult BeginExecuteScalar(string query, object[] parameters, AsyncCallback calback, object state)
        {
            ThrowIfDisposed();
            return _innerClient.BeginExecuteScalar(query, parameters, calback, state);
        }

        public override object EndExecuteScalar(IAsyncResult result)
        {
            ThrowIfDisposed();

            return _innerClient.EndExecuteScalar(result);
        }
    }
}
