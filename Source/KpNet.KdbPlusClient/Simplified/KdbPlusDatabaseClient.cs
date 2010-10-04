using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Contract class for database interactions.
    /// </summary>
    public abstract class KdbPlusDatabaseClient : IDatabaseClient
    {
        private bool _isDisposed;

        protected KdbPlusDatabaseClient()
        {
        }

        private static readonly KdbPlusDatabaseClientFactory _factory = new DefaultKdbPlusDatabaseClientFactory();

        /// <summary>
        /// disposes the client. Client can't be reused after dispose.
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public abstract TimeSpan SendTimeout { get; set; }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public abstract TimeSpan ReceiveTimeout { get; set; }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public abstract DateTime Created { get; }

        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        public abstract bool IsConnected { get; internal set; }

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        public abstract DbDataReader ExecuteQuery(string query, params object[] parameters);

        /// <summary>
        /// Executes the query returns data table.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>DataTable object</returns>
        public abstract DataTable ExecuteQueryAsDataTable(string query, params object[] parameters);

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public abstract void ExecuteNonQuery(string query, params object[] parameters);

        /// <summary>
        /// Executes the instruction that does not return results.
        /// Does not wait for the response - just puts the message into the tcp stack
        /// and exits.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public abstract void ExecuteOneWayNonQuery(string query, params object[] parameters);

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public abstract T ExecuteScalar<T>(string query, params object[] parameters) where T : struct;

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public abstract object ExecuteScalar(string query, params object[] parameters);

        public abstract IAsyncResult BeginExecuteScalar(string query, object[] parameters, AsyncCallback calback, object state);
        public abstract object EndExecuteScalar(IAsyncResult result);

        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public abstract IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters);

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <returns></returns>
        public abstract object Receive();

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public abstract T Receive<T>();

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public abstract DbDataReader ReceiveQueryResult();

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public abstract DataTable ReceiveQueryResultAsDataTable();

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public abstract IMultipleResult ReceiveMultipleQueryResult();

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public abstract string ConnectionString { get; }

        /// <summary>
        /// Gets a value indicating whether this instance is pooled.
        /// </summary>
        /// <value><c>true</c> if this instance is pooled; otherwise, <c>false</c>.</value>
        public virtual bool IsPooled
        {
            get { return false; }
        }

        /// <summary>
        /// Refreshes the connection pool if the instance is pooled.
        /// </summary>
        public virtual void RefreshPool()
        {
            throw new InvalidOperationException("This operation is invalid for this type of client.");
        }

        /// <summary>
        /// Gets the factory for creating clients.
        /// </summary>
        /// <value>The factory.</value>
        public static KdbPlusDatabaseClientFactory Factory
        {
            get { return _factory; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is disposed.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is disposed; otherwise, <c>false</c>.
        /// </value>
        protected bool IsDisposed
        {
            get { return _isDisposed; }
            set { _isDisposed = value; }
        }

        /// <summary>
        /// Throws exception if the instance is disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("Already disposed.");
        }
    }
}
