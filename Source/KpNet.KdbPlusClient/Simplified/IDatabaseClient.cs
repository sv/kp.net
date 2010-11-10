using System;
using System.Data;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Contract for database interactions.
    /// </summary>
    public interface IDatabaseClient : IDisposable
    {
        /// <summary>
        /// Gets the port.
        /// </summary>
        /// <value>The port.</value>
        int Port { get; }

        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        TimeSpan SendTimeout { get; set; }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        TimeSpan ReceiveTimeout { get; set; }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        DateTime Created { get; }


        /// <summary>
        /// Gets a value indicating whether this instance is connected.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is connected; otherwise, <c>false</c>.
        /// </value>
        bool IsConnected
        { get;
        }

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        DbDataReader ExecuteQuery(string query, params object[] parameters);

        /// <summary>
        /// Executes the query returns data table.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>DataTable object</returns>
        DataTable ExecuteQueryAsDataTable(string query, params object[] parameters);

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        void ExecuteNonQuery(string query, params object[] parameters);

        /// <summary>
        /// Executes the instruction that does not return results.
        /// Does not wait for the response - just puts the message into the tcp stack
        /// and exits.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        void ExecuteOneWayNonQuery(string query, params object[] parameters);

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        T ExecuteScalar<T>(string query, params object[] parameters) where T : struct;

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        object ExecuteScalar(string query, params object[] parameters);

        /// <summary>
        /// Executes the query asynchronously in the special IO threadpool.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="calback">The calback.</param>
        /// <param name="state">The state.</param>
        /// <returns></returns>
        IAsyncResult BeginExecuteScalar(string query, object[] parameters, AsyncCallback calback, object state);

        /// <summary>
        /// Reseives the result of the query performed by BeginExecuteScalar.
        /// </summary>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        object EndExecuteScalar(IAsyncResult result);
        
        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters);

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <returns></returns>
        object Receive();

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        T Receive<T>();

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        DbDataReader ReceiveQueryResult();

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        DataTable ReceiveQueryResultAsDataTable();

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        IMultipleResult ReceiveMultipleQueryResult();

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        string ConnectionString { get; }

        /// <summary>
        /// Gets a value indicating whether this instance is pooled.
        /// </summary>
        /// <value><c>true</c> if this instance is pooled; otherwise, <c>false</c>.</value>
        bool IsPooled { get; }

        /// <summary>
        /// Refreshes the connection pool if the instance is pooled.
        /// </summary>
        void RefreshPool();
    }
}