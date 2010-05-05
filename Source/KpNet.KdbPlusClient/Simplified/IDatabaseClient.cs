using System;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Contract for database interactions.
    /// </summary>
    public interface IDatabaseClient : IDisposable
    {
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
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        DbDataReader ExecuteQuery(string query);

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        void ExecuteNonQuery(string query);

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        T ExecuteScalar<T>(string query) where T : struct;

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        object ExecuteScalar(string query);

        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        IMultipleResult ExecuteQueryWithMultipleResult(string query);
    }
}