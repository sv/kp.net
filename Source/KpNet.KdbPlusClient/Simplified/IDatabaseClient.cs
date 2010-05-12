﻿using System;
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
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        DateTime Created { get; }

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        DbDataReader ExecuteQuery(string query, params object[] parameters);

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
        IMultipleResult ReceiveMultipleQueryResult();
    }
}