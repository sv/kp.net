using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using KpNet.Common;
using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    /// <summary>
    /// Class for the composite database client.
    /// </summary>
    public sealed class CompositeDatabaseClient : IDatabaseClient
    {
        private readonly List<IDatabaseClient> _innerClients;
        private readonly DateTime _created;
        private bool _pollAllInnerClients;

        /// <summary>
        /// Initializes a new instance of the <see cref="CompositeDatabaseClient"/> class.
        /// </summary>
        /// <param name="clients">The clients.</param>
        public CompositeDatabaseClient(IEnumerable<IDatabaseClient> clients)
        {
            Guard.ThrowIfNull(clients);

            _innerClients = new List<IDatabaseClient>(clients);

            _created = DateTime.Now;

            _pollAllInnerClients = true;
        }

        /// <summary>
        /// Gets or sets a value indicating whether [poll all inner clients].
        /// </summary>
        /// <value>
        /// 	<c>true</c> if [poll all inner clients]; otherwise, <c>false</c>.
        /// </value>
        public bool PollAllInnerClients
        {
            get { return _pollAllInnerClients; }
            set { _pollAllInnerClients = value; }
        }

        /// <summary>
        /// Gets the port.
        /// </summary>
        /// <value>The port.</value>
        public int Port
        {
            get { throw new InvalidOperationException("This operation is invalid for this type of client."); }
        }

        public List<int> Ports
        {
            get
            {
                List<int> ports = new List<int>();

                foreach (IDatabaseClient client in _innerClients)
                {
                    ports.Add(client.Port);
                }

                return ports;
            }
        }
        /// <summary>
        /// Gets or sets the send timeout.
        /// </summary>
        /// <value>The send timeout.</value>
        public TimeSpan SendTimeout
        {
            get
            {
                return _innerClients[0].SendTimeout;
            }
            set
            {
                foreach(IDatabaseClient client in _innerClients)
                {
                    client.SendTimeout = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the receive timeout.
        /// </summary>
        /// <value>The receive timeout.</value>
        public TimeSpan ReceiveTimeout
        {
            get
            {
                return _innerClients[0].ReceiveTimeout;
            }
            set
            {
                foreach (IDatabaseClient client in _innerClients)
                {
                    client.ReceiveTimeout = value;
                }
            }

        }

        /// <summary>
        /// Gets the date and time when client was created.
        /// </summary>
        /// <value>The creation date.</value>
        public DateTime Created
        {
            get { return _created; }
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
                foreach (IDatabaseClient client in _innerClients)
                {
                    if (!client.IsConnected) return false;
                }

                return true;
            }
        }

        /// <summary>
        /// Executes the query and returns the result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns>DbDataReader object</returns>
        public DbDataReader ExecuteQuery(string query, params object[] parameters)
        {
            DbDataReader result = _innerClients[0].ExecuteQuery(query, parameters);

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ExecuteQuery(query, parameters);
                }
            }

            return result;
        }

        /// <summary>
        /// Executes the query returns data table.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>DataTable object</returns>
        public DataTable ExecuteQueryAsDataTable(string query, params object[] parameters)
        {
            DataTable result = _innerClients[0].ExecuteQueryAsDataTable(query, parameters);

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ExecuteQueryAsDataTable(query, parameters);
                }
            }

            return result;
        }

        /// <summary>
        /// Executes the instruction that does not return results.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public void ExecuteNonQuery(string query, params object[] parameters)
        {
            foreach (IDatabaseClient client in _innerClients)
            {
                client.ExecuteNonQuery(query, parameters);
            }
        }

        /// <summary>
        /// Executes the instruction that does not return results.
        /// Does not wait for the response - just puts the message into the tcp stack
        /// and exits.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        public void ExecuteOneWayNonQuery(string query, params object[] parameters)
        {
            foreach (IDatabaseClient client in _innerClients)
            {
                client.ExecuteOneWayNonQuery(query, parameters);
            }
        }

        /// <summary>
        /// Executes the scalar.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public T ExecuteScalar<T>(string query, params object[] parameters) where T : struct
        {
            T result = _innerClients[0].ExecuteScalar<T>(query, parameters);

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ExecuteScalar<T>(query, parameters);
                }
            }

            return result;
        }

        /// <summary>
        /// Executes the query that returns scalar value.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public object ExecuteScalar(string query, params object[] parameters)
        {
            object result = _innerClients[0].ExecuteScalar(query, parameters);

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ExecuteScalar(query, parameters);
                }
            }

            return result;
        }

        public IAsyncResult BeginExecuteScalar(string query, object[] parameters, AsyncCallback calback, object state)
        {
            throw new NotImplementedException();
        }

        public object EndExecuteScalar(IAsyncResult result)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Executes the query with multiple result.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The query parameters</param>
        /// <returns></returns>
        public IMultipleResult ExecuteQueryWithMultipleResult(string query, params object[] parameters)
        {
            IMultipleResult result = _innerClients[0].ExecuteQueryWithMultipleResult(query, parameters);

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ExecuteQueryWithMultipleResult(query, parameters);
                }
            }

            return result;
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <returns></returns>
        public object Receive()
        {
            object result = _innerClients[0].Receive();

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].Receive();
                }
            }

            return result;
        }

        /// <summary>
        /// Receives result from server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T Receive<T>()
        {
            T result = _innerClients[0].Receive<T>();

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].Receive<T>();
                }
            }

            return result;
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public DbDataReader ReceiveQueryResult()
        {
            DbDataReader result = _innerClients[0].ReceiveQueryResult();

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ReceiveQueryResult();
                }
            }

            return result;
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public DataTable ReceiveQueryResultAsDataTable()
        {
            DataTable result = _innerClients[0].ReceiveQueryResultAsDataTable();

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ReceiveQueryResultAsDataTable();
                }
            }

            return result;
        }

        /// <summary>
        /// Receives the query result from server.
        /// </summary>
        /// <returns></returns>
        public IMultipleResult ReceiveMultipleQueryResult()
        {
            IMultipleResult result = _innerClients[0].ReceiveMultipleQueryResult();

            if (PollAllInnerClients)
            {
                for (int i = 1; i < _innerClients.Count; i++)
                {
                    _innerClients[i].ReceiveMultipleQueryResult();
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public string ConnectionString
        {
            get { return _innerClients[0].ConnectionString; }
        }

        public bool IsPooled
        {
            get { return false; }
        }

        public void RefreshPool()
        {
            throw new InvalidOperationException("This operation is invalid for this type of client.");
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            foreach (IDatabaseClient client in _innerClients)
            {
                client.Dispose();
            }
        }
    }
}
