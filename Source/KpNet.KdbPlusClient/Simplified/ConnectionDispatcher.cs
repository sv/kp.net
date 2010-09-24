
using System;
using System.Collections.Generic;
using System.Linq;
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Class for dispatching connections.
    /// </summary>
    public sealed class ConnectionDispatcher
    {
        private readonly List<KdbPlusConnectionStringBuilder> _connections;
        private int _currentIndex;
        private readonly object _lock = new object();
        private readonly int _maxValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionDispatcher"/> class.
        /// </summary>
        /// <param name="connectionStrings">The connection strings.</param>
        public ConnectionDispatcher(ICollection<string> connectionStrings):this(StringsToBuilders(connectionStrings))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionDispatcher"/> class.
        /// </summary>
        /// <param name="connections">The connections.</param>
        public ConnectionDispatcher(ICollection<KdbPlusConnectionStringBuilder> connections)
        {
            Guard.ThrowIfNull(connections, "connections");

            if(connections.Count == 0)
                throw new ArgumentException("Incorrect connections count");

            _connections = new List<KdbPlusConnectionStringBuilder>(connections);
            _maxValue = _connections.Count;
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns></returns>
        public KdbPlusConnectionStringBuilder GetRandomConnection()
        {
            lock(_lock)
            {
                KdbPlusConnectionStringBuilder builder = _connections[_currentIndex];

                _currentIndex = (_currentIndex + 1)%_maxValue;

                return builder;
            }
        }

        private static ICollection<KdbPlusConnectionStringBuilder> StringsToBuilders(ICollection<string> connectionStrings)
        {
            Guard.ThrowIfNull(connectionStrings, "connectionStrings");

            return connectionStrings.Select(connectionString => new KdbPlusConnectionStringBuilder(connectionString)).ToList();
        }
    }
}
