﻿using System;
using System.Data.Common;
using KpNet.Common;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Connection string builder for KDB+.
    /// </summary>
    public sealed class KdbPlusConnectionStringBuilder : DbConnectionStringBuilder, IEquatable<KdbPlusConnectionStringBuilder>
    {
        private const string ServerKey = "Server";
        private const string PortKey = "Port";
        private const string UserKey = "User Id";
        private const string PasswordKey = "Password";
        private const string BufferSizeKey = "Buffer Size";
        private const string PoolingKey = "Pooling";
        private const string MinPoolSizeKey = "Min Pool Size";
        private const string MaxPoolSizeKey = "Max Pool Size";
        private const string LoadBalanceTimeoutKey = "Load Balance Timeout";
        private const string InactivityTimeoutKey = "Inactivity Timeout";
        private const string ReceiveTimeoutKey = "Receive Timeout";
        private const string SendTimeoutKey = "Send Timeout";

        public const int DefaultBufferSize = 16384;
        public const int DefaultMinPoolSize = 0;
        public const int DefaultMaxPoolSize = 100;
        public const int DefaultLoadBalanceTimeoutSeconds = 0;
        public const int DefaultInactivityTimeoutSeconds = 0;
        public const bool DefaultPooling = true;
        public static readonly TimeSpan DefaultSendTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultReceiveTimeout = TimeSpan.FromMinutes(5);


        private string _server;
        private int _port;
        private string _user;
        private string _password;
        private int _bufferSize;
        private bool _pooling;
        private int _minPoolSize;
        private int _maxPoolSize;
        private int _loadBalanceTimeout;
        private int _inactivityTimeout;
        private int _hashCode;
        private TimeSpan _receiveTimeout;
        private TimeSpan _sendTimeout;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusConnectionStringBuilder"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public KdbPlusConnectionStringBuilder(string connectionString)
        {
            Guard.ThrowIfNullOrEmpty(connectionString, "connectionString");
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusConnectionStringBuilder"/> class.
        /// </summary>
        public KdbPlusConnectionStringBuilder()
        {
            Init();
        }

        public new string  ConnectionString
        {
            get
            {
                return base.ConnectionString;
            }
            set 
            {
                base.ConnectionString = value;
                Init();
            }
        }

        public override object this[string keyword]
        {
            get
            {
                return base[keyword];
            }
            set
            {
                base[keyword] = value;
                Init();
            }
        }

        public override void Clear()
        {
            base.Clear();
            Init();
        }

        /// <summary>
        /// Gets or sets the server.
        /// </summary>
        /// <value>The server.</value>
        public string Server
        {
            get
            {
                return _server;
            }
            set 
            {
                SetValue(ServerKey, value);
                _server = value;
            }
        }

        /// <summary>
        /// Gets or sets the port.
        /// </summary>
        /// <value>The port.</value>
        public int Port
        {
            get
            {
                return _port;
            }
            set
            {
                SetValue(PortKey, value.ToString());
                _port = value;
            }
        }

        /// <summary>
        /// Gets or sets the size of the buffer.
        /// </summary>
        /// <value>The size of the buffer.</value>
        public int BufferSize
        {
            get
            {
                return _bufferSize;
            }
            set
            {
                SetValue(BufferSizeKey, value.ToString());
                _bufferSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the user ID.
        /// </summary>
        /// <value>The user ID.</value>
        public string UserID
        {
            get
            {
                return _user;
            }
            set
            {
                SetValue(UserKey, value);
                _user = value;
            }
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        /// <value>The password.</value>
        public string Password
        {
            get
            {
                return _password;
            }
            set 
            { 
                SetValue(PasswordKey, value);
                _password = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="KdbPlusConnectionStringBuilder"/> is pooling.
        /// </summary>
        /// <value><c>true</c> if pooling; otherwise, <c>false</c>.</value>
        public bool Pooling
        {
            get
            {
                return _pooling;
            }
            set
            {
                SetValue(PoolingKey, value.ToString());
                _pooling = value;
            }
        }

        /// <summary>
        /// Gets or sets the size of the min pool.
        /// </summary>
        /// <value>The size of the min pool.</value>
        public int MinPoolSize
        {
            get
            {
                return _minPoolSize;
            }
            set
            {
                SetValue(MinPoolSizeKey, value.ToString());
                _minPoolSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the load balance timeout.
        /// </summary>
        /// <value>The load balance timeout.</value>
        public int LoadBalanceTimeout
        {
            get
            {
                return _loadBalanceTimeout;
            }
            set
            {
                SetValue(LoadBalanceTimeoutKey, value.ToString());
                _loadBalanceTimeout = value;
            }
        }


        /// <summary>
        /// Gets or sets the connection lazy timeout.
        /// </summary>
        /// <value>The connection lazy timeout.</value>
        public int InactivityTimeout
        {
            get
            {
                return _inactivityTimeout;
            }
            set
            {
                SetValue(InactivityTimeoutKey, value.ToString());
                _inactivityTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets the size of the max pool.
        /// </summary>
        /// <value>The size of the max pool.</value>
        public int MaxPoolSize
        {
            get
            {
                return _maxPoolSize;
            }
            set
            {
                SetValue(MaxPoolSizeKey, value.ToString());
                _maxPoolSize = value;
            }
        }

        public TimeSpan ReceiveTimeout
        {
            get { return _receiveTimeout; }
            set
            {
                SetValue(ReceiveTimeoutKey, (Convert.ToInt32(value.TotalSeconds)).ToString());
                _receiveTimeout = value;
            }
        }

        public TimeSpan SendTimeout
        {
            get { return _sendTimeout; }
            set
            {
                SetValue(SendTimeoutKey, (Convert.ToInt32(value.TotalSeconds)).ToString());
                _sendTimeout = value;
            }
        }


        private void Init()
        {
            _server = GetValueOrDefault(ServerKey, String.Empty);
            _port = GetValueOrDefault(PortKey,-1);
            _bufferSize = GetValueOrDefault(BufferSizeKey, DefaultBufferSize);
            _user = GetValueOrDefault(UserKey, String.Empty);
            _password = GetValueOrDefault(PasswordKey, String.Empty);
            _pooling = GetValueOrDefault(PoolingKey, DefaultPooling);
            _minPoolSize = GetValueOrDefault(MinPoolSizeKey, DefaultMinPoolSize);
            _loadBalanceTimeout = GetValueOrDefault(LoadBalanceTimeoutKey, DefaultLoadBalanceTimeoutSeconds);
            _inactivityTimeout = GetValueOrDefault(InactivityTimeoutKey, DefaultInactivityTimeoutSeconds);
            _maxPoolSize = GetValueOrDefault(MaxPoolSizeKey, DefaultMaxPoolSize);
            _receiveTimeout = GetValueOrDefault(ReceiveTimeoutKey, DefaultReceiveTimeout);
            _sendTimeout = GetValueOrDefault(SendTimeoutKey, DefaultSendTimeout);

            _hashCode = ConnectionString.ToLowerInvariant().GetHashCode();
            
        }

        private int GetValueOrDefault(string key, int defaultValue)
        {
            object result;
            if (!TryGetValue(key, out result))
                return defaultValue;

            return Int32.Parse((string)result);
        }

        private TimeSpan GetValueOrDefault(string key, TimeSpan defaultValue)
        {
            object result;
            if (!TryGetValue(key, out result))
                return defaultValue;

            return TimeSpan.FromSeconds(Int32.Parse((string)result));
        }

        private bool GetValueOrDefault(string key, bool defaultValue)
        {
            object result;
            if (!TryGetValue(key, out result))
                return defaultValue;

            return bool.Parse((string)result);
        }

        private string GetValueOrDefault(string key, string defaultValue)
        {
            object result;
            if (!TryGetValue(key, out result))
                return defaultValue;

            return (string)result;
        }

        
        private void SetValue(string key, string value)
        {
            this[key] = value;
        }


        public bool Equals(KdbPlusConnectionStringBuilder other)
        {
            if (other == null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return (String.Equals(ConnectionString, other.ConnectionString, StringComparison.OrdinalIgnoreCase));
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as KdbPlusConnectionStringBuilder);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }
    }
}