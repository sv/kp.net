using System;
using System.Data.Common;
using System.Globalization;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Connection string builder for KDB+.
    /// </summary>
    public sealed class KdbPlusConnectionStringBuilder : DbConnectionStringBuilder
    {
        private const string ServerKey = "Server";
        private const string PortKey = "Port";
        private const string UserKey = "User Id";
        private const string PasswordKey = "Password";
        private const string BufferSizeKey = "BufferSize";
        private const string PoolingKey = "Pooling";
        private const string MinPoolSizeKey = "Min Pool Size";
        private const string MaxPoolSizeKey = "Max Pool Size";
        private const string LoadBalanceTimeoutKey = "Load Balance Timeout";


        public const int DefaultBufferSize = 16384;
        public const int DefaultMinPoolSize = 0;
        public const int DefaultMaxPoolSize = 100;
        public const int DefaultLoadBalanceTimeoutSeconds = 0;
        public const bool DefaultPooling = true;


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
        /// Gets or sets the server.
        /// </summary>
        /// <value>The server.</value>
        public string Server
        {
            get { return GetStringValue(ServerKey); }
            set { SetValue(ServerKey, value); }
        }

        /// <summary>
        /// Gets or sets the port.
        /// </summary>
        /// <value>The port.</value>
        public int Port
        {
            get { return GetIntValue(PortKey); }
            set { SetValue(PortKey, value.ToString()); }
        }

        /// <summary>
        /// Gets or sets the size of the buffer.
        /// </summary>
        /// <value>The size of the buffer.</value>
        public int BufferSize
        {
            get
            {
                return GetValueOrDefault(BufferSizeKey, DefaultBufferSize);
            }
            set { SetValue(BufferSizeKey, value.ToString()); }
        }

        /// <summary>
        /// Gets or sets the user ID.
        /// </summary>
        /// <value>The user ID.</value>
        public string UserID
        {
            get { return GetValueOrDefault(UserKey, String.Empty); }
            set { SetValue(UserKey, value); }
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        /// <value>The password.</value>
        public string Password
        {
            get { return GetValueOrDefault(PasswordKey,String.Empty); }
            set { SetValue(PasswordKey, value); }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="KdbPlusConnectionStringBuilder"/> is pooling.
        /// </summary>
        /// <value><c>true</c> if pooling; otherwise, <c>false</c>.</value>
        public bool Pooling
        {
            get
            {
                return GetValueOrDefault(PoolingKey, DefaultPooling);
            }
            set
            {
                SetValue(PoolingKey, value.ToString());
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
                return GetValueOrDefault(MinPoolSizeKey, DefaultMinPoolSize);
            }
            set
            {
                SetValue(MinPoolSizeKey, value.ToString());
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
                return GetValueOrDefault(LoadBalanceTimeoutKey, DefaultLoadBalanceTimeoutSeconds);
            }
            set
            {
                SetValue(LoadBalanceTimeoutKey, value.ToString());
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
                return GetValueOrDefault(MaxPoolSizeKey, DefaultMaxPoolSize);
            }
            set
            {
                SetValue(MaxPoolSizeKey, value.ToString());
            }
        }

        private int GetValueOrDefault(string key, int defaultValue)
        {
            object result;
            if (!TryGetValue(key, out result))
                return defaultValue;

            return Int32.Parse((string)result);
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

        private string GetStringValue(string key)
        {
            return (string) GetValue(key);
        }

        private int GetIntValue(string key)
        {
            return Int32.Parse(GetStringValue(key));
        }

        private object GetValue(string key)
        {
            object result;
            if (!TryGetValue(key, out result))
                throw new InvalidOperationException(String.Format(CultureInfo.InvariantCulture,
                                                                  "Connection string does not contain key '{0}'.", key));

            return result;
        }
        
        private void SetValue(string key, string value)
        {
            this[key] = value;
        }
    }
}