using System;
using System.Data.Common;
using System.Globalization;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusConnectionStringBuilder : DbConnectionStringBuilder
    {
        private const string ServerKey = "Server";
        private const string PortKey = "Port";
        private const string UserKey = "User Id";
        private const string PasswordKey = "Password";
        private const string BufferSizeKey = "BufferSize";

        public KdbPlusConnectionStringBuilder(string connectionString)
        {
            Guard.ThrowIfNullOrEmpty(connectionString, "connectionString");
            ConnectionString = connectionString;
        }

        public string Server
        {
            get { return GetStringValue(ServerKey); }
            set { SetValue(ServerKey, value); }
        }

        public int Port
        {
            get { return GetIntValue(PortKey); }
            set { SetValue(PortKey, value.ToString()); }
        }

        public int BufferSize
        {
            get { return GetIntValue(BufferSizeKey); }
            set { SetValue(BufferSizeKey, value.ToString()); }
        }

        public string UserID
        {
            get { return GetStringValue(UserKey); }
            set { SetValue(UserKey, value); }
        }

        public string Password
        {
            get { return GetStringValue(PasswordKey); }
            set { SetValue(PasswordKey, value); }
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