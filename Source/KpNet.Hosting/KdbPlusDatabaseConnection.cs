using System;
using System.Globalization;
using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    public sealed class KdbPlusDatabaseConnection : IDisposable
    {
        private const string TestConnectionCommand = @"0"; //ping q process - it should return 0 back
        private readonly IDatabaseClient _client;
        private const string ErrorInQuery = "ERROR";

        public KdbPlusDatabaseConnection(string host, int port)
        {
            _client = CreateClient(host, port);
        }

        #region IDisposable Members

        public void Dispose()
        {
            if (null != _client)
            {
                _client.Dispose();
            }
        }

        #endregion

        public object SendAsync(string query, params object[] parameters)
        {
            return SendSyncRequestReceiveAsyncResponse(_client, query, parameters);
        }

        public bool CheckAsync()
        {
            try
            {
                SendAsync(TestConnectionCommand);

                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool Check()
        {
            try
            {
                Send(TestConnectionCommand);

                return true;
            }
            catch
            {
                return false;
            }
        }

        public object Send(string query, params object[] parameters)
        {
            return Send(_client, query, parameters);
        }


        public static bool Check(string host, int port)
        {
            try
            {
                using (IDatabaseClient client = CreateClient(host, port))
                {
                    Send(client, TestConnectionCommand);

                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        public static bool CheckAsync(string host, int port)
        {
            try
            {
                using (IDatabaseClient client = CreateClient(host, port))
                {
                    SendSyncRequestReceiveAsyncResponse(client, TestConnectionCommand);

                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        private static IDatabaseClient CreateClient(string host, int port)
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder();

            builder.Port = port;

            builder.Server = host;

            KdbPlusDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNewClient(builder);

            client.SendTimeout = TimeSpan.FromMinutes(15);

            client.ReceiveTimeout = TimeSpan.FromMinutes(15);

            return client;
        }


        private static object SendSyncRequestReceiveAsyncResponse(IDatabaseClient client, string query,
                                                                  params object[] parameters)
        {
            client.ExecuteScalar(query, parameters);

            object result = client.Receive();

            CheckResult(result);

            return result;
        }

        private static object Send(IDatabaseClient client, string query, params object[] parameters)
        {
            object result = client.ExecuteScalar(query, parameters);

            CheckResult(result);

            return result;
        }

        private static void CheckResult(object result)
        {
            if (result != null)
            {
                string errorMessage = result as string;

                if (errorMessage != null && errorMessage.StartsWith(ErrorInQuery, StringComparison.OrdinalIgnoreCase))
                    throw new KdbPlusException(String.Format(CultureInfo.InvariantCulture, "Error occured during K+ query: '{0}'.", errorMessage.Replace(ErrorInQuery, String.Empty)));
            }
        }
    }
}
