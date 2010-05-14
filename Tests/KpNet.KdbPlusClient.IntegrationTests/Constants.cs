namespace KpNet.KdbPlusClient.IntegrationTests
{
    public static class Constants
    {
        public const string Host = "localhost";
        public const int Port = 1001;
        public const string ConnectionString = "server=localhost;port=1001";
        public const string ConnectionStringNoPooling = "server=localhost;port=1001;pooling=false";
        public const string DescriptionMessage = "Q process should be started at localhost:1001";
    }
}
