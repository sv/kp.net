namespace KpNet.Hosting
{
    public sealed class NullSettingsStorage : ISettingsStorage
    {
        private static readonly NullSettingsStorage _instance = new NullSettingsStorage();

        public static NullSettingsStorage Instance
        {
            get { return _instance; }
        }

        public void SetProcessId(string key, int processId)
        {
            // Do nothing.
        }

        public int GetProcessId(string key)
        {
            return -1;
        }
    }
}
