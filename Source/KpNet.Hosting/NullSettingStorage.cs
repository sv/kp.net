namespace KpNet.Hosting
{
    public sealed class NullSettingStorage : ISettingStorage
    {
        private static readonly NullSettingStorage _instance = new NullSettingStorage();

        public static NullSettingStorage Instance
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
