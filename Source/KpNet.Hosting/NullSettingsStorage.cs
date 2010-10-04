namespace KpNet.Hosting
{
    /// <summary>
    /// Singleton class for null settings storage.
    /// </summary>
    public sealed class NullSettingsStorage : ISettingsStorage
    {
        private static readonly NullSettingsStorage _instance = new NullSettingsStorage();

        /// <summary>
        /// Gets the singleton instance of NullSettingsStorage.
        /// </summary>
        /// <value>The instance of NullSettingsStorage.</value>
        public static NullSettingsStorage Instance
        {
            get { return _instance; }
        }

        /// <summary>
        /// Sets the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="processId">The process id.</param>
        public void SetProcessId(string key, int processId)
        {
            // Do nothing.
        }

        /// <summary>
        /// Gets the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The process id.</returns>
        public int GetProcessId(string key)
        {
            return -1;
        }


        /// <summary>
        /// Removes the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        public void RemoveProcessId(string key)
        {
            // Do nothing.
        }
    }
}
