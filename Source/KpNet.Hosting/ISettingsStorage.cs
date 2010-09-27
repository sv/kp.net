
namespace KpNet.Hosting
{
    /// <summary>
    /// Interface for the settings storage.
    /// </summary>
    public interface ISettingsStorage
    {
        /// <summary>
        /// Sets the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="processId">The process id.</param>
        void SetProcessId(string key, int processId);

        /// <summary>
        /// Gets the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The process id.</returns>
        int GetProcessId(string key);
    }
}
