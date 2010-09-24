
namespace KpNet.Hosting
{
    public interface ISettingsStorage
    {
        void SetProcessId(string key, int processId);

        int GetProcessId(string key);
    }
}
