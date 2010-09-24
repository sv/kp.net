
namespace KpNet.Hosting
{
    public interface ISettingStorage
    {
        void SetProcessId(string key, int processId);

        int GetProcessId(string key);
    }
}
