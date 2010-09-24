using Microsoft.Win32;
using TR.Common;

namespace KpNet.Hosting
{
    public sealed class RegistrySettingStorage : ISettingStorage
    {
        private const int DefaultProcessId = -1;
        
        private readonly string _registryKey;

        public RegistrySettingStorage(string registryKey)
        {
            Guard.ThrowIfNullOrEmpty(registryKey, "registryKey");

            _registryKey = registryKey;
        }                

        public void SetProcessId(string key, int processId)
        {
            using (RegistryKey registryKey = GetRegistryKey())
            {
                registryKey.SetValue(key, processId, RegistryValueKind.DWord);
            }
        }

        public int GetProcessId(string key)
        {
            using (RegistryKey registryKey = GetRegistryKey())
            {
                return (int)registryKey.GetValue(key, DefaultProcessId);
            }
        }

        private RegistryKey GetRegistryKey()
        {
            RegistryKey regKey = Registry.LocalMachine.OpenSubKey(_registryKey, RegistryKeyPermissionCheck.ReadWriteSubTree);

            if (regKey == null)
            {
                Registry.LocalMachine.CreateSubKey(_registryKey);

                regKey = Registry.LocalMachine.OpenSubKey(_registryKey);
            }

            return regKey;
        }
    }
}
