using System;
using System.Globalization;
using KpNet.Common;
using Microsoft.Win32;

namespace KpNet.Hosting
{
    /// <summary>
    /// Class for registry settings storage.
    /// </summary>
    public sealed class RegistrySettingsStorage : ISettingsStorage
    {
        private const int DefaultProcessId = -1;
        
        private readonly string _registryKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="RegistrySettingsStorage"/> class.
        /// </summary>
        /// <param name="registryKey">The registry key.</param>
        public RegistrySettingsStorage(string registryKey)
        {
            Guard.ThrowIfNullOrEmpty(registryKey, "registryKey");

            _registryKey = registryKey;
        }

        /// <summary>
        /// Sets the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="processId">The process id.</param>
        public void SetProcessId(string key, int processId)
        {
            using (RegistryKey registryKey = GetRegistryKey())
            {
                registryKey.SetValue(key, processId.ToString(CultureInfo.InvariantCulture), RegistryValueKind.String);
            }
        }

        /// <summary>
        /// Gets the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The process id.</returns>
        public int GetProcessId(string key)
        {
            using (RegistryKey registryKey = GetRegistryKey())
            {
                string value = (string)registryKey.GetValue(key, String.Empty);

                if (string.IsNullOrEmpty(value))
                    return DefaultProcessId;

                return Int32.Parse(value, CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// Removes the process id.
        /// </summary>
        /// <param name="key">The key.</param>
        public void RemoveProcessId(string key)
        {
            using (RegistryKey registryKey = GetRegistryKey())
            {
                registryKey.DeleteValue(key);
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
