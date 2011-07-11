using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace KpNet.Hosting
{
    internal static class NativeMethods
    {
        #region Native Structs and Methods

        private const uint CREATE_NEW_CONSOLE = 0x00000010;
        private const uint CREATE_NO_WINDOW = 0x08000000;
        private const uint CREATE_SUSPENDED = 0x00000004;

        internal struct PROCESS_INFORMATION
        {
            public IntPtr hProcess;
            public IntPtr hThread;
            public uint dwProcessId;
            public uint dwThreadId;
        }

        internal struct STARTUPINFO
        {
            public uint cb;
            public string lpReserved;
            public string lpDesktop;
            public string lpTitle;
            public uint dwX;
            public uint dwY;
            public uint dwXSize;
            public uint dwYSize;
            public uint dwXCountChars;
            public uint dwYCountChars;
            public uint dwFillAttribute;
            public uint dwFlags;
            public short wShowWindow;
            public short cbReserved2;
            public IntPtr lpReserved2;
            public IntPtr hStdInput;
            public IntPtr hStdOutput;
            public IntPtr hStdError;
        }

        internal struct SECURITY_ATTRIBUTES
        {
            public int length;
            public IntPtr lpSecurityDescriptor;
            public bool bInheritHandle;
        }
        
        [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern void SetWindowText(IntPtr hWnd, string lpString);

        [DllImport("kernel32.dll")]
        static extern bool CreateProcess(string lpApplicationName, string lpCommandLine, IntPtr lpProcessAttributes, IntPtr lpThreadAttributes,
                                bool bInheritHandles, uint dwCreationFlags, IntPtr lpEnvironment,
                                string lpCurrentDirectory, ref STARTUPINFO lpStartupInfo, out PROCESS_INFORMATION lpProcessInformation);

        [DllImport("kernel32.dll")]
        static extern uint ResumeThread(IntPtr hThread);

        #endregion

        public static void SetWindowText(Process process, string text)
        {
            if(!String.IsNullOrEmpty(text))
                SetWindowText(process.MainWindowHandle,text);
        }

        public static Process CreateProcessWithAffinity(string processName, string workerDirectory, string commandLine, bool hideWindow, int numberOfCoresToUse)
        {
            STARTUPINFO si = new STARTUPINFO();
            PROCESS_INFORMATION pi;

            string fullCommandLine = String.Concat(processName, " ", commandLine);
            workerDirectory = Path.GetFullPath(workerDirectory);

            uint startFlag = CREATE_NEW_CONSOLE | CREATE_SUSPENDED;

            if(hideWindow)
                startFlag = CREATE_NO_WINDOW | CREATE_SUSPENDED;

            if (CreateProcess(null, fullCommandLine, IntPtr.Zero, IntPtr.Zero, false, startFlag, IntPtr.Zero, workerDirectory, ref si, out pi))
            {
                Process process = Process.GetProcessById(Convert.ToInt32(pi.dwProcessId));
                process.EnableRaisingEvents = true;

                // calculate affinity
                // e.g: 2 cores = 2^2 - 1 = 3.
                int affinity = Convert.ToInt32(Math.Pow(2, numberOfCoresToUse)-1);
                process.ProcessorAffinity = (IntPtr)affinity;

                // resume suspended process
                // by resuming main thread
                uint result = ResumeThread(pi.hThread);

                // if process was not resumed
                // throw
                if (result == 0)
                    ThrowWin32Error("Failed to resume suspended process.");
                
                return process;
            }
            // if process was not created
            // throw
            ThrowWin32Error("Unknown win32 error on starting suspended process.");

            return null;
        }

        private static void ThrowWin32Error(string description)
        {
            int error = Marshal.GetLastWin32Error();
            Exception ex = Marshal.GetExceptionForHR(error);

            if (ex != null)
                throw ex;

            throw new ProcessException(description);
        }
    }
}
