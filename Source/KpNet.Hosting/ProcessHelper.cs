using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace KpNet.Hosting
{
    internal static class ProcessHelper
    {
        private const int OneMinute = 60 * 1000;

        [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern void SetWindowText(IntPtr hWnd, string lpString);

        public static int StartNewProcess(string processName, string workerDirectory, string commandLine, string title)
        {
            Process kdbProc = new Process
            {
                StartInfo =
                {
                    FileName = processName,
                    WorkingDirectory = Path.GetFullPath(workerDirectory),
                    Arguments = commandLine
                }
            };

            try
            {
                kdbProc.Start();
            }
            catch (ProcessException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ProcessException(
                    String.Format(Constants.DefaultCulture, "Cannot start process {0} {1}.", processName,
                                  commandLine), ex);
            }

            Thread.Sleep(300);

            if (kdbProc.HasExited)
                throw new ProcessException(String.Format(Constants.DefaultCulture,
                                                         "Cannot start process {0} {1}. Process exited.",
                                                         processName, commandLine));

            SetWindowText(kdbProc.MainWindowHandle, title);

            return kdbProc.Id;
        }

        public static void KillProcesses(IEnumerable<int> processIds, string processName)
        {
            Process[] processes;
            try
            {
                processes = Process.GetProcessesByName(processName);
            }
            catch (Exception ex)
            {
                throw new ProcessException("Could not get information about running Kdb+ processes.", ex);
            }

            List<int> ids = new List<int>(processIds);
            List<int> notKilledIds = new List<int>();
            List<Exception> exceptions = new List<Exception>();

            foreach (Process t in processes)
            {
                if (ids.Contains(t.Id))
                {
                    try
                    {
                        t.Kill();
                        t.WaitForExit(OneMinute);
                    }
                    catch (Exception ex)
                    {
                        notKilledIds.Add(t.Id);
                        exceptions.Add(ex);
                    }
                }
            }

            if (exceptions.Count > 0)
                throw new AggregateException(String.Format(Constants.DefaultCulture, "Kdb+ processes with Ids {0} couldn't be stopped.", FormatterHelper.FormatNumbers(notKilledIds)), exceptions);

        }

        public static bool Exists(int processId, string name)
        {
            try
            {
                Process process = Process.GetProcessById(processId);
                if (process.ProcessName == name)
                    return true;
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }
    }
}
