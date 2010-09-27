using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace KpNet.Hosting
{
    /// <summary>
    /// Helper class which can start and kill processes.
    /// </summary>
    internal static class ProcessHelper
    {
        private const int OneMinute = 60 * 1000;

        [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        internal static extern void SetWindowText(IntPtr hWnd, string lpString);

        /// <summary>
        /// Starts the new process.
        /// </summary>
        /// <param name="processName">Name of the process.</param>
        /// <param name="workerDirectory">The worker directory.</param>
        /// <param name="commandLine">The command line.</param>
        /// <param name="title">The title.</param>
        /// <returns>The id of the created process.</returns>
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

        /// <summary>
        /// Kills the processes.
        /// </summary>
        /// <param name="processIds">The process ids.</param>
        /// <param name="processName">Name of the process.</param>
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
    }
}
