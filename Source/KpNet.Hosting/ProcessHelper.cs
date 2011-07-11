using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace KpNet.Hosting
{
    /// <summary>
    /// Helper class which can start and kill processes.
    /// </summary>
    internal static class ProcessHelper
    {
        public const int OneMinute = 60 * 1000;

        /// <summary>
        /// Starts the new process.
        /// </summary>
        /// <param name="processName">Name of the process.</param>
        /// <param name="workerDirectory">The worker directory.</param>
        /// <param name="commandLine">The command line.</param>
        /// <param name="title">The title.</param>
        /// <param name="hideWindow">To hide window or not</param>
        /// <param name="useShellExecute">To use shellExecute when starting the process</param>
        /// <param name="numberOfCoresToUse">Number of cores for process to use. 0 if there is no limit</param>
        /// <returns>The id of the created process.</returns>
        public static Process StartNewProcess(string processName, string workerDirectory, string commandLine, string title, bool hideWindow, bool useShellExecute, int numberOfCoresToUse)
        {
            if (numberOfCoresToUse < 0)
                throw new ArgumentException("Invalid number of cores");

            if (numberOfCoresToUse >= Environment.ProcessorCount)
                numberOfCoresToUse = 0;

            Process kdbProc = numberOfCoresToUse == 0 ? StartProcess(processName, workerDirectory, commandLine, useShellExecute, hideWindow) : StartProcessWithAffinity(processName, workerDirectory, commandLine, hideWindow, numberOfCoresToUse);

            Thread.Sleep(300);

            if (kdbProc.HasExited)
                throw new ProcessException(String.Format(Constants.DefaultCulture,
                                                         "Cannot start process {0} {1}. Process exited.",
                                                         processName, commandLine));

            if(!hideWindow)
                NativeMethods.SetWindowText(kdbProc, title);
            
            return kdbProc;
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

        public static bool IsStarted(int id, string processName)
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

            foreach (Process process in processes)
            {
                if(process.Id == id)
                {
                    return true;
                }
            }

            return false;
        }

        public static Process OpenExisting(int id, string processName)
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

            foreach (Process process in processes)
            {
                if (process.Id == id)
                {
                    process.EnableRaisingEvents = true;
                    return process;
                }
            }

            throw new ProcessException(string.Format("Could not find existing Kdb+ process. Name: {0}. Id: {1}.", processName, id));
        }

        private static Process StartProcessWithAffinity(string processName, string workerDirectory, string commandLine, bool hideWindow, int numberOfCoresToUse)
        {
            try
            {
                return NativeMethods.CreateProcessWithAffinity(processName, workerDirectory, commandLine, hideWindow,
                                                               numberOfCoresToUse);
            }
            catch (Exception ex)
            {
                throw new ProcessException(
                    String.Format(Constants.DefaultCulture, "Cannot start process {0} {1}.", processName,
                                  commandLine), ex);
            }
        }

        private static Process StartProcess(string processName, string workerDirectory, string commandLine, bool useShellExecute, bool hideWindow)
        {
            Process kdbProc = new Process
            {
                StartInfo =
                {
                    FileName = processName,
                    WorkingDirectory = Path.GetFullPath(workerDirectory),
                    Arguments = commandLine,
                    UseShellExecute = useShellExecute,
                    CreateNoWindow = hideWindow
                }
            };

            try
            {
                kdbProc.EnableRaisingEvents = true;

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
            return kdbProc;
        }
    }
}
