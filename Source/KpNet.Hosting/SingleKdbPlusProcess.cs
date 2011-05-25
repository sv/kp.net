using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using KpNet.Common;
using KpNet.KdbPlusClient;


namespace KpNet.Hosting
{
    /// <summary>
    /// Class for the usual Kdb process.
    /// </summary>
    internal sealed class SingleKdbPlusProcess : KdbPlusProcess
    {
        private const string TestConnectionCommand = @"0"; //ping q process - it should return 0 back
        private const string ErrorInQuery = "ERROR";
        private const int NoProcessId = -1;
        private readonly TimeSpan _idleTime = TimeSpan.FromMilliseconds(200);

        private readonly string _processName;
        private readonly string _host;
        private int _port;
        private readonly string _workingDirectory;
        private readonly ILogger _logger;
        private readonly ISettingsStorage _storage;
        private readonly string _commandLine;
        private readonly string _processTitle;
        private readonly List<Action<IDatabaseClient>> _setupCommands;
        private readonly List<Action> _preStartCommands;

        private readonly object _locker = new object();
        private int _id;
        private string _processKey;
        private readonly bool _hideWindow;

        private volatile bool _isAlive;
        private Process _process;

        private readonly TimeSpan _waitForPortTimeout;

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleKdbPlusProcess"/> class.
        /// </summary>
        /// <param name="processName">Name of the process.</param>
        /// <param name="host">The host.</param>
        /// <param name="port">The port.</param>
        /// <param name="commandLine">The command line.</param>
        /// <param name="processTitle">The process title.</param>
        /// <param name="workingDirectory">The working directory.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="storage">The storage.</param>
        /// <param name="preStartCommands">The pre-start commands.</param>
        /// <param name="setupCommands">The setup commands.</param>
        /// <param name="hideWindow">if set to <c>true</c> [hide window].</param>
        /// <param name="waitForPortTimeOut">The wait for port time out.</param>
        public SingleKdbPlusProcess(string processName, string host, 
                                    int port, string commandLine, string processTitle,
                                    string workingDirectory, ILogger logger, 
                                    ISettingsStorage storage,
                                    List<Action> preStartCommands,
                                    List<Action<IDatabaseClient>> setupCommands, bool hideWindow,
                                    TimeSpan waitForPortTimeOut)
        {
            Guard.ThrowIfNull(logger, "logger");
            Guard.ThrowIfNull(storage, "storage");
            Guard.ThrowIfNull(setupCommands, "setupCommands");
            Guard.ThrowIfNull(preStartCommands, "preStartCommands");
            Guard.ThrowIfNullOrEmpty(processName, "processName");
            Guard.ThrowIfNullOrEmpty(host, "host");
            Guard.ThrowIfNullOrEmpty(workingDirectory, "workingDirectory");
            Guard.ThrowIfNullOrEmpty(commandLine, "commandLine");

            _logger = logger;
            _storage = storage;
            _processName = processName;
            _host = host;
            _port = port;
            _workingDirectory = workingDirectory;
            _commandLine = commandLine;
            
            _processTitle = processTitle;

            _processKey = string.Format("{0}_{1}", _host, _port);
            _id = _storage.GetProcessId(_processKey);
            
            if(string.IsNullOrWhiteSpace(_processTitle))
            {
                _processTitle = _processKey;
            }

            _setupCommands = setupCommands;
            _preStartCommands = preStartCommands;

            _hideWindow = hideWindow;

            _waitForPortTimeout = waitForPortTimeOut;
        }

        /// <summary>
        /// Restarts this instance.
        /// </summary>
        public override bool Restart(out IDatabaseClient client)
        {
            client = null;

            lock(_locker)
            {
                if (!IsStarted())
                {
                    Start();

                    client = GetConnection();

                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Opens the existing.
        /// </summary>
        public override void OpenExisting()
        {
            lock(_locker)
            {
                try
                {
                    _process = OpenExistingProcess();

                    _process.Exited += ProcessExited;

                    _isAlive = true;

                    _id = _process.Id;
                }
                catch (Exception exc)
                {
                    _logger.Error("Failed to open existing db process.", exc);

                    throw;
                }                
            }
        }


        /// <summary>
        /// Starts this instance.
        /// </summary>
        public override void Start()
        {
            lock (_locker)
            {
                try
                {
                    KillProcessById(_id);

                    _process = StartProcess();

                    _process.Exited += ProcessExited;

                    _isAlive = !_process.HasExited;

                    _id = _process.Id;

                    _storage.SetProcessId(_processKey, _id);
                }
                catch (Exception ex)
                {
                    _logger.Error("Failed to start db process.", ex);

                    throw;
                }
            }
        }

        /// <summary>
        /// Kills this instance.
        /// </summary>
        public override void Kill()
        {
            lock (_locker)
            {
                KillProcess(_process);
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is alive.
        /// </summary>
        /// <value><c>true</c> if this instance is alive; otherwise, <c>false</c>.</value>
        public override bool IsAlive
        {
            get
            {
                return _isAlive;
            }
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns>Connection.</returns>
        public override IDatabaseClient GetConnection()
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder {Port = _port, Server = _host};

            KdbPlusDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNonPooledClient(builder);

            client.SendTimeout = TimeSpan.FromMinutes(15);

            client.ReceiveTimeout = TimeSpan.FromMinutes(15);

            return client;
        }                

        

        
        /// <summary>
        /// Sets the port.
        /// </summary>
        /// <param name="port">The port.</param>
        public override void SetPort(int port)
        {
            using (IDatabaseClient client = GetConnection())
            {
                _port = port;

                UpdateSettingsStorage();

                string command = string.Format(@"\p {0}", port);

                client.ExecuteScalar(command);
            }
        }

        /// <summary>
        /// Loads the directory.
        /// </summary>
        /// <param name="path">The path.</param>
        public override void LoadDirectory(string path)
        {
            Load(path);
        }

        /// <summary>
        /// Loads the file.
        /// </summary>
        /// <param name="path">The path.</param>
        public override void LoadFile(string path)
        {
            Load(path);
        }

        private void Load(string path)
        {
            using (IDatabaseClient client = GetConnection())
            {
                string command = string.Format(@"\l {0}", path);

                client.ExecuteScalar(command);
            }            
        }

        private void KillProcess(Process process)
        {
            if (process != null)
            {
                try
                {
                    process.Exited -= ProcessExited;

                    AskProcessToClose();

                    const int tenSeconds = 10*1000;

                    // if process has not terminated itself
                    // kill it
                    if (!process.WaitForExit(tenSeconds))
                    {
                        process.Kill();
                        if(!process.WaitForExit(ProcessHelper.OneMinute))
                            throw new KdbPlusFatalException(String.Format(Constants.DefaultCulture,"Failed to kill process {0}.", process.Id));
                    }

                    ProcessExited(this, EventArgs.Empty);
                    process.Dispose();
                }
                catch (InvalidOperationException)
                {
                    // ignore exception if process was already killed
                }
            }
        }

        private void AskProcessToClose()
        {
            if (_port <= 0)
                return;

            try
            {
                using (IDatabaseClient client = GetConnection())
                {
                    client.ExecuteScalar(@"\\");
                }
            }
            catch (KdbPlusException)
            {
                // ignore the exception saying that connection was broken
            }
        }

        private void ProcessExited(object sender, EventArgs e)
        {
            _isAlive = false;
            _storage.SetProcessId(_processKey, NoProcessId);
        }

        private void UpdateSettingsStorage()
        {
            _storage.RemoveProcessId(_processKey);

            _processKey = string.Format("{0}_{1}", _host, _port);

            _storage.SetProcessId(_processKey, _id);
        }

        private void KillProcessById(int id)
        {
            try
            {
                _logger.InfoFormat("Killing process {0}.", id);

                if(id != NoProcessId)
                    ProcessHelper.KillProcesses(new[] { id }, _processName);

                _logger.InfoFormat("Successfully killed process {0}.", id);
            }
            catch (ProcessException exc)
            {
                _logger.ErrorFormat(exc, "Failed to kill process {0}.", id);
            }
        }

        private Process StartProcess()
        {
            Process process = null;

            try
            {
                _logger.InfoFormat("Starting Kdb+ process ({0}:{1}) with command line: {2}.", _host, _port, _commandLine);

                ExecutePrestartCommands();

                process = StartNewProcess(_commandLine);

                CheckIfProcessIsRepsponding();

                SetupProcess();
            }
            catch (Exception)
            {
                KillProcess(process);

                throw;
            }

            return process;
        }

        private void ExecutePrestartCommands()
        {
            if (_preStartCommands.Count > 0)
            {
                foreach (Action command in _preStartCommands)
                {
                    command.Invoke();
                }
            }
        }

        private void SetupProcess()
        {
            if (_setupCommands.Count > 0)
            {
                using (IDatabaseClient client = GetConnection())
                {
                    foreach (Action<IDatabaseClient> command in _setupCommands)
                    {
                        command(client);
                    }
                }
            }
        }

        private Process StartNewProcess(string commandArgs)
        {
            return ProcessHelper.StartNewProcess(_processName, _workingDirectory, commandArgs, _processTitle, _hideWindow);
        }

        private Process OpenExistingProcess()
        {
            return ProcessHelper.OpenExisting(_id, _processName);
        }

        private bool IsStarted()
        {
            if (ProcessHelper.IsStarted(_id, _processName))
            {
                if (IsProcessResponding())
                {
                    return true;
                }
            }

            return false;
        }

        private void CheckIfProcessIsRepsponding()
        {
            TimeSpan timeout = _waitForPortTimeout;

            // Wait for port.
            while (timeout != TimeSpan.Zero)
            {
                if (IsProcessResponding())
                {
                    return;
                }

                if (timeout > _idleTime)
                {
                    Thread.Sleep(_idleTime);

                    timeout = timeout - _idleTime;
                }
                else
                {
                    Thread.Sleep(timeout);

                    timeout = TimeSpan.Zero;
                }
            }

            if (!IsProcessResponding())
            {
                throw new ProcessException(string.Format("{0}:{1} is not responding.", _host, _port));
            }
        }

        private bool IsProcessResponding()
        {
            try
            {
                using (IDatabaseClient client = GetConnection())
                {
                    CheckResult(client.ExecuteScalar(TestConnectionCommand));

                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        private static void CheckResult(object result)
        {
            if (result != null)
            {
                string errorMessage = result as string;

                if (errorMessage != null && errorMessage.StartsWith(ErrorInQuery, StringComparison.OrdinalIgnoreCase))
                    throw new KdbPlusException(String.Format(CultureInfo.InvariantCulture, "Error occured during K+ query: '{0}'.", errorMessage.Replace(ErrorInQuery, String.Empty)));
            }
        }
    }
}
