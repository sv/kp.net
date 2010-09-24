using System;
using System.Collections.Generic;
using System.Globalization;
using KpNet.Common;
using KpNet.KdbPlusClient;


namespace KpNet.Hosting
{
    internal sealed class SingleKdbPlusProcess : KdbPlusProcess
    {
        private const string TestConnectionCommand = @"0"; //ping q process - it should return 0 back
        private const string ErrorInQuery = "ERROR";

        private readonly string _processName;
        private readonly string _host;
        private readonly int _port;
        private readonly string _workingDirectory;
        private readonly ILogger _logger;
        private readonly ISettingsStorage _storage;
        private readonly string _commandLine;
        private readonly string _processTitle;
        private readonly List<Action<IDatabaseClient>> _commands;

        private readonly object _locker = new object();
        private int _id;
        private readonly string _processKey;

        public SingleKdbPlusProcess(string processName, string host, 
                                    int port, string commandLine, string processTitle,
                                    string workingDirectory, ILogger logger, 
                                    ISettingsStorage storage,
                                    List<Action<IDatabaseClient>> commands)
        {
            Guard.ThrowIfNull(logger, "logger");
            Guard.ThrowIfNull(storage, "storage");
            Guard.ThrowIfNull(commands, "commands");
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

            _commands = commands;
        }

        public override void Start()
        {
            lock (_locker)
            {
                try
                {
                    KillProcess(_id);

                    _id = StartProcess();

                    _storage.SetProcessId(_processKey, _id);
                }
                catch (Exception ex)
                {
                    _logger.Error("Failed to start db process.", ex);

                    throw;
                }
            }
        }

        public override void Kill()
        {
            lock (_locker)
            {
                KillProcess(_id);
            }
        }        
        
        public override bool IsAlive
        {
            get
            {
                return IsProcessResponding();
            }
        }

        public override IDatabaseClient GetConnection()
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder();

            builder.Port = _port;

            builder.Server = _host;

            KdbPlusDatabaseClient client = KdbPlusDatabaseClient.Factory.CreateNewClient(builder);

            client.SendTimeout = TimeSpan.FromMinutes(15);

            client.ReceiveTimeout = TimeSpan.FromMinutes(15);

            return client;
        }        

        private int StartProcess()
        {
            int processId = -1;

            try
            {
                _logger.InfoFormat("Starting Kdb+ process ({0}:{1}) with command line: {2}.", _host, _port, _commandLine);

                processId = StartNewProcess(_commandLine);

                CheckIfProcessIsRepsponding();

                SetupProcess();
            }
            catch (Exception)
            {
                if (processId != -1)
                {
                    KillProcess(processId);
                }

                throw;
            }

            return processId;
        }

        private void SetupProcess()
        {
            if(_commands.Count > 0)
            {
                using (IDatabaseClient client = GetConnection())
                {
                    foreach (Action<IDatabaseClient> command in _commands)
                    {
                        command(client);
                    }
                }
            }
        }

        private int StartNewProcess(string commandArgs)
        {
            return ProcessHelper.StartNewProcess(_processName, _workingDirectory, commandArgs, _processTitle);
        }

        private void CheckIfProcessIsRepsponding()
        {
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

        private void KillProcess(int id)
        {
            try
            {
                _logger.InfoFormat("Killing process {0}.", id);

                ProcessHelper.KillProcesses(new int[]{ id }, _processName);

                _logger.InfoFormat("Successfully killed process {0}.", id);
            }
            catch (ProcessException exc)
            {
                _logger.ErrorFormat(exc, "Failed to kill process {0}.", id);
            }
        }
    }
}
