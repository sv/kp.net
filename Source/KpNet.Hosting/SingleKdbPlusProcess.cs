using System;
using System.Collections.Generic;
using TR.Common;

namespace KpNet.Hosting
{
    internal sealed class SingleKdbPlusProcess : KdbPlusProcess
    {
        private readonly string _processName;
        private readonly string _host;
        private readonly int _port;
        private readonly string _workingDirectory;
        private readonly ILogger _logger;
        private readonly ISettingsStorage _storage;
        private readonly string _commandLine;
        private readonly string _processTitle;
        private readonly List<Action<KdbPlusDatabaseConnection>> _commands;

        private readonly object _locker = new object();
        private int _id;
        private readonly string _processKey;

        public SingleKdbPlusProcess(string processName, string host, 
                                    int port, string commandLine, string processTitle,
                                    string workingDirectory, ILogger logger, 
                                    ISettingsStorage storage, 
                                    List<Action<KdbPlusDatabaseConnection>> commands)
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

            _commands = new List<Action<KdbPlusDatabaseConnection>>();
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
                try
                {
                    CheckIfProcessIsRepsponding();

                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public override KdbPlusDatabaseConnection GetConnection()
        {
            return new KdbPlusDatabaseConnection(_host, _port);
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
                using(KdbPlusDatabaseConnection connection = GetConnection())
                {
                    foreach(Action<KdbPlusDatabaseConnection> command in _commands)
                    {
                        command(connection);
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
            if (!KdbPlusDatabaseConnection.Check(_host, _port))
            {
                throw new ProcessException(string.Format("{0}:{1} is not responding.", _host, _port));
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
