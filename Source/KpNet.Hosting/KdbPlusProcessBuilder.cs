using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using KpNet.Common;
using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    public sealed class KdbPlusProcessBuilder
    {
        private const int DefaultPort = 1001;

        private string _host;
        private int? _port;
        private string _workingDirectory;
        private ISettingsStorage _settingsStorage;
        private ILogger _logger;
        private string _processName;
        private string _processTitle;
        private readonly List<Action<IDatabaseClient>> _commands;
        private string _kdbLog;
        private int? _threadCount;
        private bool _processCreated;

        public KdbPlusProcessBuilder()
        {
            _host = "localhost";

            Assembly assembly = Assembly.GetEntryAssembly();
            if(assembly != null)
            {
                _workingDirectory = Path.GetDirectoryName(assembly.Location);
            }

            _logger = NullLogger.Instance;

            _settingsStorage = NullSettingsStorage.Instance;

            _processName = "q";

            _processTitle = string.Empty;

            _commands = new List<Action<IDatabaseClient>>();            

            _kdbLog = string.Empty;

            _processCreated = false;
        }
        
        public KdbPlusProcessBuilder SetPort(int port)
        {
            ThrowExceptionfIfProcessCreated();

            _port = port;

            return this;
        }

        public KdbPlusProcessBuilder SetHost(string host)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(host, "host");

            _host = host;

            return this;
        }

        public KdbPlusProcessBuilder SetWorkingDirectory(string workingDirectory)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(workingDirectory, "workingDirectory");

            _workingDirectory = workingDirectory;

            return this;
        }

        public KdbPlusProcessBuilder SetSettingsStorage(ISettingsStorage storage)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(storage, "storage");

            _settingsStorage = storage;

            return this;
        }

        public KdbPlusProcessBuilder SetLogger(ILogger logger)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(logger, "logger");

            _logger = logger;

            return this;
        }

        public KdbPlusProcessBuilder SetProcessName(string processName)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(processName, "processName");

            _processName = processName;

            return this;
        }
        
        public KdbPlusProcessBuilder SetProcessTitle(string processTitle)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(processTitle, "processTitle");

            _processTitle = processTitle;

            return this;
        }

        public KdbPlusProcessBuilder AddCommand(Action<IDatabaseClient> command)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(command, "command");

            _commands.Add(command);

            return this;
        }

        public KdbPlusProcessBuilder AddCommands(IEnumerable<Action<IDatabaseClient>> commands)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(commands, "commands");

            _commands.AddRange(commands);

            return this;
        }

        public KdbPlusProcessBuilder SetThreadCount(int count)
        {
            ThrowExceptionfIfProcessCreated();

            _threadCount = count;

            return this;
        }

        public KdbPlusProcessBuilder SetKdbLog(string kdbLog)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(kdbLog, "kdbLog");

            _kdbLog = kdbLog;

            return this;
        }

        public KdbPlusProcess StartNew()
        {
            _processCreated = true;

            SingleKdbPlusProcess process = new SingleKdbPlusProcess(_processName, _host, Port, GetCommandLine(),
                                            _processTitle, _workingDirectory, _logger,
                                            _settingsStorage, _commands);

            process.Start();

            return process;
        }

        private string GetCommandLine()
        {
            KdbPlusCommandLineBuilder builder = new KdbPlusCommandLineBuilder();

            return builder.SetLog(_kdbLog).SetPort(Port).SetThreadCount(_threadCount).BuildCommandLine();
        }

        private void ThrowExceptionfIfProcessCreated()
        {
            if(_processCreated)
            {
                throw new ProcessAlreadyCreatedException(string.Format("Process {0}:{1} has been already created.", _host, Port));
            }
        }

        private int Port
        {
            get
            {
                return _port.HasValue ? _port.Value : DefaultPort;
            }
        }
    }
}
