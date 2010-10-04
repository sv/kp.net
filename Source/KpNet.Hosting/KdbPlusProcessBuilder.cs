using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using KpNet.Common;
using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    /// <summary>
    /// Class for building Kdb processes. 
    /// </summary>
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
        private bool _syncLoggingEnabled;
        private bool _multiThreadingEnabled;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusProcessBuilder"/> class.
        /// </summary>
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

            _syncLoggingEnabled = false;

            _multiThreadingEnabled = false;
        }

        /// <summary>
        /// Enables the sync KDB logging.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder EnableSyncKdbLogging()
        {
            return SetSyncLogging(true);
        }

        /// <summary>
        /// Disables the sync KDB logging.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder DisableSyncKdbLogging()
        {
            return SetSyncLogging(false);
        }

        /// <summary>
        /// Enables the multi threading.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder EnableMultiThreading()
        {
            return SetMultiThreading(true);
        }

        /// <summary>
        /// Disables the multi threading.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder DisableMultiThreading()
        {
            return SetMultiThreading(false);
        }

        private KdbPlusProcessBuilder SetMultiThreading(bool enabled)
        {
            _multiThreadingEnabled = enabled;

            return this;
        }

        private KdbPlusProcessBuilder SetSyncLogging(bool enabled)
        {
            _syncLoggingEnabled = enabled;

            return this;
        }

        /// <summary>
        /// Sets the port.
        /// </summary>
        /// <param name="port">The port.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetPort(int port)
        {
            ThrowExceptionfIfProcessCreated();

            _port = port;

            return this;
        }

        /// <summary>
        /// Sets the host.
        /// </summary>
        /// <param name="host">The host.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetHost(string host)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(host, "host");

            _host = host;

            return this;
        }

        /// <summary>
        /// Sets the working directory.
        /// </summary>
        /// <param name="workingDirectory">The working directory.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetWorkingDirectory(string workingDirectory)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(workingDirectory, "workingDirectory");

            _workingDirectory = workingDirectory;

            return this;
        }

        /// <summary>
        /// Sets the settings storage.
        /// </summary>
        /// <param name="storage">The storage.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetSettingsStorage(ISettingsStorage storage)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(storage, "storage");

            _settingsStorage = storage;

            return this;
        }

        /// <summary>
        /// Sets the logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetLogger(ILogger logger)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(logger, "logger");

            _logger = logger;

            return this;
        }

        /// <summary>
        /// Sets the name of the process.
        /// </summary>
        /// <param name="processName">Name of the process.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetProcessName(string processName)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(processName, "processName");

            _processName = processName;

            return this;
        }

        /// <summary>
        /// Sets the process title.
        /// </summary>
        /// <param name="processTitle">The process title.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetProcessTitle(string processTitle)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(processTitle, "processTitle");

            _processTitle = processTitle;

            return this;
        }

        /// <summary>
        /// Adds the command.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder AddCommand(Action<IDatabaseClient> command)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(command, "command");

            _commands.Add(command);

            return this;
        }

        /// <summary>
        /// Adds the commands.
        /// </summary>
        /// <param name="commands">The commands.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder AddCommands(IEnumerable<Action<IDatabaseClient>> commands)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNull(commands, "commands");

            _commands.AddRange(commands);

            return this;
        }

        /// <summary>
        /// Sets the thread count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetThreadCount(int count)
        {
            ThrowExceptionfIfProcessCreated();

            _threadCount = count;

            return this;
        }

        /// <summary>
        /// Sets the KDB log.
        /// </summary>
        /// <param name="kdbLog">The KDB log.</param>
        /// <returns>Itself.</returns>
        public KdbPlusProcessBuilder SetKdbLog(string kdbLog)
        {
            ThrowExceptionfIfProcessCreated();

            Guard.ThrowIfNullOrEmpty(kdbLog, "kdbLog");

            _kdbLog = kdbLog;

            return this;
        }

        /// <summary>
        /// Starts new Kdb process.
        /// </summary>
        /// <returns>Kdb process.</returns>
        public KdbPlusProcess StartNew()
        {
            _processCreated = true;

            SingleKdbPlusProcess process = new SingleKdbPlusProcess(_processName, _host, Port, GetCommandLine(Port),
                                            _processTitle, _workingDirectory, _logger,
                                            _settingsStorage, _commands);

            process.Start();

            return process;
        }

        /// <summary>
        /// Starts new composite Kdb process.
        /// </summary>
        /// <param name="count">The count of inner processes.</param>
        /// <returns></returns>
        public KdbPlusProcess StartNewComposite(int count)
        {
            List<KdbPlusProcess> processes = new List<KdbPlusProcess>(count);            

            for(int i = 0; i < count; i++)
            {
                processes.Add(new SingleKdbPlusProcess(_processName, _host, Port + i, GetCommandLine(Port + i),
                                            _processTitle, _workingDirectory, _logger,
                                            _settingsStorage, _commands));
            }

            CompositeKdbPlusProcess result = new CompositeKdbPlusProcess(processes);

            return result;
        }

        private string GetCommandLine(int port)
        {
            KdbPlusCommandLineBuilder builder = new KdbPlusCommandLineBuilder();

            if(_syncLoggingEnabled)
            {
                builder.EnableSyncLogging();
            }

            if(_multiThreadingEnabled)
            {
                builder.EnableMultiThreading();
            }

            return builder.SetLog(_kdbLog).SetPort(port).SetThreadCount(_threadCount).CreateNew();
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
