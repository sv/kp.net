using System.Collections.Generic;
using System.Text;

namespace KpNet.Hosting
{
    internal sealed class KdbPlusCommandLineBuilder
    {
        private int? _port;
        private int? _threadCount;
        private string _log;
        private bool _syncLoggingEnabled;
        private bool _multiThreadingEnabled;
        private string _startupScript;
        private List<string> _commandLineArguments;

        /// <summary>
        /// Enables the sync logging.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder EnableSyncLogging()
        {
            return SetSyncLogging(true);
        }

        /// <summary>
        /// Disables the sync logging.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder DisableSyncLogging()
        {
            return SetSyncLogging(false);
        }

        /// <summary>
        /// Enables the multi threading.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder EnableMultiThreading()
        {
            return SetMultiThreading(true);
        }

        /// <summary>
        /// Disables the multi threading.
        /// </summary>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder DisableMultiThreading()
        {
            return SetMultiThreading(false);
        }

        private KdbPlusCommandLineBuilder SetMultiThreading(bool enabled)
        {
            _multiThreadingEnabled = enabled;

            return this;
        }

        private KdbPlusCommandLineBuilder SetSyncLogging(bool enabled)
        {
            _syncLoggingEnabled = enabled;

            return this;
        }

        /// <summary>
        /// Sets the port.
        /// </summary>
        /// <param name="port">The port.</param>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder SetPort(int? port)
        {
            _port = port;

            return this;
        }

        /// <summary>
        /// Sets the thread count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder SetThreadCount(int? count)
        {
            _threadCount = count;

            return this;
        }

        /// <summary>
        /// Sets the log.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <returns>Itself.</returns>
        public KdbPlusCommandLineBuilder SetLog(string log)
        {
            _log = log;

            return this;
        }

        /// <summary>
        /// Sets the startup script.
        /// </summary>
        /// <param name="scriptFileName">Name of the script file.</param>
        /// <returns></returns>
        public KdbPlusCommandLineBuilder SetStartupScript(string scriptFileName)
        {
            _startupScript = scriptFileName;

            return this;
        }

        public KdbPlusCommandLineBuilder SetCommandLineArguments(List<string> arguments)
        {
            _commandLineArguments = arguments;
            return this;
        }

        /// <summary>
        /// Creates new command line.
        /// </summary>
        /// <returns>New command line.</returns>
        public string CreateNew()
        {
            StringBuilder builder = new StringBuilder();

            if(!string.IsNullOrEmpty(_startupScript))
            {
                builder.AppendFormat(" {0}", _startupScript);
            }

            if(!string.IsNullOrWhiteSpace(_log))
            {
                builder.AppendFormat(_syncLoggingEnabled ? "{0} -L " : "{0} -l ", _log);
            }

            if(_port.HasValue)
            {
                builder.AppendFormat(_multiThreadingEnabled ? "-p -{0} " : "-p {0} ", _port.Value);
            }

            if(_threadCount.HasValue)
            {
                builder.AppendFormat("-s {0} ", _threadCount.Value);
            }

            if(_commandLineArguments != null && _commandLineArguments.Count > 0)
            {
                foreach (string argument in _commandLineArguments)
                {
                    builder.AppendFormat(" {0}", argument);
                }
            }

            return builder.ToString();
        }
    }
}
