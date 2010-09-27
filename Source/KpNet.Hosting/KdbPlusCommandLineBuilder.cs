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

        public KdbPlusCommandLineBuilder EnableSyncLogging()
        {
            return SetSyncLogging(true);
        }

        public KdbPlusCommandLineBuilder DisableSyncLogging()
        {
            return SetSyncLogging(false);
        }

        public KdbPlusCommandLineBuilder EnableMultiThreading()
        {
            return SetMultiThreading(true);
        }

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

        public KdbPlusCommandLineBuilder SetPort(int? port)
        {
            _port = port;

            return this;
        }

        public KdbPlusCommandLineBuilder SetThreadCount(int? count)
        {
            _threadCount = count;

            return this;
        }

        public KdbPlusCommandLineBuilder SetLog(string log)
        {
            _log = log;

            return this;
        }

        public string CreateNew()
        {
            StringBuilder builder = new StringBuilder();

            if(!string.IsNullOrWhiteSpace(_log))
            {
                builder.AppendFormat(_syncLoggingEnabled ? "{0} -L " : "{0} -l ", _log);
            }

            if(_port.HasValue)
            {
                builder.AppendFormat(_multiThreadingEnabled ? "-p -{0} " : "-p {0}", _port.Value);
            }

            if(_threadCount.HasValue)
            {
                builder.AppendFormat("-s {0} ", _threadCount.Value);
            }

            return builder.ToString();
        }
    }
}
