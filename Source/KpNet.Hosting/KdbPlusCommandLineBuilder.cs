using System.Text;

namespace KpNet.Hosting
{
    internal sealed class KdbPlusCommandLineBuilder
    {
        private int? _port;
        private int? _threadCount;
        private string _log;

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

        public string BuildCommandLine()
        {
            StringBuilder builder = new StringBuilder();

            if(!string.IsNullOrWhiteSpace(_log))
            {
                builder.AppendFormat("{0} -l ", _log);
            }

            if(_port.HasValue)
            {
                builder.AppendFormat("-p {0} ", _port.Value);
            }

            if(_threadCount.HasValue)
            {
                builder.AppendFormat("-s {0} ", _threadCount.Value);
            }

            return builder.ToString();
        }
    }
}
