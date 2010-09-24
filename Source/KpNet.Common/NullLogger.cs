using System;

namespace KpNet.Common
{
    /// <summary>
    /// NullObject for logger
    /// </summary>
    public sealed class NullLogger : ILogger
    {
        public static NullLogger Instance = new NullLogger();

        private NullLogger()
        {
        }

        public void Debug(string message)
        {
        }

        public void DebugFormat(string messagePattern, params object[] items)
        {
        }

        public void Info(string message)
        {
        }

        public void InfoFormat(string messagePattern, params object[] items)
        {
        }

        public void Warning(string message)
        {
        }

        public void WarningFormat(string messagePattern, params object[] items)
        {
        }

        public void Error(string message)
        {
        }

        public void Error(string message, Exception exception)
        {
        }

        public void ErrorFormat(Exception exception, string messagePattern, params object[] items)
        {
        }

        public void ErrorFormat(string messagePattern, params object[] items)
        {
        }
    }
}
