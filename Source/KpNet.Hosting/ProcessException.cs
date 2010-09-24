using System;

namespace KpNet.Hosting
{
    internal class ProcessException : Exception
    {
        public ProcessException(string message)
            : base(message)
        {
        }

        public ProcessException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
