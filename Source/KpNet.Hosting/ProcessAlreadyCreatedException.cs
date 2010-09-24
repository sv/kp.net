using System;

namespace KpNet.Hosting
{
    public sealed class ProcessAlreadyCreatedException : Exception
    {
        public ProcessAlreadyCreatedException(string message)
            : base(message)
        {
        }

        public ProcessAlreadyCreatedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
