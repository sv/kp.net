using System;

namespace KpNet.Hosting
{
    /// <summary>
    /// The exception that is thrown when wrong operation is performed on a created process.
    /// </summary>
    public sealed class ProcessAlreadyCreatedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessAlreadyCreatedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ProcessAlreadyCreatedException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessAlreadyCreatedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public ProcessAlreadyCreatedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
