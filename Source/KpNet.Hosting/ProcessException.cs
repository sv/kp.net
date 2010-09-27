using System;

namespace KpNet.Hosting
{
    /// <summary>
    /// The exception that is thrown when something is wrong with the process.
    /// </summary>
    internal class ProcessException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ProcessException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public ProcessException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
