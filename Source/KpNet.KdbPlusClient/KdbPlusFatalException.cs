using System;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Exception class to be thrown when database client cannot be reused after error.
    /// </summary>
    public sealed class KdbPlusFatalException : KdbPlusException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusFatalException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="query">The query.</param>
        /// <param name="innerException">The inner exception.</param>
        public KdbPlusFatalException(string message, string query, Exception innerException) : base(message, query, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusFatalException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public KdbPlusFatalException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusFatalException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public KdbPlusFatalException(string message) : base(message)
        {
        }
    }
}
