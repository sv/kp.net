using System;
using System.Data.Common;
using System.Globalization;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// DbException implementation for KDB+ provider.
    /// </summary>
    public class KdbPlusException : DbException
    {
        private readonly string _query;

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="query">The query.</param>
        /// <param name="innerException">The inner exception.</param>
        public KdbPlusException(string message, string query, Exception innerException)
            : base(message, innerException)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            _query = query;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public KdbPlusException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public KdbPlusException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Gets the query.
        /// </summary>
        /// <value>The query.</value>
        public string Query
        {
            get { return _query; }
        }

        /// <summary>
        /// Gets a message that describes the current exception.
        /// </summary>
        /// <value></value>
        /// <returns>
        /// The error message that explains the reason for the exception, or an empty string("").
        /// </returns>
        public override string Message
        {
            get
            {
                if (String.IsNullOrEmpty(_query))
                    return base.Message;

                return String.Concat(base.Message, Environment.NewLine,
                                     String.Format(CultureInfo.InvariantCulture, "Query:{0}.", _query));
            }
        }
    }
}
