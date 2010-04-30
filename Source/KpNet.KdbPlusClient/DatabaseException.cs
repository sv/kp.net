using System;
using System.Globalization;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Exception class for db-related errors.
    /// </summary>
    public sealed class DatabaseException : ApplicationException
    {
        private readonly string _query;

        public DatabaseException(string message, string query, Exception innerException) : base(message, innerException)
        {
            _query = query;
        }

        public string Query
        {
            get { return _query; }
        }

        public override string Message
        {
            get
            {
                return String.Concat(base.Message, Environment.NewLine,
                                     String.Format(CultureInfo.InvariantCulture, "Query:{0}.", _query));
            }
        }
    }
}