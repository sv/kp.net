using System;
using System.Data.Common;
using System.Globalization;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusException : DbException
    {
        private readonly string _query;

        public KdbPlusException(string message, string query, Exception innerException)
            : base(message, innerException)
        {
            Guard.ThrowIfNullOrEmpty(query, "query");

            _query = query;
        }

        public KdbPlusException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public KdbPlusException(string message)
            : base(message)
        {
        }

        public string Query
        {
            get { return _query; }
        }

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
