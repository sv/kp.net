
using System;
using Kdbplus;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// Helper class for working with null values for Kdb+
    /// </summary>
    public static class KdbPlusNull
    {
        /// <summary>
        /// Null value for int32
        /// </summary>
        public const int NullInt = Int32.MinValue;

        /// <summary>
        /// Null value for int 16
        /// </summary>
        public const short NullShort = Int16.MinValue;

        /// <summary>
        /// Null value for int64
        /// </summary>
        public const long NullLong = Int64.MinValue;

        /// <summary>
        /// Null value for string
        /// </summary>
        public static readonly string NullString = String.Empty;

        /// <summary>
        /// Null value for double
        /// </summary>
        public static Double NullDouble = Double.NaN;


        /// <summary>
        /// Null value for byte
        /// </summary>
        public const byte NullByte = 0;


        /// <summary>
        /// Null value for datetime
        /// </summary>
        public static readonly DateTime NullDateTime = new DateTime(0);


        /// <summary>
        /// Null value for date
        /// </summary>
        public static readonly c.Date NullDate = new c.Date(NullInt);
    }
}
