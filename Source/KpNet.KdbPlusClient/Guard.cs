using System;

namespace KpNet.KdbPlusClient
{
    /// <summary>
    /// This class provides checking input parameter values for null or empty values. 
    /// </summary>
    public static class Guard
    {
        private const string ValueIsNullOrEmpty = "Provided argument value is null or empty.";

        /// <summary>
        /// Throws ArgumentNullException if value is null
        /// </summary>
        /// <param name="value"></param>
        public static void ThrowIfNull(object value)
        {
            if (value == null) throw new ArgumentNullException(string.Empty, ValueIsNullOrEmpty);
        }

        /// <summary>
        /// Throws ArgumentNullException with parameter name specified if value is null 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="parameterName"></param>
        public static void ThrowIfNull(object value, string parameterName)
        {
            if (value == null) throw new ArgumentNullException(parameterName, ValueIsNullOrEmpty);
        } 
        
        /// <summary>
        /// Throws ArgumentNullException with parameter name and custom message if value is null 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="parameterName"></param>
        /// <param name="message"></param>
        public static void ThrowIfNull(object value, string parameterName, string message)
        {
            if (value == null) throw new ArgumentNullException(parameterName, message);
        }

        /// <summary>
        /// Throws ArgumentNullException if value is null or empty
        /// </summary>
        /// <param name="value"></param>
        public static void ThrowIfNullOrEmpty(string value)
        {
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(string.Empty, ValueIsNullOrEmpty);
        }

        /// <summary>
        /// Throws ArgumentNullException with parameter name specified if value is null or empty
        /// </summary>
        /// <param name="value"></param>
        /// <param name="parameterName"></param>
        public static void ThrowIfNullOrEmpty(string value, string parameterName)
        {
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(parameterName, ValueIsNullOrEmpty);
        }
        
        /// <summary>
        /// Throws ArgumentNullException with parameter name and custom message if value is null or empty
        /// </summary>
        /// <param name="value"></param>
        /// <param name="parameterName"></param>
        /// <param name="message"></param>
        public static void ThrowIfNullOrEmpty(string value, string parameterName, string message)
        {
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(parameterName, message);
        }
    }
}