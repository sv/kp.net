using System;

namespace KpNet.Common
{
    /// <summary>
    /// Interface contract for application logger.
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Log message as debug.
        /// </summary>
        /// <param name="message">The message.</param>
        void Debug(string message);

        /// <summary>
        /// Log message as debug.
        /// </summary>
        /// <param name="messagePattern">The message pattern.</param>
        /// <param name="items">The items.</param>
        void DebugFormat(string messagePattern, params object[] items);

        /// <summary>
        /// Log message as info.
        /// </summary>
        /// <param name="message">The message.</param>
        void Info(string message);

        /// <summary>
        /// Log message as info.
        /// </summary>
        /// <param name="messagePattern">The message pattern.</param>
        /// <param name="items">The items.</param>
        void InfoFormat(string messagePattern, params object[] items);

        /// <summary>
        /// Log message as warning.
        /// </summary>
        /// <param name="message">The message.</param>
        void Warning(string message);

        /// <summary>
        /// Log message as warning.
        /// </summary>
        /// <param name="messagePattern">The message pattern.</param>
        /// <param name="items">The items.</param>
        void WarningFormat(string messagePattern, params object[] items);

        /// <summary>
        /// Log the message as error.
        /// </summary>
        /// <param name="message">The message.</param>
        void Error(string message);

        /// <summary>
        /// Log the message as error.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="exception">The exception.</param>
        void Error(string message, Exception exception);

        /// <summary>
        /// Log the message as error.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <param name="messagePattern">The message pattern.</param>
        /// <param name="items">The items.</param>
        void ErrorFormat(Exception exception, string messagePattern, params object[] items);

        /// <summary>
        /// Log the message as error.
        /// </summary>
        /// <param name="messagePattern">The message pattern.</param>
        /// <param name="items">The items.</param>
        void ErrorFormat(string messagePattern, params object[] items);
    }
}