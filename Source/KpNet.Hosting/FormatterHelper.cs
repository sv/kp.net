using System.Collections.Generic;
using System.Text;

namespace KpNet.Hosting
{
    /// <summary>
    /// Class for formatting numbers.
    /// </summary>
    public static class FormatterHelper
    {
        private const string Delimeter = ";";

        /// <summary>
        /// Formats the numbers.
        /// </summary>
        /// <param name="numbers">The numbers.</param>
        /// <returns>Formatted numbers.</returns>
        public static string FormatNumbers(IEnumerable<int> numbers)
        {
            StringBuilder buffer = new StringBuilder();

            foreach (int id in numbers)
            {
                buffer.Append(id);
                buffer.Append(Delimeter);
            }

            return buffer.ToString();
        }
    }
}
