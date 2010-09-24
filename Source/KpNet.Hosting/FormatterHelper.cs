using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KpNet.Hosting
{
    public static class FormatterHelper
    {
        private const string Delimeter = ";";

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

        public static List<int> ParseNumbers(string value)
        {
            List<int> ids = new List<int>();

            if (String.IsNullOrEmpty(value))
                return ids;

            string[] parts = value.Split(new[] { Delimeter }, StringSplitOptions.RemoveEmptyEntries);

            ids.AddRange(parts.Select(Int32.Parse));

            return ids;
        }
    }
}
