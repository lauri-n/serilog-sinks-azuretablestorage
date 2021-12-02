using System;
using System.Text;
using System.Text.RegularExpressions;
using Serilog.Events;
using Serilog.Sinks.AzureTableStorage.KeyGenerator;

namespace Serilog.Sinks.AzureTableStorage.Sinks.KeyGenerator
{
    public class PropertiesKeyGenerator : DefaultKeyGenerator
    {
        // Valid RowKey name characters
        private static readonly Regex _rowKeyNotAllowedMatch = new Regex(@"(\\|/|#|\?|[\x00-\x1f]|[\x7f-\x9f])");

        // Maximum RowKey length (1024B so 512 chars)
        private const int _maxRowKeyLengthCharacters = 1024 / 2;

        /// <summary>
        /// Generate a valid string for a table property key by removing invalid characters
        /// </summary>
        /// <param name="s">
        /// The input string
        /// </param>
        /// <returns>
        /// The string that can be used as a property
        /// </returns>
        public static string GetValidStringForTableKey(string s)
        {
            return _rowKeyNotAllowedMatch.Replace(s, "");
        }

        /// <summary>
        /// Automatically generates the RowKey using the following template: {Level|MessageTemplate|suffix|Guid}
        /// </summary>
        /// <param name="logEvent">the log event</param>
        /// <param name="suffix">suffix for the RowKey</param>
        /// <returns>The generated RowKey</returns>
        public override string GenerateRowKey(LogEvent logEvent, string suffix = null)
        {
            var prefixBuilder = new StringBuilder(_maxRowKeyLengthCharacters);

            // Join level and message template
            prefixBuilder.Append(logEvent.Level).Append('|').Append(GetValidStringForTableKey(logEvent.MessageTemplate.Text));

            var postfixBuilder = new StringBuilder(_maxRowKeyLengthCharacters);

            if (suffix != null)
                postfixBuilder.Append('|').Append(GetValidStringForTableKey(suffix));

            // Append GUID to postfix
            postfixBuilder.Append('|').Append(Guid.NewGuid());

            // Truncate prefix if too long
            var maxPrefixLength = _maxRowKeyLengthCharacters - postfixBuilder.Length;
            if (prefixBuilder.Length > maxPrefixLength)
            {
                prefixBuilder.Length = maxPrefixLength;
            }
            return prefixBuilder.Append(postfixBuilder).ToString();
        }
    }
}
