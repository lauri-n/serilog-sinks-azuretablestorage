// Copyright 2014 Serilog Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Serilog.Sinks.AzureTableStorage.KeyGenerator;
using Azure.Data.Tables;
using System.Text;

namespace Serilog.Sinks.AzureTableStorage
{
    /// <summary>
    /// Utility class for Azure Storage Table entity
    /// </summary>
    public static class AzureTableStorageEntityFactory
    {
        // Azure tables support a maximum of 255 properties. PartitionKey, RowKey and Timestamp
        // bring the maximum to 252.
        private const int _maxNumberOfPropertiesPerRow = 252;

        // Maximum size of a property in Azure Table Storage
        private const int _propertyMaximumSize = 65536;

        /// <summary>
        /// Creates a DynamicTableEntity for Azure Storage, given a Serilog <see cref="LogEvent"/>.Properties
        /// are stored as separate columns.
        /// </summary>
        /// <param name="logEvent">The event to log</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="additionalRowKeyPostfix">Additional postfix string that will be appended to row keys</param>
        /// <param name="keyGenerator">The IKeyGenerator for the PartitionKey and RowKey</param>
        /// <param name="propertyColumns">Specific properties to be written to columns. By default, all properties will be written to columns.</param>
        /// <returns></returns>
        public static TableEntity CreateEntityWithProperties(LogEvent logEvent, IFormatProvider formatProvider, string additionalRowKeyPostfix, IKeyGenerator keyGenerator, string[] propertyColumns = null)
        {
            var tableEntity = new TableEntity
            {
                PartitionKey = keyGenerator.GeneratePartitionKey(logEvent),
                RowKey = keyGenerator.GenerateRowKey(logEvent, additionalRowKeyPostfix),
                Timestamp = logEvent.Timestamp
            };

            AddTableEntityProperty(tableEntity, "MessageTemplate", logEvent.MessageTemplate.Text);
            AddTableEntityProperty(tableEntity, "Level", logEvent.Level.ToString());
            AddTableEntityProperty(tableEntity, "RenderedMessage", logEvent.RenderMessage(formatProvider));

            if (logEvent.Exception != null)
            {
                AddTableEntityProperty(tableEntity, "Exception", logEvent.Exception.ToString());
            }

            List<KeyValuePair<ScalarValue, LogEventPropertyValue>> additionalData = null;
            var count = tableEntity.Count;
            bool isValid;

            foreach (var logProperty in logEvent.Properties)
            {
                isValid = IsValidColumnName(logProperty.Key) && ShouldIncludeProperty(logProperty.Key, propertyColumns);

                // Don't add table properties for numeric property names
                if (isValid && (count++ < _maxNumberOfPropertiesPerRow - 1))
                {
                    AddTableEntityProperty(tableEntity, logProperty.Key, AzurePropertyFormatter.ToEntityProperty(logProperty.Value, null, formatProvider));
                }
                else
                {
                    if (additionalData == null)
                    {
                        additionalData = new List<KeyValuePair<ScalarValue, LogEventPropertyValue>>();
                    }
                    additionalData.Add(new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue(logProperty.Key), logProperty.Value));
                }
            }

            if (additionalData != null)
            {
                AddTableEntityProperty(tableEntity, "AggregatedProperties", AzurePropertyFormatter.ToEntityProperty(new DictionaryValue(additionalData), null, formatProvider));
            }

            return tableEntity;
        }

        /// <summary>
        /// Determines whether or not the given property names conforms to naming rules for C# identifiers
        /// </summary>
        /// <param name="propertyName">Name of the property to check</param>
        /// <returns>true if the property name conforms to C# identifier naming rules and can therefore be added as a table property</returns>
        private static bool IsValidColumnName(string propertyName)
        {
            const string regex = @"^(?:((?!\d)\w+(?:\.(?!\d)\w+)*)\.)?((?!\d)\w+)$";
            return Regex.Match(propertyName, regex).Success;
        }

        /// <summary>
        /// Determines if the given property name exists in the specific columns. 
        /// Note: If specific columns is not defined then the property name is considered valid. 
        /// </summary>
        /// <param name="propertyName">Name of the property to check</param>
        /// <param name="propertyColumns">List of defined properties only to be added as columns</param>
        /// <returns>true if the no propertyColumns are specified or it is included in the propertyColumns property</returns>
        private static bool ShouldIncludeProperty(string propertyName, string[] propertyColumns)
        {
            return propertyColumns == null || propertyColumns.Contains(propertyName);
        }

        /// <summary>
        /// Adds the given property to the table entity cropping too long string values to maximum allowed length.
        /// </summary>
        /// <param name="entity">The TableEntity to which the property is added</param>
        /// <param name="propertyName">Name of the property to add</param>
        /// <param name="value">Property value</param>
        private static void AddTableEntityProperty(TableEntity entity, string propertyName, object value)
        {
            if (value is string stringValue)
                entity.Add(propertyName, CropValueIsNecessary(stringValue, _propertyMaximumSize));
            else
                entity.Add(propertyName, value);
        }

        /// <summary>
        /// Crops the given string so that it's UTF-16 encoded size is at most the specified number of bytes.
        /// The cropping avoids cutting the string between surrogate pairs.
        /// </summary>
        /// <param name="str">String to crop</param>
        /// <param name="maxByteLength">Maximum length of the returned string in bytes when UTF-16 encoded.</param>
        /// <returns>The given string cropped to specified maximum size</returns>
        private static string CropValueIsNecessary(string str, int maxByteLength)
        {
            if (str.Length < maxByteLength / 2)
                return str;

            byte[] byteArray = Encoding.Unicode.GetBytes(str);
            int evenByteLength = maxByteLength - maxByteLength % 2;

            if (byteArray.Length <= evenByteLength)
                return str;

            int bytePointer = evenByteLength;

            if (IsUtf16TrailingSurrogate(byteArray[bytePointer + 1]))
                bytePointer -= 2;

            return Encoding.Unicode.GetString(byteArray, 0, bytePointer);
        }

        /// <summary>
        /// Tests is the given byte a trailing surrogate's high byte.
        /// </summary>
        /// <param name="b">The byte to test</param>
        /// <returns>True if the given byte is a trailing surrogate's high byte. False otherwise.</returns>
        private static bool IsUtf16TrailingSurrogate(byte b)
        {
            return (b & 0b1111_1100) == 0b1101_1100;
        }
    }
}
