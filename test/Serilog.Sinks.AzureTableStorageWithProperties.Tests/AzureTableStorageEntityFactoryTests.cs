using Xunit;
using Serilog.Events;
using Serilog.Parsing;
using System;
using System.Collections.Generic;
using System.Linq;
using Serilog.Sinks.AzureTableStorage.Sinks.KeyGenerator;
using System.Text;

namespace Serilog.Sinks.AzureTableStorage.Tests
{
    public class AzureTableStorageEntityFactoryTests
    {
        private const int _propertyMaximumSizeBytes = 65536;

        [Fact]
        public void CreateEntityWithPropertiesShouldGenerateValidEntity()
        {
            var timestamp = new DateTimeOffset(2014, 12, 01, 18, 42, 20, 666, TimeSpan.FromHours(2));
            var exception = new ArgumentException("Some exceptional exception happened");
            var level = LogEventLevel.Information;
            var messageTemplate = "Template {Temp} {Prop}";
            var template = new MessageTemplateParser().Parse(messageTemplate);
            var properties = new List<LogEventProperty> {
                new LogEventProperty("Temp", new ScalarValue("Temporary")),
                new LogEventProperty("Prop", new ScalarValue("Property"))
            };
            var additionalRowKeyPostfix = "Some postfix";

            var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);

            var keyGenerator = new PropertiesKeyGenerator();
            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, additionalRowKeyPostfix, keyGenerator);

            // Make sure the partition key is in the expected format
            Assert.Equal(entity.PartitionKey, "0" + new DateTime(long.Parse(entity.PartitionKey)).Ticks);

            // Row Key
            var expectedRowKeyWithoutGuid = "Information|Template {Temp} {Prop}|Some postfix|";
            var rowKeyWithoutGuid = entity.RowKey.Substring(0, expectedRowKeyWithoutGuid.Length);
            var rowKeyGuid = entity.RowKey.Substring(expectedRowKeyWithoutGuid.Length);

            Assert.Equal(expectedRowKeyWithoutGuid, rowKeyWithoutGuid);
            Guid.Parse(rowKeyGuid);
            Assert.Equal(Guid.Parse(rowKeyGuid).ToString(), rowKeyGuid);

            // Timestamp
            Assert.Equal(logEvent.Timestamp, entity.Timestamp);

            // Properties
            Assert.Equal(9, entity.Count);

            Assert.Equal(messageTemplate, entity["MessageTemplate"]);
            Assert.Equal("Information", entity["Level"]);
            Assert.Equal("Template \"Temporary\" \"Property\"", entity["RenderedMessage"]);
            Assert.Equal(exception.ToString(), entity["Exception"]);
            Assert.Equal("Temporary", entity["Temp"]);
            Assert.Equal("Property", entity["Prop"]);
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldGenerateValidRowKey()
        {
            var timestamp = new DateTimeOffset(2014, 12, 01, 18, 42, 20, 666, TimeSpan.FromHours(2));
            var exception = new ArgumentException("Some exceptional exception happened");
            var level = LogEventLevel.Information;
            var additionalRowKeyPostfix = "POSTFIX";

            var postLength = additionalRowKeyPostfix.Length + 1 + Guid.NewGuid().ToString().Length;
            var messageSpace = 512 - (level.ToString().Length + 1) - (1 + postLength);

            // Message up to available space, plus some characters (Z) that will be removed
            var messageTemplate = new string('x', messageSpace-4) + "ABCD" + new string('Z', 20);

            var template = new MessageTemplateParser().Parse(messageTemplate);
            var properties = new List<LogEventProperty>();

            var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);

            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, additionalRowKeyPostfix, new PropertiesKeyGenerator());

            // Row Key
            var expectedRowKeyWithoutGuid = "Information|" + new string('x', messageSpace-4) + "ABCD|POSTFIX|";
            var rowKeyWithoutGuid = entity.RowKey.Substring(0, expectedRowKeyWithoutGuid.Length);
            var rowKeyGuid = entity.RowKey.Substring(expectedRowKeyWithoutGuid.Length);

            Assert.Equal(512, entity.RowKey.Length);
            Assert.Equal(expectedRowKeyWithoutGuid, rowKeyWithoutGuid);
            Guid.Parse(rowKeyGuid);
            Assert.Equal(Guid.Parse(rowKeyGuid).ToString(), rowKeyGuid);
            Assert.DoesNotContain("Z", entity.RowKey);
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldSupportAzureTableTypesForScalar()
        {
            var messageTemplate = "{ByteArray} {Boolean} {DateTime} {DateTimeOffset} {Double} {Guid} {Int} {Long} {String}";
            var bytearrayValue = new byte[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 250, 251, 252, 253, 254, 255 };
            var booleanValue = true;
            var datetimeValue = DateTime.UtcNow;
            var datetimeoffsetValue = new DateTimeOffset(datetimeValue, TimeSpan.FromHours(0));
            var doubleValue = Math.PI;
            var guidValue = Guid.NewGuid();
            var intValue = int.MaxValue;
            var longValue = long.MaxValue;
            var stringValue = "Some string value";

            var properties = new List<LogEventProperty> {
                new LogEventProperty("ByteArray", new ScalarValue(bytearrayValue)),
                new LogEventProperty("Boolean", new ScalarValue(booleanValue)),
                new LogEventProperty("DateTime", new ScalarValue(datetimeValue)),
                new LogEventProperty("DateTimeOffset", new ScalarValue(datetimeoffsetValue)),
                new LogEventProperty("Double", new ScalarValue(doubleValue)),
                new LogEventProperty("Guid", new ScalarValue(guidValue)),
                new LogEventProperty("Int", new ScalarValue(intValue)),
                new LogEventProperty("Long", new ScalarValue(longValue)),
                new LogEventProperty("String", new ScalarValue(stringValue))
            };

            var template = new MessageTemplateParser().Parse(messageTemplate);

            var logEvent = new Events.LogEvent(DateTime.Now, LogEventLevel.Information, null, template, properties);

            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, new PropertiesKeyGenerator());

            Assert.Equal(6 + properties.Count, entity.Count);

            Assert.IsType<byte[]>(entity["ByteArray"]);
            Assert.Equal(bytearrayValue, entity["ByteArray"]);
            Assert.Equal(booleanValue, entity["Boolean"]);
            Assert.Equal(datetimeValue, entity["DateTime"]);
            Assert.Equal(datetimeoffsetValue, entity["DateTimeOffset"]);
            Assert.Equal(doubleValue, entity["Double"]);
            Assert.Equal(guidValue, entity["Guid"]);
            Assert.Equal(intValue, entity["Int"]);
            Assert.Equal(longValue, entity["Long"]);
            Assert.Equal(stringValue, entity["String"]);
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldSupportAzureTableTypesForDictionary()
        {
            var messageTemplate = "{Dictionary}";

            var dict1 = new DictionaryValue(new List<KeyValuePair<ScalarValue, LogEventPropertyValue>>{
                new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d1k1"), new ScalarValue("d1k1v1")),
                new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d1k2"), new ScalarValue("d1k2v2")),
                new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d1k3"), new ScalarValue("d1k3v3")),
            });

            var dict2 = new DictionaryValue(new List<KeyValuePair<ScalarValue, LogEventPropertyValue>>{
                new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d2k1"), new ScalarValue("d2k1v1")),
                new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d2k2"), new ScalarValue("d2k2v2")),
                new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d2k3"), new ScalarValue("d2k3v3")),
            });

            var dict0 = new DictionaryValue(new List<KeyValuePair<ScalarValue, LogEventPropertyValue>>{
                 new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d1"), dict1),
                 new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d2"), dict2),
                 new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("d0"), new ScalarValue(0))
            });

            var properties = new List<LogEventProperty> {
                new LogEventProperty("Dictionary", dict0)
            };

            var template = new MessageTemplateParser().Parse(messageTemplate);

            var logEvent = new Events.LogEvent(DateTime.Now, LogEventLevel.Information, null, template, properties);

            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, new PropertiesKeyGenerator());

            Assert.Equal(6 + properties.Count, entity.Count);
            Assert.Equal("[(\"d1\": [(\"d1k1\": \"d1k1v1\"), (\"d1k2\": \"d1k2v2\"), (\"d1k3\": \"d1k3v3\")]), (\"d2\": [(\"d2k1\": \"d2k1v1\"), (\"d2k2\": \"d2k2v2\"), (\"d2k3\": \"d2k3v3\")]), (\"d0\": 0)]", entity["Dictionary"]);
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldSupportAzureTableTypesForSequence()
        {
            var messageTemplate = "{Sequence}";

            var sequence1 = new SequenceValue(new List<LogEventPropertyValue>
            {
                new ScalarValue(1),
                new ScalarValue(2),
                new ScalarValue(3),
                new ScalarValue(4),
                new ScalarValue(5)
            });

            var sequence2 = new SequenceValue(new List<LogEventPropertyValue>
            {
                new ScalarValue("a"),
                new ScalarValue("b"),
                new ScalarValue("c"),
                new ScalarValue("d"),
                new ScalarValue("e")
            });

            var sequence0 = new SequenceValue(new List<LogEventPropertyValue>
            {
                sequence1,
                sequence2
            });

            var properties = new List<LogEventProperty> {
                new LogEventProperty("Sequence", sequence0)
            };

            var template = new MessageTemplateParser().Parse(messageTemplate);

            var logEvent = new Events.LogEvent(DateTime.Now, LogEventLevel.Information, null, template, properties);

            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, new PropertiesKeyGenerator());

            Assert.Equal(6 + properties.Count, entity.Count);
            Assert.Equal("[[1, 2, 3, 4, 5], [\"a\", \"b\", \"c\", \"d\", \"e\"]]", entity["Sequence"]);
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldNotAddMoreThan252Properties()
        {
            var messageTemplate = string.Empty;

            var properties = new List<LogEventProperty>();

            for (var i = 0; i < 300; ++i)
            {
                var propName = "Prop" + i;
                properties.Add(new LogEventProperty(propName, new ScalarValue(i)));

                messageTemplate += $"{{{propName}}}";
            }

            var template = new MessageTemplateParser().Parse(messageTemplate);

            var logEvent = new Events.LogEvent(DateTime.Now, LogEventLevel.Information, null, template, properties);

            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, new PropertiesKeyGenerator());

            Assert.Equal(252, entity.Count);
            Assert.Contains("AggregatedProperties", entity.Keys.ToList());
        }

        [Fact]
        public void CreateEntityWithAdditionalPropertiesOnlyShouldNotAddUnspecifiedProperties()
        {
            const string messageTemplate = "{IncludedProperty} {AdditionalProperty}";
            const string includedPropertyValue = "included value";
            const string excludedPropertyValue = "excluded value";
            var includedProperties = new[] {"IncludedProperty"};

            var properties = new List<LogEventProperty> {
                new LogEventProperty("IncludedProperty", new ScalarValue(includedPropertyValue)),
                new LogEventProperty("AdditionalProperty", new ScalarValue(excludedPropertyValue))
            };

            var template = new MessageTemplateParser().Parse(messageTemplate);
            var logEvent = new LogEvent(DateTime.Now, LogEventLevel.Information, null, template, properties);

            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, new PropertiesKeyGenerator(), includedProperties);

            Assert.True(entity.ContainsKey("IncludedProperty"));
            Assert.False(entity.ContainsKey("AdditionalProperty"));
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldCropPropertiesToMaximumSupportedLength()
        {
            var timestamp = DateTimeOffset.Now;
            var exception = new ArgumentException("Some exceptional exception happened");
            var level = LogEventLevel.Information;
            var template = new MessageTemplateParser().Parse("Template");
            var keyGenerator = new PropertiesKeyGenerator();

            var testProp = new StringBuilder();
            int maximumLengthEstimateCharacters = _propertyMaximumSizeBytes / 2 - 9;
            testProp.Append(new string('a', maximumLengthEstimateCharacters));

            int utfBytesSize = Encoding.Unicode.GetBytes(testProp.ToString()).Length;
            while (utfBytesSize <= _propertyMaximumSizeBytes + 10)
            {
                var properties = new List<LogEventProperty> { new LogEventProperty("TestProp", new ScalarValue(testProp.ToString())) };
                var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, keyGenerator);

                var entityProp = (string)entity["TestProp"];

                if (utfBytesSize <= _propertyMaximumSizeBytes)
                {
                    Assert.Equal(testProp.ToString(), entityProp);
                }
                else
                {
                    Assert.True(entityProp.Length <= _propertyMaximumSizeBytes);
                    Assert.StartsWith(entityProp, testProp.ToString());
                }

                testProp.Append('a');
                utfBytesSize = Encoding.Unicode.GetBytes(testProp.ToString()).Length;
            }
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldCropPropertiesWithUnicodeSurrogatePairsToMaximumSupportedLength()
        {
            var timestamp = DateTimeOffset.Now;
            var exception = new ArgumentException("Some exceptional exception happened");
            var level = LogEventLevel.Information;
            var template = new MessageTemplateParser().Parse("Template");
            var keyGenerator = new PropertiesKeyGenerator();

            var testProp = new StringBuilder();
            int maximumLengthEstimateCharacters = _propertyMaximumSizeBytes / 2 - 9;
            testProp.Append(new string('a', maximumLengthEstimateCharacters));

            int utfBytesSize = Encoding.Unicode.GetBytes(testProp.ToString()).Length;
            while (utfBytesSize <= _propertyMaximumSizeBytes + 10)
            {
                var properties = new List<LogEventProperty> { new LogEventProperty("TestProp", new ScalarValue(testProp.ToString())) };
                var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, keyGenerator);

                var entityProp = (string)entity["TestProp"];

                if (utfBytesSize <= _propertyMaximumSizeBytes)
                {
                    Assert.Equal(testProp.ToString(), entityProp);
                }
                else
                {
                    Assert.True(entityProp.Length <= _propertyMaximumSizeBytes);
                    Assert.StartsWith(entityProp, testProp.ToString());
                }

                testProp.Append("\U0001F01C");
                utfBytesSize = Encoding.Unicode.GetBytes(testProp.ToString()).Length;
            }
        }

        [Fact]
        public void CreateEntityWithPropertiesShouldCropStandardPropertiesToMaximumSupportedLength()
        {
            var timestamp = DateTimeOffset.Now;
            var exception = new ArgumentException("Some exceptional exception happened " + new string('a', _propertyMaximumSizeBytes));
            var level = LogEventLevel.Information;
            var template = new MessageTemplateParser().Parse("Template {TestProp} " + new string('b', _propertyMaximumSizeBytes));
            var keyGenerator = new PropertiesKeyGenerator();
            var properties = new List<LogEventProperty> { new LogEventProperty("TestProp", new ScalarValue("foo")) };

            string expectedRenderedException = $"System.ArgumentException: {exception.Message}";
            var expectedRenderedTemplate = "Template \"foo\" " + new string('b', _propertyMaximumSizeBytes);

            var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
            var entity = AzureTableStorageEntityFactory.CreateEntityWithProperties(logEvent, null, null, keyGenerator);

            string storedMessageTemplate = (string)entity["MessageTemplate"];
            string storedRenderedTemplate = (string)entity["RenderedMessage"];
            string storedException = (string)entity["Exception"];

            byte[] storedMessageTemplateBytes = Encoding.Unicode.GetBytes(storedMessageTemplate);
            byte[] storedRenderedTemplateBytes = Encoding.Unicode.GetBytes(storedRenderedTemplate);
            byte[] storedExceptionBytes = Encoding.Unicode.GetBytes(storedException);

            Assert.True(storedMessageTemplateBytes.Length <= _propertyMaximumSizeBytes);
            Assert.StartsWith(storedMessageTemplate, template.ToString());

            Assert.True(storedRenderedTemplateBytes.Length <= _propertyMaximumSizeBytes);
            Assert.StartsWith(storedRenderedTemplate, expectedRenderedTemplate);

            Assert.True(storedExceptionBytes.Length <= _propertyMaximumSizeBytes);
            Assert.StartsWith(storedException, expectedRenderedException);
        }
    }
}
