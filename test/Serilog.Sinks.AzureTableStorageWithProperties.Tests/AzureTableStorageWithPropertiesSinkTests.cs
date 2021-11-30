using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Data.Tables.Sas;
using Serilog.Events;
using Serilog.Parsing;
using Xunit;

namespace Serilog.Sinks.AzureTableStorage.Tests
{
    [Collection("AzureStorageIntegrationTests")]
    public class AzureTableStorageWithPropertiesSinkTests
    {
        static async Task<IReadOnlyList<TableEntity>> TableQueryTakeDynamicAsync(TableClient table, int takeCount)
        {
            var pages = table.QueryAsync<TableEntity>(maxPerPage: takeCount).AsPages().GetAsyncEnumerator();
            if (!await pages.MoveNextAsync())
                return null;

            return pages.Current.Values;
        }

        private TableServiceClient GetDevelopmentTableServiceClient()
        {
            return new TableServiceClient("UseDevelopmentStorage=true");
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItIsRetrievableFromTheTableWithProperties()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var exception = new ArgumentException("Some exception");

            const string messageTemplate = "{Properties} should go in their {Numbered} {Space}";

            logger.Information(exception, messageTemplate, "Properties", 1234, ' ');

            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            // Check the presence of same properties as in previous version
            Assert.Equal(messageTemplate, result["MessageTemplate"]);
            Assert.Equal("Information", result["Level"]);
            Assert.Equal("System.ArgumentException: Some exception", result["Exception"]);
            Assert.Equal("\"Properties\" should go in their 1234  ", result["RenderedMessage"]);

            // Check the presence of the new properties.
            Assert.Equal("Properties", result["Properties"]);
            Assert.Equal(1234, result["Numbered"]);
            Assert.Equal(" ", result["Space"]);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkWithAWindowsNewlineInTheTemplateItIsRetrievable()
        {
            // Prompted from https://github.com/serilog/serilog-sinks-azuretablestorage/issues/10
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            const string messageTemplate = "Line 1\r\nLine2";

            logger.Information(messageTemplate);

            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.NotNull(result);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkWithALineFeedInTheTemplateItIsRetrievable()
        {
            // Prompted from https://github.com/serilog/serilog-sinks-azuretablestorage/issues/10
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            const string messageTemplate = "Line 1\nLine2";

            logger.Information(messageTemplate);

            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.NotNull(result);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkWithACarriageReturnInTheTemplateItIsRetrievable()
        {
            // Prompted from https://github.com/serilog/serilog-sinks-azuretablestorage/issues/10
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            const string messageTemplate = "Line 1\rLine2";

            logger.Information(messageTemplate);

            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.NotNull(result);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItStoresTheCorrectTypesForScalar()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var bytearrayValue = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 250, 251, 252, 253, 254, 255 };
            var booleanValue = true;
            var datetimeValue = DateTime.UtcNow;
            var datetimeoffsetValue = new DateTimeOffset(datetimeValue, TimeSpan.FromHours(0));
            var doubleValue = Math.PI;
            var guidValue = Guid.NewGuid();
            var intValue = int.MaxValue;
            var longValue = long.MaxValue;
            var stringValue = "Some string value";

            logger.Information("{ByteArray} {Boolean} {DateTime} {DateTimeOffset} {Double} {Guid} {Int} {Long} {String}",
                bytearrayValue,
                booleanValue,
                datetimeValue,
                datetimeoffsetValue,
                doubleValue,
                guidValue,
                intValue,
                longValue,
                stringValue);

            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            //Assert.Equal(bytearrayValue, result.Properties["ByteArray"].BinaryValue);
            Assert.Equal(booleanValue, result["Boolean"]);
            Assert.Equal(datetimeValue, result.GetDateTimeOffset("DateTime").Value.UtcDateTime);
            Assert.Equal(datetimeoffsetValue, result["DateTimeOffset"]);
            Assert.Equal(doubleValue, result["Double"]);
            Assert.Equal(guidValue, result["Guid"]);
            Assert.Equal(intValue, result["Int"]);
            Assert.Equal(longValue, result["Long"]);
            Assert.Equal(stringValue, result["String"]);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItStoresTheCorrectTypesForDictionary()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var dict1 = new Dictionary<string, string>{
                {"d1k1", "d1k1v1"},
                {"d1k2", "d1k2v2"},
                {"d1k3", "d1k3v3"}
            };

            var dict2 = new Dictionary<string, string>{
                {"d2k1", "d2k1v1"},
                {"d2k2", "d2k2v2"},
                {"d2k3", "d2k3v3"}
            };

            var dict0 = new Dictionary<string, Dictionary<string, string>>{
                 {"d1", dict1},
                 {"d2", dict2}
            };

            logger.Information("{Dictionary}", dict0);
            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.Equal("[(\"d1\": [(\"d1k1\": \"d1k1v1\"), (\"d1k2\": \"d1k2v2\"), (\"d1k3\": \"d1k3v3\")]), (\"d2\": [(\"d2k1\": \"d2k1v1\"), (\"d2k2\": \"d2k2v2\"), (\"d2k3\": \"d2k3v3\")])]", result["Dictionary"]);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItStoresTheCorrectTypesForSequence()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var seq1 = new int[] { 1, 2, 3, 4, 5 };
            var seq2 = new string[] { "a", "b", "c", "d", "e" };

            logger.Information("{Seq1} {Seq2}", seq1, seq2);
            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.Equal("[1, 2, 3, 4, 5]", result["Seq1"]);
            Assert.Equal("[\"a\", \"b\", \"c\", \"d\", \"e\"]", result["Seq2"]);
        }

        private class Struct1
        {
            public int IntVal { get; set; }
            public string StringVal { get; set; }
        }

        private class Struct2
        {
            public DateTime DateTimeVal { get; set; }
            public double DoubleVal { get; set; }
        }

        private class Struct0
        {
            public Struct1 Struct1Val { get; set; }
            public Struct2 Struct2Val { get; set; }
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItStoresTheCorrectTypesForStructure()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var struct1 = new Struct1
            {
                IntVal = 10,
                StringVal = "ABCDE"
            };

            var struct2 = new Struct2
            {
                DateTimeVal = new DateTime(2014, 12, 3, 17, 37, 12),
                DoubleVal = Math.PI
            };

            var struct0 = new Struct0
            {
                Struct1Val = struct1,
                Struct2Val = struct2
            };

            logger.Information("{@Struct0}", struct0);
            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

#if NET472
            Assert.Equal("Struct0 { Struct1Val: Struct1 { IntVal: 10, StringVal: \"ABCDE\" }, Struct2Val: Struct2 { DateTimeVal: 12/03/2014 17:37:12, DoubleVal: 3.14159265358979 } }", result["Struct0"]);
#else
            Assert.Equal("Struct0 { Struct1Val: Struct1 { IntVal: 10, StringVal: \"ABCDE\" }, Struct2Val: Struct2 { DateTimeVal: 12/03/2014 17:37:12, DoubleVal: 3.141592653589793 } }", result["Struct0"]);
#endif
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItAllowsStringFormatNumericPropertyNames()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var expectedResult = "Hello \"world\"";

            logger.Information("Hello {0}", "world");
            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.Equal(expectedResult, result["RenderedMessage"]);
        }

        [Fact]
        public async Task WhenALoggerWritesToTheSinkItAllowsNamedAndNumericPropertyNames()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var name = "John Smith";
            var expectedResult = "Hello \"world\" this is \"John Smith\" 1234";

            logger.Information("Hello {0} this is {Name} {_1234}", "world", name, 1234);
            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            Assert.Equal(expectedResult, result["RenderedMessage"]);
            Assert.Equal(name, result["Name"]);
            Assert.Equal(1234, result["_1234"]);
        }

        [Fact]
        public async Task WhenABatchLoggerWritesToTheSinkItStoresAllTheEntries()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            using (var sink = new AzureBatchingTableStorageWithPropertiesSink(tableServiceClient, null, 1000, TimeSpan.FromMinutes(1)))
            {
                var timestamp = new DateTimeOffset(2014, 12, 01, 18, 42, 20, 666, TimeSpan.FromHours(2));
                var messageTemplate = "Some text";
                var template = new MessageTemplateParser().Parse(messageTemplate);
                var properties = new List<LogEventProperty>();
                for (int i = 0; i < 10; ++i)
                {
                    sink.Emit(new Events.LogEvent(timestamp, LogEventLevel.Information, null, template, properties));
                }
            }

            var result = await TableQueryTakeDynamicAsync(table, takeCount: 11);
            Assert.Equal(10, result.Count);
        }

        [Fact]
        public async Task WhenABatchLoggerWritesToTheSinkItStoresAllTheEntriesInDifferentPartitions()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            using (var sink = new AzureBatchingTableStorageWithPropertiesSink(tableServiceClient, null, 1000, TimeSpan.FromMinutes(1)))
            {
                var messageTemplate = "Some text";
                var template = new MessageTemplateParser().Parse(messageTemplate);
                var properties = new List<LogEventProperty>();

                for(int k = 0; k < 4; ++k)
                {
                    var timestamp = new DateTimeOffset(2014, 12, 01, 1+k, 42, 20, 666, TimeSpan.FromHours(2));
                    for (int i = 0; i < 2; ++i)
                    {
                        sink.Emit(new Events.LogEvent(timestamp, LogEventLevel.Information, null, template, properties));
                    }
                }
            }

            var result = await TableQueryTakeDynamicAsync(table, takeCount: 9);
            Assert.Equal(8, result.Count);
        }

        [Fact]
        public async Task WhenABatchLoggerWritesToTheSinkItStoresAllTheEntriesInLargeNumber()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();

            using (var sink = new AzureBatchingTableStorageWithPropertiesSink(tableServiceClient, null, 1000, TimeSpan.FromMinutes(1)))
            {
                var timestamp = new DateTimeOffset(2014, 12, 01, 18, 42, 20, 666, TimeSpan.FromHours(2));
                var messageTemplate = "Some text";
                var template = new MessageTemplateParser().Parse(messageTemplate);
                var properties = new List<LogEventProperty>();
                for (int i = 0; i < 300; ++i)
                {
                    sink.Emit(new Events.LogEvent(timestamp, LogEventLevel.Information, null, template, properties));
                }
            }

            var result = await TableQueryTakeDynamicAsync(table, takeCount: 301);
            Assert.Equal(300, result.Count);
        }

        [Fact]
        public void WhenALoggerUsesAnUnreachableStorageServiceItDoesntFail()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorage("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=/////////////////////////////////////////////////////////////////////////////////////w==;BlobEndpoint=http://127.0.0.1:16660/devstoreaccount1;TableEndpoint=http://127.0.0.1:16662/devstoreaccount1;QueueEndpoint=http://127.0.0.1:16661/devstoreaccount1;")
                .CreateLogger();

            Log.Information("This should silently work, even though the connection string points to invalid endpoints");

            Assert.True(true);
        }

        [Fact]
        public void WhenALoggerWithPropertiesUsesAnUnreachableStorageServiceItDoesntFail()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=/////////////////////////////////////////////////////////////////////////////////////w==;BlobEndpoint=http://127.0.0.1:16660/devstoreaccount1;TableEndpoint=http://127.0.0.1:16662/devstoreaccount1;QueueEndpoint=http://127.0.0.1:16661/devstoreaccount1;")
                .CreateLogger();

            Log.Information("This should silently work, even though the connection string points to invalid endpoints");

            Assert.True(true);
        }

        [Fact]
        public void WhenALoggerUsesAnInvalidStorageConnectionStringItDoesntFail()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorage("InvalidConnectionString!!!")
                .CreateLogger();

            Log.Information("This should silently work, even though the connection string is malformed");

            Assert.True(true);
        }

        [Fact]
        public void WhenALoggerWithPropertiesUsesAnInvalidStorageConnectionStringItDoesntFail()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties("InvalidConnectionString!!!")
                .CreateLogger();

            Log.Information("This should silently work, even though the connection string is malformed");

            Assert.True(true);
        }
        /*
        private const string PolicyName = "MyPolicy";

        private async Task SetupTableStoredAccessPolicyAsync(TableClient table)
        {
            var policiesResponse = await table.GetAccessPoliciesAsync();
            var policies = policiesResponse.Value.ToList();

            if (policies.Count > 0 && policies.Any(p => p.Id == PolicyName))
            {
                // extend the existing one by 1h
                var policy = policies.First(p => p.Id == PolicyName);
                policy.AccessPolicy.ExpiresOn = DateTime.UtcNow.AddHours(48);
            }
            else
            {
                // create a new one
                var policy = new TableSignedIdentifier(PolicyName,
                new TableAccessPolicy(DateTimeOffset.UtcNow, DateTime.UtcNow.AddHours(48), "au"));
                policies.Add(policy);
            }

            await table.SetAccessPolicyAsync(policies);
        }

        private async Task<string> GetSASUrlForTableAsync(TableClient table)
        {
            await SetupTableStoredAccessPolicyAsync(table);

            var policiesResponse = await table.GetAccessPoliciesAsync().ConfigureAwait(false);
            var policy = policiesResponse.Value.First(p => p.Id == PolicyName);

            var sasUrl = table.GenerateSasUri(TableSasPermissions.Add | TableSasPermissions.Update, DateTime.UtcNow.AddHours(48));

            return sasUrl.ToString();
        }

        [Fact]
        public async Task WhenALoggerUsesASASSinkItIsRetrievableFromTheTableWithProperties()
        {
            var tableServiceClient = GetDevelopmentTableServiceClient();
            var table = tableServiceClient.GetTableClient("LogEventEntity");

            await table.DeleteAsync();
            await table.CreateIfNotExistsAsync();

            var sasUrl = await GetSASUrlForTableAsync(table);

            tableServiceClient = new TableServiceClient(new Uri("http://127.0.0.1:10002/devstoreaccount1"), new AzureSasCredential(sasUrl));
            var logger = new LoggerConfiguration()
                .WriteTo.AzureTableStorageWithProperties(tableServiceClient)
                .CreateLogger();

            var exception = new ArgumentException("Some exception");

            const string messageTemplate = "{Properties} should go in their {Numbered} {Space}";

            logger.Information(exception, messageTemplate, "Properties", 1234, ' ');

            var result = (await TableQueryTakeDynamicAsync(table, takeCount: 1)).First();

            // Check the presence of same properties as in previous version
            Assert.Equal(messageTemplate, result["MessageTemplate"]);
            Assert.Equal("Information", result["Level"]);
            Assert.Equal("System.ArgumentException: Some exception", result["Exception"]);
            Assert.Equal("\"Properties\" should go in their 1234  ", result["RenderedMessage"]);

            // Check the presence of the new properties.
            Assert.Equal("Properties", result["Properties"]);
            Assert.Equal(1234, result["Numbered"]);
            Assert.Equal(" ", result["Space"]);
        }
        */
    }
}
