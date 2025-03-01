﻿// Copyright 2014 Serilog Contributors
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

using System;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.AzureTableStorage;
using Serilog.Sinks.AzureTableStorage.KeyGenerator;
using Serilog.Sinks.AzureTableStorage.AzureTableProvider;
using Azure.Data.Tables;

namespace Serilog
{
    /// <summary>
    /// Adds the WriteTo.AzureTableStorageWithProperties() extension method to <see cref="LoggerConfiguration"/>.
    /// </summary>
    public static class LoggerConfigurationAzureTableStorageWithPropertiesExtensions
    {
        /// <summary>
        /// A reasonable default for the number of events posted in
        /// each batch.
        /// </summary>
        public const int DefaultBatchPostingLimit = 50;

        /// <summary>
        /// A reasonable default time to wait between checking for event batches.
        /// </summary>
        public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Adds a sink that writes log events as records in an Azure Table Storage table (default LogEventEntity) using the given storage account.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="storageAccount">The Cloud Storage Account to use to insert the log entries to.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storageTableName">Table name that log entries will be written to. Note: Optional, setting this may impact performance</param>
        /// <param name="writeInBatches">Use a periodic batching sink, as opposed to a synchronous one-at-a-time sink; this alters the partition
        /// key used for the events so is not enabled by default.</param>
        /// <param name="batchPostingLimit">The maximum number of events to post in a single batch.</param>
        /// <param name="period">The time to wait between checking for event batches.</param>
        /// <param name="additionalRowKeyPostfix">Additional postfix string that will be appended to row keys</param>
        /// <param name="keyGenerator">Generates the PartitionKey and the RowKey</param>
        /// <param name="propertyColumns">Specific properties to be written to columns. By default, all properties will be written to columns.</param>
        /// <param name="bypassTableCreationValidation">Bypass the exception in case the table creation fails.</param>
        /// <param name="cloudTableProvider">Cloud table provider to get current log table.</param>
        /// <returns>Logger configuration, allowing configuration to continue.</returns>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        public static LoggerConfiguration AzureTableStorageWithProperties(
            this LoggerSinkConfiguration loggerConfiguration,
            TableServiceClient tableServiceClient,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            IFormatProvider formatProvider = null,
            string storageTableName = null,
            bool writeInBatches = false,
            TimeSpan? period = null,
            int? batchPostingLimit = null,
            string additionalRowKeyPostfix = null,
            IKeyGenerator keyGenerator = null,
            string[] propertyColumns = null,
            bool bypassTableCreationValidation = false,
            ICloudTableProvider cloudTableProvider = null)
        {
            if (loggerConfiguration == null) throw new ArgumentNullException(nameof(loggerConfiguration));
            if (tableServiceClient == null) throw new ArgumentNullException(nameof(tableServiceClient));

            ILogEventSink sink;

            try
            {
                sink = writeInBatches
                    ? (ILogEventSink)
                    new AzureBatchingTableStorageWithPropertiesSink(tableServiceClient, formatProvider, batchPostingLimit ?? DefaultBatchPostingLimit, period ?? DefaultPeriod, storageTableName, additionalRowKeyPostfix, keyGenerator, propertyColumns, bypassTableCreationValidation, cloudTableProvider)
                    : new AzureTableStorageWithPropertiesSink(tableServiceClient, formatProvider, storageTableName, additionalRowKeyPostfix, keyGenerator, propertyColumns, bypassTableCreationValidation, cloudTableProvider);
            }
            catch (Exception ex)
            {
                Debugging.SelfLog.WriteLine($"Error configuring AzureTableStorageWithProperties: {ex}");
                sink = new LoggerConfiguration().CreateLogger();
            }

            return loggerConfiguration.Sink(sink, restrictedToMinimumLevel);
        }

        /// <summary>
        /// Adds a sink that writes log events as records in Azure Table Storage table (default name LogEventEntity) using the given
        /// storage account connection string.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="connectionString">The Cloud Storage Account connection string to use to insert the log entries to.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storageTableName">Table name that log entries will be written to. Note: Optional, setting this may impact performance</param>
        /// <param name="writeInBatches">Use a periodic batching sink, as opposed to a synchronous one-at-a-time sink; this alters the partition
        /// key used for the events so is not enabled by default.</param>
        /// <param name="batchPostingLimit">The maximum number of events to post in a single batch.</param>
        /// <param name="period">The time to wait between checking for event batches.</param>
        /// <param name="additionalRowKeyPostfix">Additional postfix string that will be appended to row keys</param>
        /// <param name="keyGenerator">Generates the PartitionKey and the RowKey</param>
        /// <param name="propertyColumns">Specific properties to be written to columns. By default, all properties will be written to columns.</param>
        /// <param name="bypassTableCreationValidation">Bypass the exception in case the table creation fails.</param>
        /// <param name="cloudTableProvider">Cloud table provider to get current log table.</param>
        /// <returns>Logger configuration, allowing configuration to continue.</returns>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        public static LoggerConfiguration AzureTableStorageWithProperties(
            this LoggerSinkConfiguration loggerConfiguration,
            string connectionString,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            IFormatProvider formatProvider = null,
            string storageTableName = null,
            bool writeInBatches = false,
            TimeSpan? period = null,
            int? batchPostingLimit = null,
            string additionalRowKeyPostfix = null,
            IKeyGenerator keyGenerator = null,
            string[] propertyColumns = null,
            bool bypassTableCreationValidation = false,
            ICloudTableProvider cloudTableProvider = null)
        {
            if (loggerConfiguration == null) throw new ArgumentNullException(nameof(loggerConfiguration));
            if (String.IsNullOrEmpty(connectionString)) throw new ArgumentNullException(nameof(connectionString));

            try
            {
                var tableServiceClient = new TableServiceClient(connectionString);
                return AzureTableStorageWithProperties(loggerConfiguration, tableServiceClient, restrictedToMinimumLevel, formatProvider, storageTableName, writeInBatches, period, batchPostingLimit, additionalRowKeyPostfix, keyGenerator, propertyColumns, bypassTableCreationValidation, cloudTableProvider);
            }
            catch (Exception ex)
            {
                Debugging.SelfLog.WriteLine($"Error configuring AzureTableStorageWithProperties: {ex}");

                ILogEventSink sink = new LoggerConfiguration().CreateLogger();
                return loggerConfiguration.Sink(sink, restrictedToMinimumLevel);
            }
        }
    }
}
