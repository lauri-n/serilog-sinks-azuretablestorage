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

using System.Threading.Tasks;
using Serilog.Events;
using Serilog.Sinks.AzureTableStorage.AzureTableProvider;
using Serilog.Sinks.AzureTableStorage.KeyGenerator;
using Serilog.Sinks.PeriodicBatching;
using System;
using System.Collections.Generic;
using Serilog.Formatting;
using Azure.Data.Tables;

namespace Serilog.Sinks.AzureTableStorage
{
    /// <summary>
    /// Writes log events as records to an Azure Table Storage table.
    /// </summary>
    public class AzureBatchingTableStorageSink : PeriodicBatchingSink
    {
        readonly ITextFormatter _textFormatter;
        readonly IKeyGenerator _keyGenerator;
        readonly TableServiceClient _tableServiceClient;
        readonly string _storageTableName;
        readonly bool _bypassTableCreationValidation;
        readonly ICloudTableProvider _cloudTableProvider;

        /// <summary>
        /// Construct a sink that saves logs to the specified storage account.
        /// </summary>
        /// <param name="storageAccount">The Cloud Storage Account to use to insert the log entries to.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="batchSizeLimit"></param>
        /// <param name="period"></param>
        /// <param name="storageTableName">Table name that log entries will be written to. Note: Optional, setting this may impact performance</param>
        /// <param name="cloudTableProvider">Cloud table provider to get current log table.</param>
        public AzureBatchingTableStorageSink(
            TableServiceClient tableServiceClient,
            IFormatProvider formatProvider,
            ITextFormatter textFormatter,
            int batchSizeLimit,
            TimeSpan period,
            string storageTableName = null,
            ICloudTableProvider cloudTableProvider = null)
            : this(tableServiceClient, textFormatter, batchSizeLimit, period, storageTableName, new DefaultKeyGenerator(), cloudTableProvider: cloudTableProvider)
        {
        }

        /// <summary>
        /// Construct a sink that saves logs to the specified storage account.
        /// </summary>
        /// <param name="storageAccount">The Cloud Storage Account to use to insert the log entries to.</param>
        /// <param name="textFormatter"></param>
        /// <param name="batchSizeLimit"></param>
        /// <param name="period"></param>
        /// <param name="storageTableName">Table name that log entries will be written to. Note: Optional, setting this may impact performance</param>
        /// <param name="keyGenerator">generator used for partition keys and row keys</param>
        /// <param name="bypassTableCreationValidation">Bypass the exception in case the table creation fails.</param>
        /// <param name="cloudTableProvider">Cloud table provider to get current log table.</param>
        public AzureBatchingTableStorageSink(
            TableServiceClient tableServiceClient,
            ITextFormatter textFormatter,
            int batchSizeLimit,
            TimeSpan period,
            string storageTableName = null,
            IKeyGenerator keyGenerator = null,
            bool bypassTableCreationValidation = false,
            ICloudTableProvider cloudTableProvider = null)
            : base(batchSizeLimit, period)
        {
            if (batchSizeLimit < 1 || batchSizeLimit > 100)
                throw new ArgumentException("batchSizeLimit must be between 1 and 100 for Azure Table Storage");

            _textFormatter = textFormatter;
            _keyGenerator = keyGenerator ?? new DefaultKeyGenerator();

            if (string.IsNullOrEmpty(storageTableName))
            {
                storageTableName = typeof(LogEventEntity).Name;
            }

            _tableServiceClient = tableServiceClient;
            _storageTableName = storageTableName;
            _bypassTableCreationValidation = bypassTableCreationValidation;
            _cloudTableProvider = cloudTableProvider ?? new DefaultCloudTableProvider();
        }

        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            var table = _cloudTableProvider.GetCloudTable(_tableServiceClient, _storageTableName, _bypassTableCreationValidation);
            List<TableTransactionAction> operation = new List<TableTransactionAction>();

            string lastPartitionKey = null;

            foreach (var logEvent in events)
            {
                var partitionKey = _keyGenerator.GeneratePartitionKey(logEvent);

                if (partitionKey != lastPartitionKey)
                {
                    lastPartitionKey = partitionKey;
                    if (operation.Count > 0)
                    {
                        await table.SubmitTransactionAsync(operation).ConfigureAwait(false);
                        operation = new List<TableTransactionAction>();
                    }
                }
                var logEventEntity = new LogEventEntity(
                    logEvent,
                    _textFormatter,
                    partitionKey,
                    _keyGenerator.GenerateRowKey(logEvent)
                    );
                operation.Add(new TableTransactionAction(TableTransactionActionType.UpsertMerge, logEventEntity));
            }
            if (operation.Count > 0)
            {
                await table.SubmitTransactionAsync(operation).ConfigureAwait(false);
            }
        }
    }
}
