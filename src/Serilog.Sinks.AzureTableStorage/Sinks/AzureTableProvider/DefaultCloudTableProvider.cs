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

using Azure.Data.Tables;
using System;
using System.Threading;


namespace Serilog.Sinks.AzureTableStorage.AzureTableProvider
{
    class DefaultCloudTableProvider : ICloudTableProvider
    {
        readonly int _waitTimeoutMilliseconds = Timeout.Infinite;
        TableClient _cloudTable;

        public TableClient GetCloudTable(TableServiceClient client, string storageTableName, bool bypassTableCreationValidation)
        {
            if (_cloudTable == null)
            {
                _cloudTable = client.GetTableClient(storageTableName);

                // In some cases (e.g.: SAS URI), we might not have enough permissions to create the table if
                // it does not already exists. So, if we are in that case, we ignore the error as per bypassTableCreationValidation.
                try
                {
                    _cloudTable.CreateIfNotExistsAsync().SyncContextSafeWait(_waitTimeoutMilliseconds);
                }
                catch (Exception ex)
                {
                    Debugging.SelfLog.WriteLine($"Failed to create table: {ex}");
                    if (!bypassTableCreationValidation)
                    {
                        throw;
                    }
                }
            }
            return _cloudTable;
        }
    }
}
