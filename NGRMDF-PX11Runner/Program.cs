using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NGRMDF_PX11Runner
{
    class Program
    {
        static void Main(string[] args)
        {
            int SurveyID = 23;
            string SurveyName = "EQ19222";
            string FileType = "p111"; // { p111 | p211 }

            string vaultName = "ngrmdfvtkvdev";
            string secretName = "DatabricksStorageReaderKey";
            Console.WriteLine($"Getting {secretName} from {vaultName}");
            string storageAccountKey = GetSecret(vaultName, secretName);

            string databricksJobsStorageKey = "YOUR_KEY_HERE"; // runme: (Get-AzureRmStorageAccountKey -ResourceGroupName NgrmdfVtRGDev -Name ngrmdfvtbldev).Value[0]
            string databricksJobsStorageConnectionString = $"DefaultEndpointsProtocol=https;AccountName=ngrmdfvtbldev;AccountKey={databricksJobsStorageKey};EndpointSuffix=core.windows.net";

            string storageAccountName = "ngrmdfvtdatabldev";
            string containerName = SurveyName.ToLower();
            string folderName = FileType.ToLower();
            Console.WriteLine($"Connecting to {containerName} in {storageAccountName}");
            BlobService blobService = new BlobService(storageAccountName, containerName, storageAccountKey);
            CloudBlobContainer cloudBlobContainer = blobService.GetCloudBlobContainer();

            List<IListBlobItem> files;
            files = cloudBlobContainer.ListBlobsAsync(folderName).GetAwaiter().GetResult();

            List<CloudBlockBlob> filesProperties = files.Select(f => (CloudBlockBlob)f).ToList();
            filesProperties.Sort(CompareBlobLastModified);
            Console.WriteLine($"1st: {filesProperties[0].Properties.LastModified}, 2nd: {filesProperties[1].Properties.LastModified}"); // debug


            AzureTables azTbl = null;
            CloudTable databricksJobsTable = null;

            azTbl = new AzureTables(databricksJobsStorageConnectionString);
            databricksJobsTable = azTbl.CreateOrGetTable("databricksjobs").GetAwaiter().GetResult();

            String UriPrefix = $"https://{storageAccountName}.blob.core.windows.net/";
            files.ForEach(f =>
            {
                string pX11Blob = f.Uri.ToString().Replace(UriPrefix, "/mnt/");
                pX11Blob = System.Web.HttpUtility.UrlDecode(pX11Blob);

                Console.WriteLine(pX11Blob); // Debug

                Console.WriteLine("Adding Databricks job.");
                DatabricksJobEntity job = new DatabricksJobEntity("IngestP111", Guid.NewGuid())
                {
                    NotebookParameters = JsonConvert.SerializeObject(new Dictionary<string, dynamic> {
                                            { "survey_id", SurveyID },
                                            { $"{FileType}_blob", pX11Blob }
                                        }),
                    BatchLinked = true
                };
                DatabricksJobEntity inserted = (DatabricksJobEntity)azTbl.InsertOrMerge(databricksJobsTable, job).GetAwaiter().GetResult();
                Console.WriteLine($"Inserted {inserted.JobName} with PK {inserted.PartitionKey} and RK {inserted.RowKey}.");
            });
        }

        private static Int32 CompareBlobLastModified(CloudBlockBlob a, CloudBlockBlob b)
        {
            long aTicks = ((DateTimeOffset)(a.Properties.LastModified)).Ticks;
            long bTicks = ((DateTimeOffset)(b.Properties.LastModified)).Ticks;
            long diff = aTicks - bTicks;
            Int32 ret = diff > 0 ? 1 : diff < 0 ? -1 : 0;

            // Console.WriteLine($"{a.Properties.LastModified} ? {b.Properties.LastModified} => {ret}"); // debug

            return ret;
        }

        #region GetSecret
        private static string GetSecret(string vaultName, string secretName)
        {
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            var secret = keyVaultClient.GetSecretAsync($"https://{vaultName}.vault.azure.net/secrets/{secretName}")
            .ConfigureAwait(false).GetAwaiter().GetResult();
            return secret.Value;
        }
        #endregion // end of GetSecret

    }

    #region NonBlockingConsole
    // src: https://stackoverflow.com/questions/3670057/does-console-writeline-block
    public static class NonBlockingConsole
    {
        private static BlockingCollection<string> m_Queue = new BlockingCollection<string>();

        static NonBlockingConsole()
        {
            var thread = new Thread(
              () =>
              {
                  while (true) Console.WriteLine(m_Queue.Take());
              });
            thread.IsBackground = true;
            thread.Start();
        }

        public static void WriteLine(string value)
        {
            m_Queue.Add(value);
        }
    }
    #endregion // end of NonBlockingConsole

    #region BlobService
    public class BlobService : IBlobService
    {
        public string StorageAccountName { get; set; }
        public string StorageAccountKey { get; set; }
        public string SasToken { get; set; }
        public string ContainerName { get; set; }

        public BlobService(string storageAccountName, string containerName, string storageAccountKey = null, string sasToken = null)
        {
            StorageAccountName = storageAccountName;
            StorageAccountKey = storageAccountKey;
            ContainerName = containerName;
            SasToken = sasToken;

            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new Exception("No value provided for StorageAccountName");
            }

            if (string.IsNullOrEmpty(storageAccountKey) && string.IsNullOrEmpty(sasToken))
            {
                throw new Exception("Either storage account key or SAS token must be provided");
            }

            if (string.IsNullOrEmpty(containerName))
            {
                throw new Exception("No value provided for ContainerName");
            }
        }

        public CloudBlobContainer GetCloudBlobContainer()
        {

            Microsoft.WindowsAzure.Storage.Auth.StorageCredentials storageCreds;
            if (!string.IsNullOrEmpty(SasToken))
            {
                storageCreds = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(SasToken);
            }
            else
            {
                storageCreds = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(StorageAccountName, StorageAccountKey);
            }

            Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(storageCreds, StorageAccountName, null, true);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            //Get container reference
            var containerRef = blobClient.GetContainerReference(ContainerName);

            if (containerRef == null)
            {
                throw new Exception($"The container {ContainerName} cannot be found in storage account {StorageAccountName}");
            }

            return containerRef;
        }
    }

    public interface IBlobService
    {
        string StorageAccountName { get; set; }
        string StorageAccountKey { get; set; }
        string ContainerName { get; set; }
        CloudBlobContainer GetCloudBlobContainer();
    }

    public static class BlobStorageExtensions
    {
        public async static Task<List<IListBlobItem>> ListBlobsAsync(this CloudBlobContainer container, string folder)
        {
            BlobContinuationToken continuationToken = null;
            List<IListBlobItem> results = new List<IListBlobItem>();

            var folderRef = container.GetDirectoryReference(folder);

            if (folderRef == null)
            {
                throw new Exception($"Folder {folder} not found in container {container.Name}");
            }

            BlobResultSegment response;

            do
            {

                response = await folderRef.ListBlobsSegmentedAsync(
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.Metadata,
                    maxResults: null,
                    currentToken: continuationToken,
                    options: null,
                    operationContext: null);

                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results);

                NonBlockingConsole.WriteLine($"Files listed: {results.Count()}");
            }
            while (continuationToken != null);

            if (results.Count == 0)
            {
                throw new Exception($"Folder {folder} does not exist or there are no blobs in the folder");
            }

            return results;
        }
    }
    #endregion // end of BlobService

    #region Databricks TableEntity Models
    public class DatabricksJobEntity : TableEntity
    {

        public string JobName { get; set; }
        public string NotebookParameters { get; set; }
        public bool BatchLinked { get; set; }
        public bool Retry { get; set; }

        public DatabricksJobEntity() { }

        public DatabricksJobEntity(string jobName, Guid rowKey, bool batchLinked = false, bool retry = true)
        {
            PartitionKey = jobName;
            RowKey = rowKey.ToString();
            JobName = jobName;
            BatchLinked = batchLinked;
            Retry = retry;
        }
    }

    public class DatabricksJobRunEntity : TableEntity
    {

        public int DatabricksJobRunId { get; set; }
        public int DatabricksJobNumberInJob { get; set; }
        public string LifeCycleStage { get; set; }
        public string ResultState { get; set; }
        public string StateMessage { get; set; }

        public DatabricksJobRunEntity()
        {
        }

    }
    #endregion // end of Databricks TableEntity Models

    #region AzureTables
    public class AzureTables
    {

        private readonly Microsoft.Azure.Cosmos.Table.CloudStorageAccount stAcc;
        private readonly CloudTableClient tblClient;

        public AzureTables(string connectionString)
        {
            stAcc = Microsoft.Azure.Cosmos.Table.CloudStorageAccount.Parse(connectionString);
            tblClient = stAcc.CreateCloudTableClient(new TableClientConfiguration());
        }

        public async Task<CloudTable> CreateOrGetTable(string tableName)
        {

            CloudTable table = tblClient.GetTableReference(tableName);

            if (await table.CreateIfNotExistsAsync())
            {
                Console.WriteLine($"Created table: {tableName}.");
            }

            return table;
        }

        public async Task<TableEntity> InsertOrMerge(CloudTable table, TableEntity tableEntity)
        {

            TableOperation insertorMergeOp = TableOperation.InsertOrMerge(tableEntity);

            TableResult result = await table.ExecuteAsync(insertorMergeOp);
            TableEntity inserted = result.Result as TableEntity;
            if (result.RequestCharge.HasValue)
            {
                Console.WriteLine($"Request charge of insert or merge operation: {result.RequestCharge}");
            }

            return inserted;
        }

        public async Task<List<T>> TakeAll<T>(CloudTable table, int takeCount = 500) where T : ITableEntity, new()
        {
            List<T> entities = new List<T>();

            TableQuery<T> tq = new TableQuery<T>().Take(takeCount);

            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<T> tableQueryResult = await table.ExecuteQuerySegmentedAsync(tq, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;

                entities.AddRange(tableQueryResult.Results);
            } while (continuationToken != null);

            return entities;
        }

        public async Task<List<DatabricksJobRunEntity>> TakeDatabricksJobsRunsHavingChecks(CloudTable table, int takeCount = 500)
        {
            List<DatabricksJobRunEntity> entities = new List<DatabricksJobRunEntity>();

            TableQuery<DatabricksJobRunEntity> tq = new TableQuery<DatabricksJobRunEntity>()
                .Where(TableQuery.GenerateFilterCondition("LifeCycleStage", QueryComparisons.NotEqual, null))
                .Take(takeCount);

            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<DatabricksJobRunEntity> tableQueryResult = await table.ExecuteQuerySegmentedAsync(tq, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;

                entities.AddRange(tableQueryResult.Results);
            } while (continuationToken != null);

            return entities;
        }

        public async Task<List<DatabricksJobEntity>> TakeDatabricksJobsSegment(CloudTable table, int takeCount, bool linkedBatches)
        {
            List<DatabricksJobEntity> entities = new List<DatabricksJobEntity>();

            TableQuery<DatabricksJobEntity> tq = new TableQuery<DatabricksJobEntity>()
                .Where(TableQuery.GenerateFilterConditionForBool("BatchLinked", QueryComparisons.Equal, linkedBatches))
                .Take(takeCount);

            TableContinuationToken continuationToken = null;
            int runCount = 1;
            do
            {
                TableQuerySegment<DatabricksJobEntity> tableQueryResult = await table.ExecuteQuerySegmentedAsync(tq, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;

                entities.AddRange(tableQueryResult.Results);
                runCount += 1;
            } while (continuationToken != null && runCount == 1);

            return entities;
        }

        public async Task<List<DatabricksJobEntity>> TakeAllDatabricksJobs(CloudTable table, string jobName, int segmentTakeCount, bool linkedBatches)
        {
            List<DatabricksJobEntity> entities = new List<DatabricksJobEntity>();

            TableQuery<DatabricksJobEntity> tq = new TableQuery<DatabricksJobEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, jobName),
                        TableOperators.And,
                        TableQuery.GenerateFilterConditionForBool("BatchLinked", QueryComparisons.Equal, linkedBatches)
                        )
                    )
                .Take(segmentTakeCount);

            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<DatabricksJobEntity> tableQueryResult =
                    await table.ExecuteQuerySegmentedAsync<DatabricksJobEntity>(tq, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;

                entities.AddRange(tableQueryResult.Results);
            } while (continuationToken != null);

            return entities;
        }

        public async Task<int> CountRows(CloudTable table)
        {
            TableQuery<DynamicTableEntity> tq = new TableQuery<DynamicTableEntity>().Select(new string[] { "PartitionKey" });
            List<string> entities = new List<string>();

            string resolver(string pk, string rk, DateTimeOffset ts, IDictionary<string, EntityProperty> props, string etag) =>
                props.ContainsKey("PartitionKey") ? props["PartitionKey"].StringValue : null;

            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<string> tableQueryResult =
                    await table.ExecuteQuerySegmentedAsync(tq, resolver, continuationToken);

                continuationToken = tableQueryResult.ContinuationToken;

                entities.AddRange(tableQueryResult.Results);
            } while (continuationToken != null);

            return entities.Count;
        }

        public async Task<bool> RemoveBatch(CloudTable table, List<(string partitionKey, string rowKey)> jobList)
        {
            TableBatchOperation batchOperation = new TableBatchOperation();

            foreach ((string partitionKey, string rowKey) job in jobList)
            {
                TableEntity entity = new TableEntity(job.partitionKey, job.rowKey)
                {
                    ETag = "*"
                };
                TableOperation tableOp = TableOperation.Delete(entity);
                batchOperation.Add(tableOp);

                // Cannot execute more than 100 table operations in a batch
                if (batchOperation.Count > 98)
                {
                    TableBatchResult result = await table.ExecuteBatchAsync(batchOperation);
                    batchOperation.Clear();
                }
            }
            if (batchOperation.Count > 0)
            {
                TableBatchResult result = await table.ExecuteBatchAsync(batchOperation);
            }

            return true;
        }

        public async Task<bool> Remove(CloudTable table, ITableEntity entity)
        {
            TableOperation tableOp = TableOperation.Delete(entity);
            TableResult result = await table.ExecuteAsync(tableOp);

            if (result.HttpStatusCode == 204)
            {
                return true;
            }
            else
            {
                Console.WriteLine($"Returned {result.HttpStatusCode} when deleting a row.");
                return false;
            }
        }

        public async Task<bool> Update(CloudTable table, ITableEntity entity)
        {
            TableOperation tableOp = TableOperation.Merge(entity);
            TableResult result = await table.ExecuteAsync(tableOp);

            if (result.HttpStatusCode == 204)
            {
                return true;
            }
            else
            {
                Console.WriteLine($"Returned {result.HttpStatusCode} when merging a row.");
                return false;
            }
        }

    }
    #endregion // end of AzureTables
}


