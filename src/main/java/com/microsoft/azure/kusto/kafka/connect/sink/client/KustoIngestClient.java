package com.microsoft.azure.kusto.kafka.connect.sink.client;

import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions.KustoClientAggregateException;
import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions.KustoClientException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class KustoIngestClient {
    public static final int COMPRESSED_FILE_MULTIPLIER = 11;
    public final Logger log = LoggerFactory.getLogger(KustoIngestClient.class);
    public KustoClient m_kustoClient;
    public ResourceManager m_resourceManager;

    public KustoIngestClient(KustoConnectionStringBuilder kcsb) throws Exception {
        log.info("Creating a new KustoIngestClient");
        m_kustoClient = new KustoClient(kcsb);
        m_resourceManager = new ResourceManager(m_kustoClient);
    }

    /*
     * public Future ingestFromMultipleBlobsPaths(List<String> blobPaths, Boolean
     * deleteSourceOnSuccess, KustoIngestionProperties ingestionProperties){
     *
     * ExecutorService executorService = Executors.newSingleThreadExecutor();
     *
     * return executorService.submit(new IngestFromMultipleBlobsCallable(blobPaths,
     * deleteSourceOnSuccess, ingestionProperties, c_ingestionQueueUri)); }
     */

    public IKustoIngestionResult ingestFromMultipleBlobsPaths(List<String> blobPaths, Boolean deleteSourceOnSuccess,
                                                              KustoIngestionProperties ingestionProperties) throws Exception {

        // ingestFromMultipleBlobsAsync(blobPaths, deleteSourceOnSuccess,
        // ingestionProperties).get();
        List<BlobDescription> blobDescriptions = blobPaths.stream().map(b -> new BlobDescription(b, null))
                .collect(Collectors.toList());
        return ingestFromMultipleBlobsImpl(blobDescriptions, deleteSourceOnSuccess, ingestionProperties);
    }

    /*
     * public Future ingestFromSingleBlob(String blobPath, Boolean deleteSourceOnSuccess,
            KustoIngestionProperties ingestionProperties, Long rawDataSize){
     * 
     * ExecutorService executorService = Executors.newSingleThreadExecutor();
     * 
     * return executorService.submit(new IngestFromMultipleBlobsCallable(blobPaths,
     * deleteSourceOnSuccess, ingestionProperties, c_ingestionQueueUri)); }
     */

    public IKustoIngestionResult ingestFromSingleBlob(String blobPath, Boolean deleteSourceOnSuccess,
                                                      KustoIngestionProperties ingestionProperties, Long rawDataSize) throws Exception {

        BlobDescription blobDescription = new BlobDescription(blobPath, rawDataSize);
        return ingestFromMultipleBlobsImpl(new ArrayList(Arrays.asList(blobDescription)), deleteSourceOnSuccess, ingestionProperties);
    }

    public IKustoIngestionResult ingestFromMultipleBlobs(List<BlobDescription> blobDescriptions,
                                                         Boolean deleteSourceOnSuccess, KustoIngestionProperties ingestionProperties) throws Exception {
        return ingestFromMultipleBlobsImpl(blobDescriptions, deleteSourceOnSuccess, ingestionProperties);
    }

    public IKustoIngestionResult ingestFromMultipleBlobsImpl(List<BlobDescription> blobDescriptions,
                                                              Boolean deleteSourceOnSuccess, KustoIngestionProperties ingestionProperties) throws Exception {

        // ingestFromMultipleBlobsAsync(blobPaths, deleteSourceOnSuccess,
        // ingestionProperties).get();
        if (blobDescriptions == null || blobDescriptions.size() == 0) {
            throw new KustoClientException("blobs must have at least 1 path");
        }

        ingestionProperties.setAuthorizationContextToken(m_resourceManager.getKustoIdentityToken());

        List<KustoClientException> ingestionErrors = new LinkedList();
        List<IngestionStatusInTableDescription> tableStatuses = new LinkedList<>();

        for (BlobDescription blobDescription : blobDescriptions) {
            try {
                // Create the ingestion message
                IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobDescription.getBlobPath(),
                        ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
                ingestionBlobInfo.rawDataSize = blobDescription.getBlobSize() != null ? blobDescription.getBlobSize()
                        : estimateBlobRawSize(blobDescription);
                ingestionBlobInfo.retainBlobOnSuccess = !deleteSourceOnSuccess;
                ingestionBlobInfo.reportLevel = ingestionProperties.getReportLevel();
                ingestionBlobInfo.reportMethod = ingestionProperties.getReportMethod();
                ingestionBlobInfo.flushImmediately = ingestionProperties.getFlushImmediately();
                ingestionBlobInfo.additionalProperties = ingestionProperties.getAdditionalProperties();
                if (blobDescription.getSourceId() != null) {
                    ingestionBlobInfo.id = blobDescription.getSourceId();
                }

                if (ingestionProperties.getReportMethod() != KustoIngestionProperties.IngestionReportMethod.Queue) {
                    String tableStatusUri = GetTableStatus();
                    ingestionBlobInfo.IngestionStatusInTable = new IngestionStatusInTableDescription();
                    ingestionBlobInfo.IngestionStatusInTable.TableConnectionString = tableStatusUri;
                    ingestionBlobInfo.IngestionStatusInTable.RowKey = ingestionBlobInfo.id.toString();
                    ingestionBlobInfo.IngestionStatusInTable.PartitionKey = ingestionBlobInfo.id.toString();

                    IngestionStatus status = new IngestionStatus(ingestionBlobInfo.id);
                    status.database = ingestionProperties.getDatabaseName();
                    status.table = ingestionProperties.getTableName();
                    status.status = OperationStatus.Pending;
                    status.updatedOn = Date.from(Instant.now());
                    status.ingestionSourceId = ingestionBlobInfo.id;
                    status.setIngestionSourcePath(blobDescription.getBlobPath());

                    AzureStorageHelper.azureTableInsertEntity(tableStatusUri, status);
                    tableStatuses.add(ingestionBlobInfo.IngestionStatusInTable);
                }

                ObjectMapper objectMapper = new ObjectMapper();
                String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

                AzureStorageHelper.postMessageToQueue(m_resourceManager.getAggregationQueue(), serializedIngestionBlobInfo);
            } catch (Exception ex) {
                ingestionErrors.add(
                        new KustoClientException(blobDescription.getBlobPath(), "fail to post message to queue", ex));
            }
        }

        if (ingestionErrors.size() > 0) {
            throw new KustoClientAggregateException(ingestionErrors);
        }

        return new TableReportKustoIngestionResult(tableStatuses);
    }

    public void ingestFromSingleFile(String filePath, KustoIngestionProperties ingestionProperties) throws Exception {
        try {
            String blobName = genBlobName(filePath, ingestionProperties.getDatabaseName(), ingestionProperties.getTableName());
            CloudBlockBlob blob = AzureStorageHelper.uploadLocalFileToBlob(filePath, blobName, m_resourceManager.getStorageUri());
            String blobPath = AzureStorageHelper.getBlobPathWithSas(blob);
            long rawDataSize = ingestionProperties.getFileSize() != null ? ingestionProperties.getFileSize() : estimateLocalFileSize(filePath);

            ingestFromSingleBlob(blobPath, true, ingestionProperties, rawDataSize);

        } catch (Exception ex) {
            log.error(String.format("ingestFromSingleFile: Error while uploading file (compression mode): %s. Error: %s", filePath, ex.getMessage()), ex);
            throw ex;
        }
    }

    public List<IngestionBlobInfo> GetAndDiscardTopIngestionFailures() throws Exception {
        // Get ingestion queues from DM
        KustoResults failedIngestionsQueues = m_kustoClient
                .execute(CslCommandGenerator.generateIngestionResourcesShowCommand());
        String failedIngestionsQueue = failedIngestionsQueues.getValues().get(0)
                .get(failedIngestionsQueues.getIndexByColumnName("Uri"));

        CloudQueue queue = new CloudQueue(new URI(failedIngestionsQueue));

        Iterable<CloudQueueMessage> messages = queue.retrieveMessages(32, 5000, null, null);

        return null;
    }


    public String GetTableStatus() throws Exception {
        KustoResults result = m_kustoClient.execute(CslCommandGenerator.generateIngestionResourcesShowCommand());
        return GetRandomResourceByResourceTypeName(result, "IngestionsStatusTable");
    }

    public String GetRandomResourceByResourceTypeName(KustoResults kustoResults, String name) {
        ArrayList<String> results = new ArrayList<String>();
        for (Iterator<ArrayList<String>> it = kustoResults.getValues().iterator(); it.hasNext(); ) {
            ArrayList<String> next = it.next();
            if (next.get(0).equals(name)) {
                results.add(next.get(1));
            }
        }

        Random randomizer = new Random();
        return results.get(randomizer.nextInt(results.size()));
    }

    public Long estimateBlobRawSize(BlobDescription blobDescription) throws Exception {

        try {
            String blobPath = blobDescription.getBlobPath();
            CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
            blockBlob.downloadAttributes();
            long length = blockBlob.getProperties().getLength();

            if (length == 0) {
                return null;
            }
            if (blobPath.contains(".zip") || blobPath.contains(".gz")) {
                length = length * COMPRESSED_FILE_MULTIPLIER;
            }

            return length;

        } catch (Exception e) {
            throw e;
        }
    }

    public long estimateLocalFileSize(String filePath) {
        File file = new File(filePath);
        long fileSize = file.length();
        if (filePath.endsWith(".zip") || filePath.endsWith(".gz")) {
            fileSize = fileSize * COMPRESSED_FILE_MULTIPLIER;
        }
        return fileSize;
    }

    public String genBlobName(String filePath, String databaseName, String tableName) {
        String fileName = (new File(filePath)).getName();
        return String.format("%s__%s__%s__%s", databaseName, tableName, UUID.randomUUID().toString(), fileName);
    }
}
