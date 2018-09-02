package com.microsoft.azure.kusto.kafka.connect.sink.client;

import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions.KustoClientAggregateException;
import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions.KustoClientException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

public class IngestFromMultipleBlobsCallable implements Callable<Object> {

    private final String m_ingestionQueueUri;
    private List<String> m_blobPaths;
    private Boolean m_deleteSourceOnSuccess;
    private KustoIngestionProperties m_ingestionProperties;


    public IngestFromMultipleBlobsCallable(List<String> blobPaths, Boolean deleteSourceOnSuccess, KustoIngestionProperties ingestionProperties, final String ingestionQueueUri) {
        m_blobPaths = blobPaths;
        m_deleteSourceOnSuccess = deleteSourceOnSuccess;
        m_ingestionProperties = ingestionProperties;
        m_ingestionQueueUri = ingestionQueueUri;
    }

    @Override
    public Object call() throws Exception {
        if (m_blobPaths == null || m_blobPaths.size() == 0) {
            throw new KustoClientException("blobs must have at least 1 path");
        }

        List<KustoClientException> ingestionErrors = new LinkedList<KustoClientException>();

        for (String blobPath : m_blobPaths) {
            try {
                // Create the ingestion message
                IngestionBlobInfo ingestionBlobInfo = new IngestionBlobInfo(blobPath, m_ingestionProperties.getDatabaseName(), m_ingestionProperties.getTableName());
                ingestionBlobInfo.rawDataSize = estimateBlobRawSize(blobPath);
                ingestionBlobInfo.retainBlobOnSuccess = !m_deleteSourceOnSuccess;
                ingestionBlobInfo.reportLevel = m_ingestionProperties.getReportLevel();
                ingestionBlobInfo.reportMethod = m_ingestionProperties.getReportMethod();
                ingestionBlobInfo.flushImmediately = m_ingestionProperties.getFlushImmediately();
                ingestionBlobInfo.additionalProperties = m_ingestionProperties.getAdditionalProperties();

                ObjectMapper objectMapper = new ObjectMapper();
                String serializedIngestionBlobInfo = objectMapper.writeValueAsString(ingestionBlobInfo);

                AzureStorageHelper.postMessageToQueue(m_ingestionQueueUri, serializedIngestionBlobInfo);
            } catch (Exception ex) {
                ingestionErrors.add(new KustoClientException(blobPath, "fail to post message to queue", ex));
            }
        }

        if (ingestionErrors.size() > 0) {
            throw new KustoClientAggregateException(ingestionErrors);
        }
        return null;
    }


    private Long estimateBlobRawSize(String blobPath) throws Exception {

        try {
            CloudBlockBlob blockBlob = new CloudBlockBlob(new URI(blobPath));
            blockBlob.downloadAttributes();
            long length = blockBlob.getProperties().getLength();

            if (length == 0) {
                return null;
            }
            return length;

        } catch (Exception e) {
            return null;
        }
    }
}
