package com.microsoft.azure.kusto.kafka.connect.sink.client;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.zip.GZIPOutputStream;

public class AzureStorageHelper {

    private static final Logger log = LoggerFactory.getLogger(AzureStorageHelper.class);
    private static final int GZIP_BUFFER_SIZE = 16384;

    public static void postMessageToQueue(String queuePath, String content) throws Exception {

        try
        {
            CloudQueue queue = new CloudQueue(new URI(queuePath));
            CloudQueueMessage queueMessage = new CloudQueueMessage(content);
            queue.addMessage(queueMessage);
        }
        catch (Exception e)
        {
            log.error(String.format("postMessageToQueue: %s.",e.getMessage()), e);
            throw e;
        }
    }

    private static void postMessageStorageAccountNotWorkingYet(String content) throws URISyntaxException, InvalidKeyException, StorageException {
        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount = CloudStorageAccount.parse("");

        // Create the queue client.
        CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

        // Retrieve a reference to a queue.
        CloudQueue queue = queueClient.getQueueReference("myqueue");

        // Create the queue if it doesn't already exist.
        queue.createIfNotExists();

        // Create a message and add it to the queue.
        CloudQueueMessage message = new CloudQueueMessage(content);
        queue.addMessage(message);
    }

    public static void postMessageToQueue2(String queueName, String content, int invisibleTimeInSeconds) throws Exception {

        try {

            final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=6txkstrldaaoa01;AccountKey=****;EndpointSuffix=core.windows.net";
            final CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            final CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
            CloudQueue queue = queueClient.getQueueReference("readyforaggregation");



            CloudQueueMessage cloudQueueMessage = new CloudQueueMessage("{\"Id\":\"bfe6ae6a-3582-42fd-b871-f2749cda62d7\",\"BlobPath\":\"https://6txkstrldaaoa01.blob.core.windows.net/8up-20171106-temp-e5c334ee145d4b43a3a2d3a96fbac1df/41ef7c03-851d-461e-91eb-34fc91e6a789?sig=gLnw2ZXjh8D3hitwfQuVRcOz7QCMm3S3msLLKRmyTvY%3D&st=2017-11-07T05%3A08%3A05Z&se=2017-11-09T05%3A08%3A05Z&sv=2017-04-17&si=DownloadPolicy&sp=rwd&sr=b\",\"RawDataSize\":1645,\"DatabaseName\":\"AdobeAnalytics\",\"TableName\":\"RonylTest\",\"RetainBlobOnSuccess\":false,\"Format\":\"tsv\",\"FlushImmediately\":false,\"ReportLevel\":0}");
            queue.addMessage(cloudQueueMessage);
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
        }
    }
    
    public static void azureTableInsertEntity(String tableUri, TableServiceEntity entity) throws StorageException, URISyntaxException {
        CloudTable table = new CloudTable(new URI(tableUri));
        // Create an operation to add the new customer to the table basics table.
        TableOperation insert = TableOperation.insert(entity);
        // Submit the operation to the table service.
        table.execute(insert);
    }

    public static CloudBlockBlob uploadLocalFileToBlob(String filePath, String blobName, String storageUri) throws Exception{
        try {
            log.debug(String.format("uploadLocalFileToBlob: filePath: %s, blobName: %s, storageUri: %s", filePath, blobName, storageUri));

            // Check if the file is already compressed:
            boolean isCompressed = filePath.endsWith(".gz") || filePath.endsWith(".zip");

            CloudBlobContainer container = new CloudBlobContainer(new URI(storageUri));
            File sourceFile = new File(filePath);

            //Getting a blob reference
            CloudBlockBlob blob;

            if(!isCompressed)
            {
                blob = container.getBlockBlobReference(blobName+".gz");
                InputStream fin = Files.newInputStream(Paths.get(filePath));
                BlobOutputStream bos = blob.openOutputStream();
                GZIPOutputStream gzout = new GZIPOutputStream(bos);

                byte[] buffer = new byte[GZIP_BUFFER_SIZE];
                int length;
                while ((length = fin.read(buffer)) > 0) {
                    gzout.write(buffer, 0, length);
                }

                gzout.close();
                fin.close();

            } else {
                blob = container.getBlockBlobReference(blobName);
                blob.uploadFromFile(sourceFile.getAbsolutePath());
            }

            return blob;
        }
        catch (StorageException se)
        {
            log.error(String.format("uploadLocalFileToBlob: Error returned from the service. Http code: %d and error code: %s", se.getHttpStatusCode(), se.getErrorCode()), se);
            throw se;
        }
        catch (Exception ex)
        {
            log.error(String.format("uploadLocalFileToBlob: Error while uploading file to blob."), ex);
            throw ex;
        }
    }

    public static String getBlobPathWithSas(CloudBlockBlob blob) {
        StorageCredentialsSharedAccessSignature signature = (StorageCredentialsSharedAccessSignature)blob.getServiceClient().getCredentials();
        return blob.getStorageUri().getPrimaryUri().toString() + "?" + signature.getToken();
    }
}
