package com.microsoft.azure.kusto.kafka.connect.sink.client;

import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.blob.*;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class Sample {

    public static void main(String[] args) {
        try {

            // This will work with MS AAD tenant. If another tenant is required, set this variable to the right one
            String authorityId = AadAuthenticationHelper.getMicrosoftAadAuthorityId();
            //AzureStorageHelper.postMessageToQueue2("aaa", "aaa", 1);

            //String engineClusterUrl = "https://<ClusterName>.kusto.windows.net";
            String dmClusterUrl = "https://ingest-kustoppas2.kusto.windows.net";

            String databaseName = "velostrata";
            String tableName = "tmpLogs";

            //String userUsername = "<AadUserUsername>";
            //String userPassword = "<AadUserPassword>";

            String applicationClientId = "";
            String applicationKey = "";

            //String dmClusterUrl = "https://ingest-roil.kusto.windows.net";
            //String userUsername = "roil@microsoft.com";
            //String userPassword = "";

            // Kusto connection string that uses the given user and password during authentication
            //KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadUserCredentials(dmClusterUrl, userUsername, userPassword, authorityId);

            // Kusto connection string that uses the given application client ID and key during authentication
            KustoConnectionStringBuilder kcsb =
                    KustoConnectionStringBuilder.createWithAadApplicationCredentials(dmClusterUrl, applicationClientId, applicationKey, authorityId);

            // Create an instance of KustoClient that will be used for executing commands against the cluster
            KustoIngestClient kustoClient = new KustoIngestClient(kcsb);


            // Running a basic count query on the given database and table
            //KustoResults results = kustoClient.execute(databaseName, String.format("%s | count", tableName));

            // Get a lost of EventHub connection string from the DM cluster using KustoClient
            //KustoResults eventHubSourceSettings = kustoClient.execute(CslCommandGenerator.generateDmEventHubSourceSettingsShowCommand());

            // Ingest from a single blob
            //String blobPath = "";

            String blob1 = "";
            String blob2 = "";
            String blob3 = "";

            KustoIngestionProperties ingestionProperties = new KustoIngestionProperties(databaseName, tableName, (long) 0);
            //kustoClient.ingestFromSingleBlob(blobPath, false, ingestionProperties);

            RunVelostrataIngest(kustoClient, ingestionProperties);

            //kustoClient.ingestFromMultipleBlobs(new ArrayList<String>(Arrays.asList(blob1,blob2,blob3)), false, ingestionProperties);
            //System.out.print("DONE !!!");

            /*
            Future task = kustoClient.ingestFromMultipleBlobsAsync(new ArrayList<String>(Arrays.asList(blob1,blob2,blob3)),false, ingestionProperties);

            System.out.print("task done = " + task.isDone());
            Object res = task.get();
            System.out.print("task done = " + task.isDone());
            System.out.print("DONE !!!");
            */

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void RunVelostrataIngest(KustoIngestClient kustoIngestClient, KustoIngestionProperties ingestionProperties) {
        String containerUri = "";

        try {
            URI uri = new URI(containerUri);
            CloudBlobContainer container = new CloudBlobContainer(uri);

            ArrayList<String> blobs = new ArrayList<>();


            CloudBlobDirectory directory = container.getDirectoryReference("ES");
            Iterable<ListBlobItem> blobItems = directory.listBlobs();


            StorageCredentialsSharedAccessSignature signature = (StorageCredentialsSharedAccessSignature)container.getServiceClient().getCredentials();
            for (ListBlobItem blobItem: blobItems) {
                blobs.add(blobItem.getUri().toString() + "?" + signature.getToken());
            }

            ingestionProperties.setJsonMappingName("LogsMapping1");
            ingestionProperties.setFlushImmediately(true);

            kustoIngestClient.ingestFromMultipleBlobsPaths(blobs, false, ingestionProperties);


            System.out.print("DONE !!!");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    private final static SharedAccessBlobPolicy createSharedAccessPolicy(EnumSet<SharedAccessBlobPermissions> sap, int expireTimeInSeconds) {

        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.setTime(new Date());
        cal.add(Calendar.SECOND, expireTimeInSeconds);
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissions(sap);
        policy.setSharedAccessExpiryTime(cal.getTime());
        return policy;

    }
}
