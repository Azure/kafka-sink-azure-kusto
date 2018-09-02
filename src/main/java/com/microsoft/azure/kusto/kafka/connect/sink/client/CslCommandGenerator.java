package com.microsoft.azure.kusto.kafka.connect.sink.client;

public class CslCommandGenerator {
    public static String generateDmEventHubSourceSettingsShowCommand()
    {
        String command = ".show EventHub ingestion sources settings";
        return command;
    }

    public static String generateIngestionResourcesShowCommand() {
        String command = ".show ingestion resources";
        return command;
    }
    
    public static String generateKustoIdentityGetCommand() {
        String command = ".get kusto identity token";
        return command;
    }
}