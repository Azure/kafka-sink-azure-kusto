package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;

public class TableReportKustoIngestionResult implements IKustoIngestionResult {

    private List<IngestionStatusInTableDescription> m_descriptors;

    public TableReportKustoIngestionResult(List<IngestionStatusInTableDescription> descriptors) {
        m_descriptors = descriptors;
    }

    @Override
    public List<IngestionStatus> GetIngestionStatusCollection() throws StorageException, URISyntaxException {
        List<IngestionStatus> results = new LinkedList<>();
        for (IngestionStatusInTableDescription descriptor : m_descriptors) {
            CloudTable table = new CloudTable(new URI(descriptor.TableConnectionString));
            TableOperation operation = TableOperation.retrieve(descriptor.PartitionKey, descriptor.RowKey,
                    IngestionStatus.class);
            results.add(table.execute(operation).getResultAsType());
        }

        return results;
    }

}
