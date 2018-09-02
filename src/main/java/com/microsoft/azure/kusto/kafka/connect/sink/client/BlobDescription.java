package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.util.UUID;

public class BlobDescription {
    private String m_blobPath;
    private Long m_blobSize;
    private UUID m_sourceId;

    public String getBlobPath()
    {
        return m_blobPath;
    }

    public void setBlobPath(String blobPath)
    {
        m_blobPath = blobPath;
    }

    public Long getBlobSize()
    {
        return m_blobSize;
    }

    public void setBlobSize(Long blobSize)
    {
        m_blobSize = blobSize;
    }
    
    public UUID getSourceId()
    {
        return m_sourceId;
    }

    public void setSourceId(UUID sourceId)
    {
        m_sourceId = sourceId;
    }

    public BlobDescription()
    {
    }

    public BlobDescription(String blobPath, Long blobSize)
    {
        m_blobPath = blobPath;
        m_blobSize = blobSize;
    }
}
