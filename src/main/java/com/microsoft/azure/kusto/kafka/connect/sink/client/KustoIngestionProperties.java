package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.util.HashMap;
import java.util.Map;

public class KustoIngestionProperties implements Comparable<KustoIngestionProperties> {
    private String m_databaseName;
    private String m_tableName;
    private Long m_fileSize;
    private boolean m_flushImmediately;
    private IngestionReportLevel m_reportLevel;
    private IngestionReportMethod m_reportMethod;
    private Map<String, String> m_additionalProperties;

    public KustoIngestionProperties(String databaseName, String tableName, Long fileSize) {
        m_databaseName = databaseName;
        m_tableName = tableName;
        m_reportLevel = IngestionReportLevel.FailuresOnly;
        m_reportMethod = IngestionReportMethod.Queue;
        m_flushImmediately = false;
        m_fileSize = fileSize;
        m_additionalProperties = new HashMap();
    }

    public String getDatabaseName() {
        return m_databaseName;
    }

    public Long getFileSize() {
        return m_fileSize;
    }

    public String getTableName() {
        return m_tableName;
    }

    public boolean getFlushImmediately() {
        return m_flushImmediately;
    }

    public void setFlushImmediately(boolean flushImmediately) {
        m_flushImmediately = flushImmediately;
    }

    public IngestionReportLevel getReportLevel() {
        return m_reportLevel;
    }

    public void setReportLevel(IngestionReportLevel reportLevel) {
        m_reportLevel = reportLevel;
    }

    public IngestionReportMethod getReportMethod() {
        return m_reportMethod;
    }

    public void setReportMethod(IngestionReportMethod reportMethod) {
        m_reportMethod = reportMethod;
    }

    public Map<String, String> getAdditionalProperties() {
        return m_additionalProperties;
    }

    public void setDataFormat(DATA_FORMAT dataFormat) {
        m_additionalProperties.put("format", dataFormat.name());
    }

    /**
     * Sets the data format by its name. If the name does not exist, then it does not add it.
     *
     * @param dataFormatName
     */
    public void setDataFormat(String dataFormatName) {
        String dataFormat = DATA_FORMAT.valueOf(dataFormatName.toLowerCase()).name();
        if (!dataFormat.isEmpty()) {
            m_additionalProperties.put("format", dataFormat);
        }
    }

    public void setJsonMappingName(String jsonMappingName) {
        m_additionalProperties.put("jsonMappingReference", jsonMappingName);
        m_additionalProperties.put("format", DATA_FORMAT.json.name());
    }

    public void setCsvMappingName(String mappingName) {
        m_additionalProperties.put("csvMappingReference", mappingName);
        m_additionalProperties.put("format", DATA_FORMAT.csv.name());
    }

    public void setAuthorizationContextToken(String token) {
        m_additionalProperties.put("authorizationContext", token);
    }

    @Override
    public int compareTo(KustoIngestionProperties o) {
        return this.m_tableName == o.m_tableName && this.m_databaseName == o.m_databaseName ? 1 : -1;
    }

    public enum DATA_FORMAT {csv, tsv, scsv, sohsv, psv, txt, json, singlejson, avro, parquet}

    public enum IngestionReportLevel {
        FailuresOnly,
        None,
        FailuresAndSuccesses
    }

    public enum IngestionReportMethod {
        Queue,
        Table,
        QueueAndTable
    }
}