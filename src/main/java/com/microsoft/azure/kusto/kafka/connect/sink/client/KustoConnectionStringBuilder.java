package com.microsoft.azure.kusto.kafka.connect.sink.client;

public class KustoConnectionStringBuilder {

    private String m_clusterUri;
    private String m_username;
    private String m_password;
    private String m_applicationClientId;
    private String m_applicationKey;
    private String m_aadAuthorityId; // AAD tenant Id (GUID)

    public String getClusterUrl() { return m_clusterUri; }
    public String getUserUsername() { return m_username; }
    public String getUserPassword() { return m_password; }
    public String getApplicationClientId() { return m_applicationClientId; }
    public String getApplicationKey() { return m_applicationKey; }
    public String getAuthorityId() { return m_aadAuthorityId; }

    private KustoConnectionStringBuilder(String resourceUri)
    {
        m_clusterUri = resourceUri;
        m_username = null;
        m_password = null;
        m_applicationClientId = null;
        m_applicationKey = null;
        m_aadAuthorityId = null;
    }

    public static KustoConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                            String username,
                                                                            String password,
                                                                            String authorityId)
    {
        KustoConnectionStringBuilder kcsb = new KustoConnectionStringBuilder(resourceUri);
        kcsb.m_username = username;
        kcsb.m_password = password;
        kcsb.m_aadAuthorityId = authorityId;
        return kcsb;
    }

    public static KustoConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                            String username,
                                                                            String password)
    {
        return createWithAadUserCredentials(resourceUri, username, password, null);
    }

    public static KustoConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                                   String applicationClientId,
                                                                                   String applicationKey,
                                                                                   String authorityId)
    {
        KustoConnectionStringBuilder kcsb = new KustoConnectionStringBuilder(resourceUri);
        kcsb.m_applicationClientId = applicationClientId;
        kcsb.m_applicationKey = applicationKey;
        kcsb.m_aadAuthorityId = authorityId;
        return kcsb;
    }

    public static KustoConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                                   String applicationClientId,
                                                                                   String applicationKey)
    {
        return createWithAadApplicationCredentials(resourceUri, applicationClientId, applicationKey, null);
    }
}
