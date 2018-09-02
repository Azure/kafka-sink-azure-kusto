package com.microsoft.azure.kusto.kafka.connect.sink.client;

import com.microsoft.aad.adal4j.*;

import javax.naming.ServiceUnavailableException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AadAuthenticationHelper {

    private final static String c_microsoftAadTenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
    private final static String c_kustoClientId = "ad30ae9e-ac1b-4249-8817-d24f5d7ad3de";

    private ClientCredential m_clientCredential;
    private String m_userUsername;
    private String m_userPassword;
    private String m_clusterUrl;
    private String m_aadAuthorityId;
    private String m_aadAuthorityUri;

    public static String getMicrosoftAadAuthorityId() { return c_microsoftAadTenantId; }

    public AadAuthenticationHelper(KustoConnectionStringBuilder kcsb) throws Exception{
        m_clusterUrl = kcsb.getClusterUrl();

        if (!Utils.isNullOrEmpty(kcsb.getApplicationClientId()) && !Utils.isNullOrEmpty(kcsb.getApplicationKey())) {
            m_clientCredential = new ClientCredential(kcsb.getApplicationClientId(), kcsb.getApplicationKey());
        } else {
            m_userUsername = kcsb.getUserUsername();
            m_userPassword = kcsb.getUserPassword();
        }

        // Set the AAD Authority URI
        m_aadAuthorityId = (kcsb.getAuthorityId() == null ? c_microsoftAadTenantId : kcsb.getAuthorityId());
        m_aadAuthorityUri = "https://login.microsoftonline.com/" + m_aadAuthorityId + "/oauth2/authorize";
    }

    public String acquireAccessToken() throws Exception {
        if (m_clientCredential != null){
            return acquireAadApplicationAccessToken().getAccessToken();
        } else {
            return acquireAadUserAccessToken().getAccessToken();
        }
    }

    private AuthenticationResult acquireAadUserAccessToken() throws Exception {
        AuthenticationContext context = null;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(m_aadAuthorityUri, true, service);

            Future<AuthenticationResult> future = context.acquireToken(
                    m_clusterUrl, c_kustoClientId, m_userUsername, m_userPassword,
                    null);
            result = future.get();
        } finally {
            service.shutdown();
        }

        if (result == null) {
            throw new ServiceUnavailableException("acquireAadUserAccessToken got 'null' authentication result");
        }
        return result;
    }

    private AuthenticationResult acquireAadApplicationAccessToken() throws Exception {
        AuthenticationContext context = null;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(m_aadAuthorityUri, true, service);
            Future<AuthenticationResult> future = context.acquireToken(m_clusterUrl, m_clientCredential, null);
            result = future.get();
        } finally {
            service.shutdown();
        }

        if (result == null) {
            throw new ServiceUnavailableException("acquireAadApplicationAccessToken got 'null' authentication result");
        }
        return result;
    }

}
