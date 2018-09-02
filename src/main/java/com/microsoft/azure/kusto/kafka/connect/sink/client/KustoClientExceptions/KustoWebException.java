package com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions;

import org.apache.http.HttpResponse;

public class KustoWebException extends Exception{

    private String m_message;
    private HttpResponse m_httpResponse;

    public String getMessage() { return m_message; }

    public HttpResponse getHttpResponse() { return m_httpResponse; }

    public KustoWebException(String message, HttpResponse httpResponse) {
        m_message = message;
        m_httpResponse = httpResponse;
    }
}
