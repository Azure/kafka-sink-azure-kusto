package com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions;

import java.util.List;

public class KustoClientAggregateException extends Exception{

    List<KustoClientException> m_kustoClientExceptions;

    public List<KustoClientException> getExceptions() { return m_kustoClientExceptions; }

    public KustoClientAggregateException(List<KustoClientException> kustoClientExceptions)
    {
        m_kustoClientExceptions = kustoClientExceptions;
    }
}
