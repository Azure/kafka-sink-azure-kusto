package com.microsoft.azure.kusto.kafka.connect.sink.client;

import java.util.ArrayList;
import java.util.HashMap;

public class KustoResults {
    private HashMap<String, Integer> m_columnNameToIndex;
    private HashMap<String, String> m_columnNameToType;
    private ArrayList<ArrayList<String>> m_values;

    public KustoResults(HashMap<String, Integer> columnNameToIndex, HashMap<String, String> columnNameToType,
                        ArrayList<ArrayList<String>> values) {
        m_columnNameToIndex = columnNameToIndex;
        m_columnNameToType = columnNameToType;
        m_values = values;
    }

    public HashMap<String, Integer> getColumnNameToIndex() {
        return m_columnNameToIndex;
    }

    public HashMap<String, String> getColumnNameToType() {
        return m_columnNameToType;
    }

    public Integer getIndexByColumnName(String columnName) {
        return m_columnNameToIndex.get(columnName);
    }

    public String getTypeByColumnName(String columnName) {
        return m_columnNameToType.get(columnName);
    }

    ;

    public ArrayList<ArrayList<String>> getValues() {
        return m_values;
    }
}
