package com.microsoft.azure.kusto.kafka.connect.sink.client;

import com.microsoft.azure.kusto.kafka.connect.sink.client.KustoClientExceptions.KustoWebException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

public class Utils {
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static KustoResults post(String url, String aadAccessToken, String payload) throws Exception {
        HttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);

        // Request parameters and other properties.
        StringEntity requestEntity = new StringEntity(
                payload,
                ContentType.APPLICATION_JSON);


        httpPost.setEntity(requestEntity);

        httpPost.addHeader("Authorization", String.format("Bearer %s", aadAccessToken));
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("Accept-Encoding", "gzip,deflate");
        httpPost.addHeader("Fed", "True");
        httpPost.addHeader("x-ms-client-version", "Kusto.Java.Client");

        //Execute and get the response.
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();

        if (entity != null) {

            StatusLine statusLine = response.getStatusLine();
            String responseContent = EntityUtils.toString(entity);

            if (statusLine.getStatusCode() == 200) {

                JSONObject jsonObject = new JSONObject(responseContent);
                JSONArray tablesArray = jsonObject.getJSONArray("Tables");
                JSONObject table0 = tablesArray.getJSONObject(0);
                JSONArray resultsColumns = table0.getJSONArray("Columns");

                HashMap<String, Integer> columnNameToIndex = new HashMap<String, Integer>();
                HashMap<String, String> columnNameToType = new HashMap<String, String>();
                for (int i = 0; i < resultsColumns.length(); i++) {
                    JSONObject column = resultsColumns.getJSONObject(i);
                    String columnName = column.getString("ColumnName");
                    columnNameToIndex.put(columnName, i);
                    columnNameToType.put(columnName, column.getString("DataType"));
                }

                JSONArray resultsRows = table0.getJSONArray("Rows");
                ArrayList<ArrayList<String>> values = new ArrayList<ArrayList<String>>();
                for (int i = 0; i < resultsRows.length(); i++) {
                    JSONArray row = resultsRows.getJSONArray(i);
                    ArrayList<String> rowVector = new ArrayList<String>();
                    for (int j = 0; j < row.length(); j++) {
                        rowVector.add(row.getString(j));
                    }
                    values.add(rowVector);
                }

                return new KustoResults(columnNameToIndex, columnNameToType, values);
            } else {
                throw new KustoWebException(responseContent, response);
            }
        }
        return null;
    }
}
