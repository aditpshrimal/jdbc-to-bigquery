package org.example.analytics;

import com.google.cloud.bigquery.*;

public class Helper {
    public static TableResult getTableResult(String query) throws InterruptedException {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        TableResult results = bigquery.query(queryConfig);
        return  results;
    }
}
