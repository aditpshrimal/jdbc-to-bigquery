package org.example.analytics;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;

import java.sql.*;


public class JdbcToBigQueryMultiplePipelinesV3 {
    public static String driverClassName;
    public static String jdbcUrl;
    public static String username;
    public static String password;
    public static String sqlQuery;
    public static String bigqueryDataset;
    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        String query ="SELECT * FROM `future-sunrise-333208.tink_poc.stage_params` WHERE pipeline_status!='SUCCESSFUL'";

        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = bigquery.query(queryConfig);

            for (FieldValueList row:results.iterateAll()){
                driverClassName = row.get(0).getStringValue();
                jdbcUrl = row.get(1).getStringValue();
                username = row.get(2).getStringValue();
                password = row.get(3).getStringValue();
                sqlQuery = row.get(4).getStringValue();
                bigqueryDataset = row.get(5).getStringValue();

                try {
                    PCollection<TableRow> inputData = pipeline.apply(JdbcIO.<TableRow>read().withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create(driverClassName, jdbcUrl)
                                    .withUsername(username)
                                    .withPassword(password))
                            .withQuery(sqlQuery)
                            .withCoder(TableRowJsonCoder.of())
                            .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
                                @Override
                                public TableRow mapRow(ResultSet resultSet) throws Exception {
                                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                                    TableRow outputTableRow = new TableRow();
                                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                        outputTableRow.set(resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                                    }
                                    return outputTableRow;
                                }
                            }));
                    inputData.apply(BigQueryIO.writeTableRows()
                            .withoutValidation()
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                            .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                            .to(bigqueryDataset));
                }
                catch (Exception e){
                    String failedQuery = "UPDATE tink_poc.stage_params SET pipeline_status='FAILED' WHERE bigquery_dataset='"+bigqueryDataset+"'";
                    QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(failedQuery).build();
                    TableResult tableResult = bigquery.query(queryJobConfiguration);
                    System.out.println(tableResult.getValues());
                }
            }

                System.out.println("Query performed successfully.");
            } catch (BigQueryException | InterruptedException e) {
                System.out.println("Query not performed \n" + e.toString());
        }



    }
}
