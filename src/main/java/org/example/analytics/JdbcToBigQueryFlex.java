package org.example.analytics;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


public class JdbcToBigQueryFlex {
    public static ValueProvider<String> driverClassName;
    public static ValueProvider<String> jdbcUrl;
    public static ValueProvider<String> username;
    public static ValueProvider<String> password;
    public static ValueProvider<String> sqlQuery;
    public static ValueProvider<String> bigqueryDataset;
    public static ValueProvider<String> loadType;
    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        BigQueryIO.Write.WriteDisposition disposition = BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

        try {

                driverClassName = options.getDriverClassName();
                jdbcUrl = options.getJdbcUrl();
                username = options.getUsername();
                password = options.getPassword();
                sqlQuery = options.getSqlQuery();
                bigqueryDataset = options.getOutputTable();
                loadType = options.getLoadType();

                PCollection<TableRow> inputData = pipeline.apply(JdbcIO.<TableRow>read().withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(driverClassName,jdbcUrl)
                                .withUsername(username)
                                .withPassword(password))
                        .withQuery(sqlQuery)
                        .withCoder(TableRowJsonCoder.of())
                        .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
                            @Override
                            public TableRow mapRow(ResultSet resultSet) throws Exception {
                                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                                TableRow outputTableRow = new TableRow();
                                for(int i=1;i<=resultSetMetaData.getColumnCount();i++){
                                    outputTableRow.set(resultSetMetaData.getColumnName(i),resultSet.getObject(i));
                                }
                                return outputTableRow;
                            }
                        }));

                if(loadType.equals("FULL")){
                    disposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
                }

                inputData.apply(BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(disposition)
                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                        .to(bigqueryDataset));


            } catch (BigQueryException e) {
                System.out.println("Query not performed \n" + e.toString());
        }


        pipeline.run();
    }
}
