package org.example.analytics;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import java.sql.*;


public class JdbcToBigQueryMultiplePipelinesV2 {
    public static String driverClassName;
    public static String jdbcUrl;
    public static String username;
    public static String password;
    public static String sqlQuery;
    public static String bigqueryDataset;
    public static void main(String[] args) throws SQLException {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        String u = "root";
        String p = "password123";
        String url = "jdbc:mysql://google/classicmodels?cloudSqlInstance=future-sunrise-333208:asia-south1:tink-poc-sql&socketFactory=com.google.cloud.sql.mysql.SocketFactory";
        Connection connection = DriverManager.getConnection(url, u, p);

        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM stage_params");
            while (resultSet.next()) {
                driverClassName = resultSet.getString(1);
                jdbcUrl = resultSet.getString(2);
                username = resultSet.getString(3);
                password = resultSet.getString(4);
                sqlQuery = resultSet.getString(5);
                bigqueryDataset = resultSet.getString(6);

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
                inputData.apply(BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                        .to(bigqueryDataset));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        pipeline.run();
    }
}
