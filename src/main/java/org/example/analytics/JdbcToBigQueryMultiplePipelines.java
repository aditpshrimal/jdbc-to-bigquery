package org.example.analytics;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Sets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import sun.tools.jconsole.Tab;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class JdbcToBigQueryMultiplePipelines {

    public static void main(String[] args) throws GeneralSecurityException, IOException {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        KmsEncryption.initializeOnce();

            PCollection<TableRow> inputData =  pipeline.apply("Reading Database",
                        JdbcIO.<TableRow>read()
                                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                        .create(options.getDriverClassName(), options.getJdbcUrl())
                                        .withUsername(options.getUsername()).withPassword(options.getPassword()))
                                .withQuery(options.getSqlQuery())
                                .withCoder(TableRowJsonCoder.of())
                                .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
                                    @Override
                                    public TableRow mapRow(ResultSet resultSet) throws Exception {
                                        String driverClassName = resultSet.getString(0);
                                        String jdbcUrl = resultSet.getString(1);
                                        String username = resultSet.getString(2);
                                        String password = resultSet.getString(3);
                                        String sqlQuery = resultSet.getString(4);
                                        String bigqueryDataset = resultSet.getString(5);
                                        TableRow outputTableRow = new TableRow();

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

                                         return outputTableRow;
                                    }
                                }));

        pipeline.run();
    }
}
