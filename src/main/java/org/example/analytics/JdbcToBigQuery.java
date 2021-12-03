package org.example.analytics;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Sets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class JdbcToBigQuery {
    static class nonPiiParDo extends DoFn<TableRow, TableRow> {
        ValueProvider<String> piiColumnNames;
        public nonPiiParDo(ValueProvider<String> piiColumnNames){
            this.piiColumnNames = piiColumnNames;
        }
        @ProcessElement
        public void processElement(ProcessContext c)  {
            String[] values = piiColumnNames.get().split(",");
            Set<String> piiSet = new HashSet<String>(Arrays.asList(values));
            TableRow row = c.element();
            TableRow newRow = new TableRow();
            Set<String> keys = row.keySet();
            keys = Sets.difference(keys,piiSet);
            for(String key:keys) {
                newRow.set(key, row.get(key));
                c.output(newRow);
            }
        }
    }
    static class piiPardo extends DoFn<TableRow, TableRow> {
        ValueProvider<String> piiColumnNames;
        public piiPardo(ValueProvider<String> piiColumnNames){
            this.piiColumnNames = piiColumnNames;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws GeneralSecurityException, IOException {
            String[] values = piiColumnNames.get().split(",");
            Set<String> piiSet = new HashSet<String>(Arrays.asList(values));
            TableRow row = c.element();
            TableRow newRow = new TableRow();
            for(String key:piiSet) {
                Object object = row.get(key);
                if(object==null){
                    newRow.set(key,null);
                }
                else {
                    byte[] encryptedData= KmsEncryption.encrypt(object.toString());
                    newRow.set(key,encryptedData);
                }

                c.output(newRow);
            }
        }
    }
    public static void main(String[] args) throws GeneralSecurityException, IOException {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);

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
                                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                                        TableRow outputTableRow = new TableRow();
                                         for(int i=1;i<=resultSetMetaData.getColumnCount();i++){
                                             outputTableRow.set(resultSetMetaData.getColumnName(i),resultSet.getObject(i));
                                         }
                                         return outputTableRow;
                                    }
                                }));
            if(options.getPiiFlag().get().equals("yes")) {
                String[] tableNames = options.getOutputTable().get().split(",");
                inputData.
                        apply(ParDo.of(new nonPiiParDo(options.getPiiColumnNames())))
                        .apply(
                                "Write to BigQuery",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                        .to(tableNames[0]));
                inputData.
                        apply(ParDo.of(new piiPardo(options.getPiiColumnNames())))
                        .apply(
                                "Write to BigQuery",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                        .to(tableNames[1]));
            }
            else {
                inputData.apply(
                        "Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withoutValidation()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                .to(options.getOutputTable()));
            }
        pipeline.run();
    }
}
