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
import java.util.Set;


public class EncryptionPoc {
    static class nonPiiParDo extends DoFn<TableRow, TableRow> {
        String piiColumnNames;
        public nonPiiParDo(String  piiColumnNames){
            this.piiColumnNames = piiColumnNames;
        }
        @ProcessElement
        public void processElement(ProcessContext c)  {
            String[] values = piiColumnNames.split(",");
            Set<String> piiSet = new HashSet<String>(Arrays.asList(values));
            TableRow row = c.element();
            TableRow newRow = new TableRow();
            Set<String> keys = row.keySet();
            System.out.println(keys);
            keys = Sets.difference(keys,piiSet);
            for(String key:keys) {
                newRow.set(key, row.get(key));

            }
            c.output(newRow);
        }
    }
    static class piiPardo extends DoFn<TableRow, TableRow> {
        String piiColumnNames;
        String joinKey;
        public piiPardo(String piiColumnNames,String joinKey){
            this.piiColumnNames = piiColumnNames;
            this.joinKey = joinKey;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws GeneralSecurityException, IOException {
            String[] values = piiColumnNames.split(",");
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
            }
            newRow.set(joinKey,row.get(joinKey));
            c.output(newRow);
        }
    }
    public static void main(String[] args) throws GeneralSecurityException, IOException {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        String[] outputTableNames = options.getOutputTable().get().split(",");
        String piiColumnNames = options.getPiiColumnNames().get();
        String joinKey = options.getJoinKey().get();
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
                                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                                        TableRow outputTableRow = new TableRow();
                                         for(int i=1;i<=resultSetMetaData.getColumnCount();i++){
                                             outputTableRow.set(resultSetMetaData.getColumnName(i),resultSet.getObject(i));
                                         }
                                         return outputTableRow;
                                    }
                                }));
                inputData.
                        apply(ParDo.of(new nonPiiParDo(piiColumnNames)))
                        .apply(
                                "Write to BigQuery",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                        .to(outputTableNames[0]));
                inputData.
                        apply(ParDo.of(new piiPardo(piiColumnNames,joinKey)))
                        .apply(
                                "Write to BigQuery",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                        .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                        .to(outputTableNames[1]));

        pipeline.run();
    }
}
