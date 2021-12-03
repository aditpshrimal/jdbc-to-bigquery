package org.example.analytics;

import com.google.api.services.bigquery.model.TableRow;
import com.mysql.cj.protocol.Resultset;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


public class CustomRowMapper implements JdbcIO.RowMapper<TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomRowMapper.class);
    ValueProvider<String> piiFlag;
    ValueProvider<String> piiColumnsIndex;

    public CustomRowMapper(ValueProvider<String> piiFlag,ValueProvider<String> piiColumnsIndex){
        this.piiFlag = piiFlag;
        this.piiColumnsIndex = piiColumnsIndex;
    }

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception{
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        TableRow outputTableRow = new TableRow();

        return outputTableRow;
    }
}
