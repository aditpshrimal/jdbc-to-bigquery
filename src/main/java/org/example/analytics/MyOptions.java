package org.example.analytics;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface MyOptions extends PipelineOptions {

    @Description("SQL Query")
    @Default.String("SELECT customerNumber,customerName,contactLastName,contactFirstName,phone,addressLine1,addressLine2,city,state,country FROM customers limit 10")
    ValueProvider<String> getSqlQuery();
    void setSqlQuery(ValueProvider<String> sqlQuery);

    @Description("Driver Class")
    @Default.String("com.mysql.cj.jdbc.Driver")
    ValueProvider<String> getDriverClassName();
    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description("JDBC URL")
    //@Default.String("jdbc:mysql://localhost:3306/")
    @Default.String("jdbc:mysql://google/classicmodels?cloudSqlInstance=future-sunrise-333208:asia-south1:tink-poc-sql&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=password123")
    ValueProvider<String> getJdbcUrl();
    void setJdbcUrl(ValueProvider<String> jdbcUrl);

    @Description("JDBC Username")
    @Default.String("root")
    ValueProvider<String> getUsername();
    void setUsername(ValueProvider<String> username);

    @Description("JDBC Password")
    @Default.String("password123")
    ValueProvider<String> getPassword();
    void setPassword(ValueProvider<String> password);

    @Description("BigQuery temp location")
    @Default.String("gs://tink-poc/")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();
    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> bigQueryLoadingTemporaryDirectory);

    @Description("BigQuery table")
    //@Default.String("project-name:database.table")
    @Default.String("future-sunrise-333208:kms_poc.customersNonPii,future-sunrise-333208:kms_poc.customersPii")
    ValueProvider<String> getOutputTable();
    void setOutputTable(ValueProvider<String> outputTable);

    @Description("PII Flag")
    @Default.String("yes")
    ValueProvider<String> getPiiFlag();
    void setPiiFlag(ValueProvider<String> piiFlag);

    @Description("PII column names")
    @Default.String("phone,addressLine1,addressLine2")
    ValueProvider<String> getPiiColumnNames();
    void setPiiColumnNames(ValueProvider<String> piiColumnNames);

}
