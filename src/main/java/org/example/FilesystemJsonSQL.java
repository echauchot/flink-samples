package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class FilesystemJsonSQL {

  public static void main(String[] args) {
    // CLI
    String inputDir = args[0];
    String outputDir = args[1];

    // init Table Env
    EnvironmentSettings environmentSettings =
        EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

    // create source
    tableEnv.executeSql(
        String.format(
            "CREATE TEMPORARY TABLE SourceTable (my_field1 BIGINT, my_field2 INT,  my_field3 VARCHAR(2147483647)) WITH ('connector' = 'filesystem', 'path' = '%s', 'format' = 'json')",
            inputDir));
    // create sink
    tableEnv.executeSql(
        String.format(
            "CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'filesystem', 'path' = '%s', 'format' = 'csv') LIKE SourceTable",
            outputDir));

    Table sourceTable = tableEnv.from("SourceTable");
    sourceTable.insertInto("SinkTable").execute();

  }
}
