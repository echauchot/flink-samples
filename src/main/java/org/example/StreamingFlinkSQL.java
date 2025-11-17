package org.example;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

public class StreamingFlinkSQL {

  public static void main(String[] args) {
    // init Table Env
    EnvironmentSettings environmentSettings =
        EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

    tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
            .schema(Schema.newBuilder()
                    .column("f0", DataTypes.STRING())
                    .column("event_time", DataTypes.TIMESTAMP(3))
                    .watermark("event_time", "event_time - INTERVAL '1' SECOND")
                    .build())

            .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
            .build());

    // Create a sink table
    tableEnv.createTemporaryTable("SinkTable",
            TableDescriptor.forConnector("blackhole")
              .schema(Schema.newBuilder()
                      .column("window_start", DataTypes.TIMESTAMP(3))
                      .column("window_end", DataTypes.TIMESTAMP(3))
                      .column("total", DataTypes.BIGINT())
                      .build())
            .build());

    // Select the count of records per session window of 5s gap duration
    Table table1 =
        tableEnv.sqlQuery(
            "SELECT window_start, window_end, COUNT(f0) AS total"
                + "  FROM TABLE(SESSION(TABLE SourceTable, DESCRIPTOR(event_time), INTERVAL '5' SECOND))"
                + "  GROUP BY window_start, window_end;");


// Emit a Table API result Table to a TableSink, same for SQL result
    table1.insertInto("SinkTable").execute();
  }
}
