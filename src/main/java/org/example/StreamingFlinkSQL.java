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

// Create a sink table (using SQL DDL)

    tableEnv.createTemporaryTable("SinkTable",
            TableDescriptor.forConnector("blackhole")
              .schema(Schema.newBuilder()
                      .column("f0", DataTypes.STRING())
                      .build())
            .build());





// Create a Table object from a SQL query
    Table table1 = tableEnv.sqlQuery("SELECT f0 FROM TABLE(SESSION(TABLE SourceTable, DESCRIPTOR(event_time), INTERVAL '1' SECOND))");

// Emit a Table API result Table to a TableSink, same for SQL result
    table1.insertInto("SinkTable").execute();
  }
}
