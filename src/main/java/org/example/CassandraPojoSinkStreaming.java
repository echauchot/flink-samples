/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

/**
 * This is an example showing the to use the Pojo Cassandra Sink in the Streaming API.
 *
 * <p>Pojo's have to be annotated with datastax annotations to work with this sink.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the
 * following queries:
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication={'class':'SimpleStrategy','replication_factor':'1'};
 * CREATE TABLE IF NOT EXISTS test.pojo(id bigint PRIMARY KEY);
 */
public class CassandraPojoSinkStreaming {
  // source rate is random between 200 and 1000 records per second
  private static final int MAX_PERIOD = 5;
  private static final int MIN_PERIOD = 1;
  // 10% of records will be late of a random time between 1s and 10s
  private static final int MIN_LATENESS = 1000;
  private static final int MAX_LATENESS = 10000;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureCheckpointing(env);

    DataStreamSource<Pojo> source =
        env.addSource(new PojoSource(MIN_PERIOD, MAX_PERIOD, MIN_LATENESS, MAX_LATENESS), "Infinite Pojo source", TypeInformation.of(Pojo.class));
    final DataStream<Pojo> stream = source.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(MAX_LATENESS + 1)));
    final DataStream<Pojo> out = stream.windowAll(
        TumblingEventTimeWindows.of(Time.seconds(10L)))
      .reduce((pojo1, pojo2) -> new Pojo(pojo1.getId() + pojo2.getId()));

    CassandraSink.addSink(out)
        .setClusterBuilder(
            new ClusterBuilder() {
              @Override
              protected Cluster buildCluster(Builder builder) {
                return builder.addContactPoint("127.0.0.1").build();
              }
            })
        .setMapperOptions(() -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)})
        .build();

    env.execute("Cassandra Pojo Sink Streaming example");
  }

  private static void configureCheckpointing(StreamExecutionEnvironment env) {
    // start a checkpoint every 2 min
    env.enableCheckpointing(120_000L);

    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    // checkpoints have to complete within 1 minute, or are discarded
    env.getCheckpointConfig().setCheckpointTimeout(60000);

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // enable externalized checkpoints which are retained
    // after job cancellation
    env.getCheckpointConfig().setExternalizedCheckpointCleanup(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    // sets the checkpoint storage where checkpoint snapshots will be written
    env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
  }

  /**
   * Source that generated long values indefinitely each number of specified milliseconds.
   */
  public static class PojoSource implements SourceFunction<Pojo> {

    private static Random random = new Random();
    private volatile boolean shouldBeInterrupted = false;
    private final int minPeriod;
    private final int maxPeriod;
    private final int minLateness;
    private final int maxLateness;

    private PojoSource(int minPeriod, int maxPeriod, int minLateness, int maxLateness) {
      this.minPeriod = minPeriod;
      this.maxPeriod = maxPeriod;
      this.minLateness = minLateness;
      this.maxLateness = maxLateness;
    }

    @Override
    public void run(SourceContext<Pojo> sourceContext) throws Exception {
      int i = 0;
      // emit a Pojo at a random rate between minPeriod and maxPeriod.
      // Records have timestamps mainly monotonically increasing except 10% of late records (between minLateness and maxLateness)
      while (!shouldBeInterrupted) {
        sourceContext.collectWithTimestamp(
            new Pojo(i),
            i % 10 == 0
                ? System.currentTimeMillis() - randomIntInRange(minLateness, maxLateness)
                : System.currentTimeMillis());
        Thread.sleep(randomIntInRange(minPeriod, maxPeriod));
        i++;
      }
    }
    @Override
    public void cancel() {
      shouldBeInterrupted = true;
    }

    private static int randomIntInRange(int min, int max){
      return random.nextInt(max - min) + min;
    }
  }

  /** Pojo class for test. */
  @Table(keyspace = "test", name = "pojo")
  public static class Pojo {

    @PartitionKey
    private long id;

    public Pojo() {}

    public Pojo(long id) {
      this.id = id;
    }

    public long getId() {
      return id;
    }

    public void setId(long id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Pojo pojo = (Pojo) o;
      return id == pojo.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }
}