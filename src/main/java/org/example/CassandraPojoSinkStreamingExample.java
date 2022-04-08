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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Objects;
import java.util.Random;

/**
 * This is an example showing the to use the Pojo Cassandra Sink in the Streaming API.
 *
 * <p>Pojo's have to be annotated with datastax annotations to work with this sink.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the
 * following queries: CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':
 * 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE IF NOT EXISTS test.pojo(id int PRIMARY KEY)
 */
public class CassandraPojoSinkStreamingExample {

  private static final long PERIOD = 100L;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Pojo> source =
        env.addSource(new PojoSource(PERIOD), "Infinite Pojo source", TypeInformation.of(Pojo.class));

    CassandraSink.addSink(source)
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

  /**
   * Source that generated long values indefinitely each number of specified milliseconds.
   */
  public static class PojoSource implements SourceFunction<Pojo> {

    private static Random intGenerator = new Random();
    private volatile boolean shouldBeInterrupted = false;
    private long period;

    private PojoSource(long period) {
      this.period = period;
    }

    @Override public void run(SourceContext<Pojo> sourceContext) throws Exception {
      while (!shouldBeInterrupted) {
        sourceContext.collect(new Pojo(intGenerator.nextInt()));
        Thread.sleep(period);
      }
    }

    @Override public void cancel() {
      shouldBeInterrupted = true;
    }
  }

  /** Pojo class for test. */
  @Table(keyspace = "test", name = "pojo")
  public static class Pojo {

    @PartitionKey private int id;

    public Pojo() {}

    public Pojo(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
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