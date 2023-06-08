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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * This is an example showing the to use the Pojo Cassandra Source in the Streaming API.
 *
 * <p>Pojo's have to be annotated with datastax annotations to work with this sink.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the
 * following queries:
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication={'class':'SimpleStrategy','replication_factor':'1'};
 * CREATE TABLE IF NOT EXISTS test.pojo(id bigint PRIMARY KEY); and that this table contains data.
 */
public class CassandraPojoSource {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ClusterBuilder clusterBuilder = new ClusterBuilder() {
      @Override    protected Cluster buildCluster(Cluster.Builder builder) {
        return builder.addContactPointsWithPorts(new InetSocketAddress("127.0.0.1",9042))
          .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
          .build();
      }  };
    long maxSplitMemorySize = MemorySize.ofMebiBytes(15).getBytes();

    final CassandraSource<Pojo> cassandraSource = new CassandraSource<>(clusterBuilder,
      maxSplitMemorySize, Pojo.class, "select * from test.pojo ;",
      () -> new Mapper.Option[] { Mapper.Option.saveNullFields(true) });
    DataStream<Pojo> stream =
        env.fromSource(cassandraSource, WatermarkStrategy.noWatermarks(), "CassandraSource");
    stream.print();
    env.execute("Cassandra Pojo Source example");
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