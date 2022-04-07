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

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.concurrent.FutureUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This is an example showing the to use the Pojo Cassandra Sink in the Streaming API.
 *
 * <p>Pojo's have to be annotated with datastax annotations to work with this sink.
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the
 * following queries: CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':
 * 'SimpleStrategy', 'replication_factor': '1'}; CREATE TABLE IF NOT EXISTS test.pojo(id int PRIMARY
 * KEY)
 */
public class CassandraPojoSinkStreamingExample {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Pojo> source =
      env.fromSource(
        new PojoSource(72, 16, 0, 60 * 60 * 1000L),
        WatermarkStrategy.noWatermarks(),
        "PojoSource 72h");

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

  /** source that generates longs in a fixed number of splits. */
  private static class PojoSource
    implements Source<Pojo, PojoSource.PojoSplit, PojoSource.EnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(PojoSource.class);
    private static Random random = new Random();
    private final int minCheckpoints;
    private final int numSplits;
    private final int expectedRestarts;
    private final long checkpointingInterval;

    private PojoSource(
      int minCheckpoints,
      int numSplits,
      int expectedRestarts,
      long checkpointingInterval) {
      this.minCheckpoints = minCheckpoints;
      this.numSplits = numSplits;
      this.expectedRestarts = expectedRestarts;
      this.checkpointingInterval = checkpointingInterval;
    }

    @Override
    public Boundedness getBoundedness() {
      return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Pojo, PojoSplit> createReader(SourceReaderContext readerContext) {
      return new PojoSourceReader(
        readerContext.getIndexOfSubtask(),
        minCheckpoints,
        expectedRestarts,
        checkpointingInterval);
    }

    @Override
    public SplitEnumerator<PojoSplit, EnumeratorState> createEnumerator(
      SplitEnumeratorContext<PojoSplit> enumContext) {
      List<PojoSplit> splits =
        IntStream.range(0, numSplits)
          .mapToObj(i -> new PojoSplit(i, numSplits))
          .collect(Collectors.toList());
      return new PojoSplitSplitEnumerator(enumContext, new EnumeratorState(splits, 0, 0));
    }

    @Override
    public SplitEnumerator<PojoSplit, EnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<PojoSplit> enumContext, EnumeratorState state) {
      return new PojoSplitSplitEnumerator(enumContext, state);
    }

    @Override
    public SimpleVersionedSerializer<PojoSplit> getSplitSerializer() {
      return new SplitVersionedSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
      return new EnumeratorVersionedSerializer();
    }

    private static class PojoSourceReader implements SourceReader<Pojo, PojoSplit> {
      private final int subtaskIndex;
      private final int minCheckpoints;
      private final int expectedRestarts;
      private final LongCounter numInputsCounter = new LongCounter();
      private final List<PojoSplit> splits = new ArrayList<>();
      private final Duration pumpInterval;
      private int numAbortedCheckpoints;
      private int numRestarts;
      private int numCompletedCheckpoints;
      private boolean finishing;
      private boolean recovered;
      @Nullable private Deadline pumpingUntil = null;

      public PojoSourceReader(
        int subtaskIndex,
        int minCheckpoints,
        int expectedRestarts,
        long checkpointingInterval) {
        this.subtaskIndex = subtaskIndex;
        this.minCheckpoints = minCheckpoints;
        this.expectedRestarts = expectedRestarts;
        pumpInterval = Duration.ofMillis(checkpointingInterval);
      }

      @Override
      public void start() {}

      @Override
      public InputStatus pollNext(ReaderOutput<Pojo> output) throws InterruptedException {
        for (PojoSplit split : splits) {
          output.collect(new Pojo(random.nextInt()), split.nextNumber);
          split.nextNumber += split.increment;
        }

        if (finishing) {
          return InputStatus.END_OF_INPUT;
        }

        if (pumpingUntil != null && pumpingUntil.isOverdue()) {
          pumpingUntil = null;
        }
        if (pumpingUntil == null) {
          Thread.sleep(100);
        }
        return InputStatus.MORE_AVAILABLE;
      }

      @Override
      public List<PojoSplit> snapshotState(long checkpointId) {
        LOG.info(
          "Snapshotted {} @ {} subtask ({} attempt)",
          splits,
          subtaskIndex,
          numRestarts);
        // barrier passed, so no need to add more data for this test
        pumpingUntil = null;
        return splits;
      }

      @Override
      public void notifyCheckpointComplete(long checkpointId) {
        LOG.info(
          "notifyCheckpointComplete {} @ {} subtask ({} attempt)",
          numCompletedCheckpoints,
          subtaskIndex,
          numRestarts);
        // Update polling state before final checkpoint such that if there is an issue
        // during finishing, after recovery the source immediately starts finishing
        // again. In this way, we avoid a deadlock where some tasks need another
        // checkpoint completed, while some tasks are finishing (and thus there are no
        // new checkpoint).
        updatePollingState();
        numCompletedCheckpoints++;
        recovered = true;
        numAbortedCheckpoints = 0;
      }

      @Override
      public void notifyCheckpointAborted(long checkpointId) {
        if (numAbortedCheckpoints++ > 100) {
          // aborted too many checkpoints in a row, which usually indicates that part of
          // the pipeline is already completed
          // here simply also advance completed checkpoints to avoid running into a live
          // lock
          numCompletedCheckpoints = minCheckpoints + 1;
        }
        updatePollingState();
      }

      @Override
      public CompletableFuture<Void> isAvailable() {
        return FutureUtils.completedVoidFuture();
      }

      @Override
      public void addSplits(List<PojoSplit> splits) {
        this.splits.addAll(splits);
        updatePollingState();
        LOG.info(
          "Added splits {}, finishing={}, pumping until {} @ {} subtask ({} attempt)",
          splits,
          finishing,
          pumpingUntil,
          subtaskIndex,
          numRestarts);
      }

      @Override
      public void notifyNoMoreSplits() {
        updatePollingState();
      }

      private void updatePollingState() {
        if (numCompletedCheckpoints >= minCheckpoints && numRestarts >= expectedRestarts) {
          finishing = true;
          LOG.info("Finishing @ {} subtask ({} attempt)", subtaskIndex, numRestarts);
        } else if (recovered) {
          // a successful checkpoint as a proxy for a finished recovery
          // cause backpressure until next checkpoint is added
          pumpingUntil = Deadline.fromNow(pumpInterval);
          LOG.info(
            "Pumping until {} @ {} subtask ({} attempt)",
            pumpingUntil,
            subtaskIndex,
            numRestarts);
        }
      }

      @Override
      public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof SyncEvent) {
          numRestarts = ((SyncEvent) sourceEvent).numRestarts;
          numCompletedCheckpoints = ((SyncEvent) sourceEvent).numCheckpoints;
          LOG.info(
            "Set restarts={}, numCompletedCheckpoints={} @ {} subtask ({} attempt)",
            numRestarts,
            numCompletedCheckpoints,
            subtaskIndex,
            numRestarts);
          updatePollingState();
        }
      }

      @Override
      public void close() throws Exception {
        for (PojoSplit split : splits) {
          numInputsCounter.add(split.nextNumber / split.increment);
        }
      }
    }

    private static class SyncEvent implements SourceEvent {
      final int numRestarts;
      final int numCheckpoints;

      SyncEvent(int numRestarts, int numCheckpoints) {
        this.numRestarts = numRestarts;
        this.numCheckpoints = numCheckpoints;
      }
    }

    private static class PojoSplit implements SourceSplit {
      private final int increment;
      private long nextNumber;

      public PojoSplit(long nextNumber, int increment) {
        this.nextNumber = nextNumber;
        this.increment = increment;
      }

      public int getBaseNumber() {
        return (int) (nextNumber % increment);
      }

      @Override
      public String splitId() {
        return String.valueOf(increment);
      }

      @Override
      public String toString() {
        return "PojoSplit{" + "increment=" + increment + ", nextNumber=" + nextNumber + '}';
      }
    }

    private static class PojoSplitSplitEnumerator
      implements SplitEnumerator<PojoSplit, EnumeratorState> {
      private final SplitEnumeratorContext<PojoSplit> context;
      private final EnumeratorState state;
      private final Map<Integer, Integer> subtaskRestarts = new HashMap<>();

      private PojoSplitSplitEnumerator(
        SplitEnumeratorContext<PojoSplit> context, EnumeratorState state) {
        this.context = context;
        this.state = state;
      }

      @Override
      public void start() {}

      @Override
      public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

      @Override
      public void addSplitsBack(List<PojoSplit> splits, int subtaskId) {
        LOG.info("addSplitsBack {}", splits);
        // Called on recovery
        subtaskRestarts.compute(
          subtaskId,
          (id, oldCount) -> oldCount == null ? state.numRestarts + 1 : oldCount + 1);
        state.unassignedSplits.addAll(splits);
      }

      @Override
      public void addReader(int subtaskId) {
        if (context.registeredReaders().size() == context.currentParallelism()) {
          if (!state.unassignedSplits.isEmpty()) {
            Map<Integer, List<PojoSplit>> assignment =
              state.unassignedSplits.stream()
                .collect(Collectors.groupingBy(PojoSplit::getBaseNumber));
            LOG.info("Assigning splits {}", assignment);
            context.assignSplits(new SplitsAssignment<>(assignment));
            state.unassignedSplits.clear();
          }
          context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
          Optional<Integer> restarts =
            subtaskRestarts.values().stream().max(Comparator.naturalOrder());
          if (restarts.isPresent() && restarts.get() > state.numRestarts) {
            state.numRestarts = restarts.get();
            // Implicitly sync the restart count of all subtasks with state.numRestarts
            subtaskRestarts.clear();
            final SyncEvent event =
              new SyncEvent(state.numRestarts, state.numCompletedCheckpoints);
            context.registeredReaders()
              .keySet()
              .forEach(index -> context.sendEventToSourceReader(index, event));
          }
        }
      }

      @Override
      public void notifyCheckpointComplete(long checkpointId) {
        state.numCompletedCheckpoints++;
      }

      @Override
      public EnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.info("snapshotState {}", state);
        return state;
      }

      @Override
      public void close() throws IOException {}
    }

    private static class EnumeratorState {
      final List<PojoSplit> unassignedSplits;
      int numRestarts;
      int numCompletedCheckpoints;

      public EnumeratorState(
        List<PojoSplit> unassignedSplits,
        int numRestarts,
        int numCompletedCheckpoints) {
        this.unassignedSplits = unassignedSplits;
        this.numRestarts = numRestarts;
        this.numCompletedCheckpoints = numCompletedCheckpoints;
      }

      @Override
      public String toString() {
        return "EnumeratorState{"
          + "unassignedSplits="
          + unassignedSplits
          + ", numRestarts="
          + numRestarts
          + ", numCompletedCheckpoints="
          + numCompletedCheckpoints
          + '}';
      }
    }

    private static class EnumeratorVersionedSerializer
      implements SimpleVersionedSerializer<EnumeratorState> {
      private final SplitVersionedSerializer splitVersionedSerializer =
        new SplitVersionedSerializer();

      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public byte[] serialize(EnumeratorState state) {
        final ByteBuffer byteBuffer =
          ByteBuffer.allocate(
            state.unassignedSplits.size() * SplitVersionedSerializer.LENGTH
              + 8);
        byteBuffer.putInt(state.numRestarts);
        byteBuffer.putInt(state.numCompletedCheckpoints);
        for (final PojoSplit unassignedSplit : state.unassignedSplits) {
          byteBuffer.put(splitVersionedSerializer.serialize(unassignedSplit));
        }
        return byteBuffer.array();
      }

      @Override
      public EnumeratorState deserialize(int version, byte[] serialized) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
        final int numRestarts = byteBuffer.getInt();
        final int numCompletedCheckpoints = byteBuffer.getInt();

        final List<PojoSplit> splits =
          new ArrayList<>(serialized.length / SplitVersionedSerializer.LENGTH);

        final byte[] serializedSplit = new byte[SplitVersionedSerializer.LENGTH];
        while (byteBuffer.hasRemaining()) {
          byteBuffer.get(serializedSplit);
          splits.add(splitVersionedSerializer.deserialize(version, serializedSplit));
        }
        return new EnumeratorState(splits, numRestarts, numCompletedCheckpoints);
      }
    }

    private static class SplitVersionedSerializer
      implements SimpleVersionedSerializer<PojoSplit> {
      static final int LENGTH = 16;

      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public byte[] serialize(PojoSplit split) {
        final byte[] bytes = new byte[LENGTH];
        ByteBuffer.wrap(bytes).putLong(split.nextNumber).putInt(split.increment);
        return bytes;
      }

      @Override
      public PojoSplit deserialize(int version, byte[] serialized) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized);
        return new PojoSplit(byteBuffer.getLong(), byteBuffer.getInt());
      }
    }
  }
}
