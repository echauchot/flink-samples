/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.echauchot;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		DataStream<String> text = env.socketTextStream("127.0.0.1", 8088);
		final SingleOutputStreamOperator<String> map = text.map(new RichMapFunction<String, String>() {

			private IntCounter numLines = new IntCounter();

			@Override public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				getRuntimeContext().addAccumulator("num-linues", numLines);
			}

			@Override public String map(String s) throws Exception {
				numLines.add(1);
				return s;
			}
		});
		map.writeAsText("file:////tmp/out");
		JobClient client = env
			.executeAsync("Flink Streaming blocking accumulator test");
		CompletableFuture<JobStatus> statusFuture = client.getJobStatus();
		LOG.warn("status = " + statusFuture.get());

		CompletableFuture<Map<String, Object>> accumulatorsFuture = client.getAccumulators();
		LOG.warn("accus = " + accumulatorsFuture.get(5, TimeUnit.SECONDS));
	}
}
