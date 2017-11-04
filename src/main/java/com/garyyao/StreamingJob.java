package com.garyyao;

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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		// high parallelism seems to trigger faulty behavior more often
		env.setParallelism(32);

		final Properties producerConfig = new Properties();
		producerConfig.put("bootstrap.servers", "localhost:9092");
		producerConfig.put("transaction.timeout.ms", "900000");

		final DataStreamSource<String> stream = env.fromCollection(new InfiniteStringIterator(), String.class);
		stream
				.addSink(new FlinkKafkaProducer011<>(
						"producer-ea-test-" + System.currentTimeMillis(),
						new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
						producerConfig,
						FlinkKafkaProducer011.Semantic.EXACTLY_ONCE))
				.name("kafka-sink-" + System.currentTimeMillis());

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class InfiniteStringIterator implements Iterator<String>, Serializable {

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public String next() {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return UUID.randomUUID().toString();
		}

	}

}
