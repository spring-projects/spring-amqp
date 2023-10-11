/*
 * Copyright 2021-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.rabbit.stream.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.springframework.util.StringUtils;

/**
 * Builds a Spring AMQP Super Stream using a fluent API.
 * Based on <a href="https://www.rabbitmq.com/streams.html">Streams documentation</a>
 *
 * @author Sergei Kurenchuk
 * @since 3.1
 */
public class SuperStreamBuilder {
	private final Map<String, Object> arguments = new HashMap<>();
	private String name;
	private int partitions = -1;

	private BiFunction<String, Integer, List<String>> routingKeyStrategy;

	/**
	 * Creates a builder for Super Stream.
	 * @param name stream name
	 * @return the builder
	 */
	public static SuperStreamBuilder superStream(String name) {
		SuperStreamBuilder builder = new SuperStreamBuilder();
		builder.name(name);
		return builder;
	}

	/**
	 * Creates a builder for Super Stream.
	 * @param name stream name
	 * @param partitions partitions number
	 * @return the builder
	 */
	public static SuperStreamBuilder superStream(String name, int partitions) {
		return superStream(name).partitions(partitions);
	}

	/**
	 * Set the maximum age retention per stream, which will remove the oldest data.
	 * @param maxAge valid units: Y, M, D, h, m, s. For example: "7D" for a week
	 * @return the builder
	 */
	public SuperStreamBuilder maxAge(String maxAge) {
		return withArgument("x-max-age", maxAge);
	}

	/**
	 * Set the maximum log size as the retention configuration for each stream,
	 * which will truncate the log based on the data size.
	 * @param bytes the max total size in bytes
	 * @return the builder
	 */
	public SuperStreamBuilder maxLength(int bytes) {
		return withArgument("max-length-bytes", bytes);
	}

	/**
	 * Set the maximum size limit for segment file.
	 * @param bytes the max segments size in bytes
	 * @return the builder
	 */
	public SuperStreamBuilder maxSegmentSize(int bytes) {
		return withArgument("x-stream-max-segment-size-bytes", bytes);
	}

	/**
	 * Set initial replication factor for each partition.
	 * @param count number of nodes per partition
	 * @return the builder
	 */
	public SuperStreamBuilder initialClusterSize(int count) {
		return withArgument("x-initial-cluster-size", count);
	}

	/**
	 * Set extra argument which is not covered by builder's methods.
	 * @param key argument name
	 * @param value argument value
	 * @return the builder
	 */
	public SuperStreamBuilder withArgument(String key, Object value) {
		if ("x-queue-type".equals(key) && !"stream".equals(value)) {
			throw new IllegalArgumentException("Changing x-queue-type argument is not permitted");
		}
		this.arguments.put(key, value);
		return this;
	}

	/**
	 * Set the stream name.
	 * @param name the stream name.
	 * @return the builder
	 */
	public SuperStreamBuilder name(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Set the partitions number.
	 * @param partitions the partitions number
	 * @return the builder
	 */
	public SuperStreamBuilder partitions(int partitions) {
		this.partitions = partitions;
		return this;
	}

	/**
	 * Set a strategy to determine routing keys to use for the
	 * partitions. The first parameter is the queue name, the second the number of
	 * partitions, the returned list must have a size equal to the partitions.
	 * @param routingKeyStrategy the strategy
	 * @return the builder
	 */
	public SuperStreamBuilder routingKeyStrategy(BiFunction<String, Integer, List<String>> routingKeyStrategy) {
		this.routingKeyStrategy = routingKeyStrategy;
		return this;
	}

	/**
	 * Builds a final Super Stream.
	 * @return the Super Stream instance
	 */
	public SuperStream build() {
		if (!StringUtils.hasText(this.name)) {
			throw new IllegalArgumentException("Stream name can't be empty");
		}

		if (this.partitions <= 0) {
			throw new IllegalArgumentException(
					String.format("Partitions number should be great then zero. Current value; %d", this.partitions)
			);
		}

		if (this.routingKeyStrategy == null) {
			return new SuperStream(this.name, this.partitions, this.arguments);
		}

		return new SuperStream(this.name, this.partitions, this.routingKeyStrategy, this.arguments);
	}
}
