/*
 * Copyright 2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.util.Assert;

/**
 * Create Super Stream Topology {@link Declarable}s.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class SuperStream extends Declarables {

	/**
	 * Create a Super Stream with the provided parameters.
	 * @param name the stream name.
	 * @param partitions the number of partitions.
	 */
	public SuperStream(String name, int partitions) {
		this(name, partitions, (q, i) -> IntStream.range(0, i)
				.mapToObj(String::valueOf)
				.collect(Collectors.toList()));
	}

	/**
	 * Create a Super Stream with the provided parameters.
	 * @param name the stream name.
	 * @param partitions the number of partitions.
	 * @param routingKeyStrategy a strategy to determine routing keys to use for the
	 * partitions. The first parameter is the queue name, the second the number of
	 * partitions, the returned list must have a size equal to the partitions.
	 */
	public SuperStream(String name, int partitions, BiFunction<String, Integer, List<String>> routingKeyStrategy) {
		super(declarables(name, partitions, routingKeyStrategy));
	}

	private static Collection<Declarable> declarables(String name, int partitions,
			BiFunction<String, Integer, List<String>> routingKeyStrategy) {

		List<Declarable> declarables = new ArrayList<>();
		List<String> rks = routingKeyStrategy.apply(name, partitions);
		Assert.state(rks.size() == partitions, () -> "Expected " + partitions + " routing keys, not " + rks.size());
		declarables.add(new DirectExchange(name, true, false, Map.of("x-super-stream", true)));
		for (int i = 0; i < partitions; i++) {
			String rk = rks.get(i);
			Queue q = new Queue(name + "-" + i, true, false, false, Map.of("x-queue-type", "stream"));
			declarables.add(q);
			declarables.add(new Binding(q.getName(), DestinationType.QUEUE, name, rk,
					Map.of("x-stream-partition-order", i)));
		}
		return declarables;
	}

}
