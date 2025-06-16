/*
 * Copyright 2021-present the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;

/**
 * @author Sergei Kurenchuk
 * @since 3.1
 */
public class SuperStreamConfigurationTests {

	@Test
	void argumentsShouldBeAppliedToAllPartitions() {
		int partitions = 3;
		var argKey = "x-max-age";
		var argValue = 10_000;

		Map<String, Object> testArguments = Map.of(argKey, argValue);
		SuperStream superStream = new SuperStream("stream", partitions, testArguments);

		List<Queue> streams = superStream.getDeclarablesByType(Queue.class);
		Assertions.assertEquals(partitions, streams.size());

		streams.forEach(
				it -> {
					Object value = it.getArguments().get(argKey);
					Assertions.assertNotNull(value, "Arg value should be present");
					Assertions.assertEquals(argValue, value, "Value should be the same");
				}
		);
	}

	@Test
	void testCustomPartitionsRoutingStrategy() {
		var streamName = "test-super-stream-name";
		var partitions = 3;
		var names = List.of("test.stream.1", "test.stream.2", "test.stream.3");

		SuperStream superStream = SuperStreamBuilder.superStream(streamName, partitions)
				.routingKeyStrategy((name, partition) -> names)
				.build();

		List<Binding> bindings = superStream.getDeclarablesByType(Binding.class);
		Set<String> routingKeys = bindings.stream().map(Binding::getRoutingKey).collect(Collectors.toSet());
		Assertions.assertTrue(routingKeys.containsAll(names));
	}

	@Test
	void builderMustSetupNameAndPartitionsNumber() {
		var name = "test-super-stream-name";
		var partitions = 3;
		SuperStream superStream = SuperStreamBuilder.superStream(name, partitions).build();
		List<Queue> streams = superStream.getDeclarablesByType(Queue.class);
		Assertions.assertEquals(partitions, streams.size());

		streams.forEach(it -> Assertions.assertTrue(it.getName().startsWith(name)));
	}

	@Test
	void builderMustSetupArguments() {
		var finalPartitionsNumber = 4;
		var finalName = "test-name";
		var maxAge = "1D";
		var maxLength = 10_000_000L;
		var maxSegmentsSize = 100_000L;
		var initialClusterSize = 5;

		var testArgName = "test-key";
		var testArgValue = "test-value";

		SuperStream superStream = SuperStreamBuilder.superStream("name", 3)
				.partitions(finalPartitionsNumber)
				.maxAge(maxAge)
				.maxLength(maxLength)
				.maxSegmentSize(maxSegmentsSize)
				.initialClusterSize(initialClusterSize)
				.name(finalName)
				.withArgument(testArgName, testArgValue)
				.build();

		List<Queue> streams = superStream.getDeclarablesByType(Queue.class);

		Assertions.assertEquals(finalPartitionsNumber, streams.size());
		streams.forEach(
				it -> {
					Assertions.assertTrue(it.getName().startsWith(finalName));
					Assertions.assertEquals(maxAge, it.getArguments().get("x-max-age"));
					Assertions.assertEquals(maxLength, it.getArguments().get("max-length-bytes"));
					Assertions.assertEquals(initialClusterSize, it.getArguments().get("x-initial-cluster-size"));
					Assertions.assertEquals(maxSegmentsSize, it.getArguments().get("x-stream-max-segment-size-bytes"));
					Assertions.assertEquals(testArgValue, it.getArguments().get(testArgName));
				}
		);
	}

	@Test
	void builderShouldForbidInternalArgumentsChanges() {
		SuperStreamBuilder builder = SuperStreamBuilder.superStream("name", 3);

		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withArgument("x-queue-type", "quorum"));
	}

	@Test
	void nameCantBeEmpty() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> SuperStreamBuilder.superStream("", 3).build()
		);

		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> SuperStreamBuilder.superStream("testName", 3).name("").build()
		);

		Assertions.assertDoesNotThrow(
				() -> SuperStreamBuilder.superStream("testName", 3).build()
		);
	}

	@Test
	void partitionsNumberShouldBeGreatThenZero() {
		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> SuperStreamBuilder.superStream("testName", 0).build()
		);

		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> SuperStreamBuilder.superStream("testName", -1).build()
		);

		Assertions.assertThrows(
				IllegalArgumentException.class,
				() -> SuperStreamBuilder.superStream("testName", 1).partitions(0).build()
		);

		Assertions.assertDoesNotThrow(
				() -> SuperStreamBuilder.superStream("testName", 1).build()
		);
	}

}
