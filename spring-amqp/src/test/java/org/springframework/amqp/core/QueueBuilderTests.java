/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.core;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link QueueBuilder}
 *
 * @author Maciej Walkowiak
 * @since 1.6.0
 */
public class QueueBuilderTests {

	@Test
	public void buildsDurableQueue() {
		Queue queue = QueueBuilder.durable("name").build();

		assertThat(queue.isDurable()).isTrue();
		assertThat(queue.getName()).isEqualTo("name");
	}

	@Test
	public void buildsNonDurableQueue() {
		Queue queue = QueueBuilder.nonDurable("name").build();

		assertThat(queue.isDurable()).isFalse();
		assertThat(queue.getName()).isEqualTo("name");
	}

	@Test
	public void buildsAutoDeleteQueue() {
		Queue queue = QueueBuilder.durable("name").autoDelete().build();

		assertThat(queue.isAutoDelete()).isTrue();
	}

	@Test
	public void buildsExclusiveQueue() {
		Queue queue = QueueBuilder.durable("name").exclusive().build();

		assertThat(queue.isExclusive()).isTrue();
	}

	@Test
	public void addsArguments() {
		Queue queue = QueueBuilder.durable("name")
				.withArgument("key1", "value1")
				.withArgument("key2", "value2")
				.build();

		assertThat(queue.getArguments()).containsEntry("key1", "value1");
		assertThat(queue.getArguments()).containsEntry("key2", "value2");
	}

	@Test
	public void addsMultipleArgumentsAtOnce() {
		Map<String, Object> arguments = new HashMap<String, Object>();
		arguments.put("key1", "value1");
		arguments.put("key2", "value2");

		Queue queue = QueueBuilder.durable("name").withArguments(arguments).build();

		assertThat(queue.getArguments()).containsEntry("key1", "value1");
		assertThat(queue.getArguments()).containsEntry("key2", "value2");
	}

}
