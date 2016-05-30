/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.core;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

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

		assertTrue(queue.isDurable());
		assertEquals("name", queue.getName());
	}

	@Test
	public void buildsNonDurableQueue() {
		Queue queue = QueueBuilder.nonDurable("name").build();

		assertFalse(queue.isDurable());
		assertEquals("name", queue.getName());
	}

	@Test
	public void buildsAutoDeleteQueue() {
		Queue queue = QueueBuilder.durable("name").autoDelete().build();

		assertTrue(queue.isAutoDelete());
	}

	@Test
	public void buildsExclusiveQueue() {
		Queue queue = QueueBuilder.durable("name").exclusive().build();

		assertTrue(queue.isExclusive());
	}

	@Test
	public void addsArguments() {
		Queue queue = QueueBuilder.durable("name")
				.withArgument("key1", "value1")
				.withArgument("key2", "value2")
				.build();

		assertThat(queue.getArguments(), hasEntry("key1", (Object) "value1"));
		assertThat(queue.getArguments(), hasEntry("key2", (Object) "value2"));
	}

	@Test
	public void addsMultipleArgumentsAtOnce() {
		Map<String, Object> arguments = new HashMap<String, Object>();
		arguments.put("key1", "value1");
		arguments.put("key2", "value2");

		Queue queue = QueueBuilder.durable("name").withArguments(arguments).build();

		assertThat(queue.getArguments(), hasEntry("key1", (Object) "value1"));
		assertThat(queue.getArguments(), hasEntry("key2", (Object) "value2"));
	}

}
