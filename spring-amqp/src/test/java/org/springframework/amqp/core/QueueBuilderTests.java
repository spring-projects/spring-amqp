/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
