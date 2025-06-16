/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class SimpleRabbitListenerEndpointTests {

	private final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

	private final MessageListener messageListener = new MessageListenerAdapter();

	@Test
	public void createListener() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(messageListener);
		assertThat(endpoint.createMessageListener(container)).isSameAs(messageListener);
	}

	@Test
	public void queueAndQueueNamesSet() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(messageListener);

		endpoint.setQueueNames("foo", "bar");
		endpoint.setQueues(mock(Queue.class));

		assertThatIllegalStateException()
			.isThrownBy(() -> endpoint.setupListenerContainer(container));
	}

}
