/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.rabbit.config;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author Stephane Nicoll
 */
public class SimpleRabbitListenerEndpointTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	private final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

	private final MessageListener messageListener = new MessageListenerAdapter();

	@Test
	public void createListener() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(messageListener);
		assertSame(messageListener, endpoint.createMessageListener(container));
	}

	@Test
	public void queueAndQueueNamesSet() {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(messageListener);

		endpoint.setQueueNames("foo", "bar");
		endpoint.setQueues(mock(Queue.class));

		thrown.expect(IllegalStateException.class);
		endpoint.setupListenerContainer(container);
	}

}
