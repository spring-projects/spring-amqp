/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.util.ErrorHandler;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0
 *
 */
public class RabbitTemplateDirectReplyToContainerIntegrationTests extends RabbitTemplateIntegrationTests {

	@Override
	protected RabbitTemplate createSendAndReceiveRabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate template = super.createSendAndReceiveRabbitTemplate(connectionFactory);
		template.setUseDirectReplyToContainer(true);
		template.setBeanName(this.testName.getMethodName() + "SendReceiveRabbitTemplate");
		return template;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void channelReleasedOnTimeout() throws Exception {
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(connectionFactory);
		template.setReplyTimeout(1);
		AtomicReference<Throwable> exception = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		ErrorHandler replyErrorHandler = t -> {
			exception.set(t);
			latch.countDown();
		};
		template.setReplyErrorHandler(replyErrorHandler);
		Object reply = template.convertSendAndReceive(ROUTE, "foo");
		assertNull(reply);
		Object container = TestUtils.getPropertyValue(template, "directReplyToContainers", Map.class)
				.get(template.isUsePublisherConnection()
						? connectionFactory.getPublisherConnectionFactory()
						: connectionFactory);
		assertEquals(0, TestUtils.getPropertyValue(container, "inUseConsumerChannels", Map.class).size());
		assertSame(replyErrorHandler, TestUtils.getPropertyValue(container, "errorHandler"));
		Message replyMessage = new Message("foo".getBytes(), new MessageProperties());
		try {
			template.onMessage(replyMessage);
		}
		catch (Exception e) {
			assertThat(e, instanceOf(AmqpRejectAndDontRequeueException.class));
			assertEquals("No correlation header in reply", e.getMessage());
		}

		replyMessage.getMessageProperties().setCorrelationId("foo");

		try {
			template.onMessage(replyMessage);
		}
		catch (Exception e) {
			assertThat(e, instanceOf(AmqpRejectAndDontRequeueException.class));
			assertEquals("Reply received after timeout", e.getMessage());
		}

		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		executor.submit(() -> {
			Message message = template.receive(ROUTE, 10_000);
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), replyMessage);
			return message;
		});
		while (template.receive(ROUTE, 100) != null) {
			// empty
		}
		reply = template.convertSendAndReceive(ROUTE, "foo");
		assertNull(reply);
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertThat(exception.get(), instanceOf(ListenerExecutionFailedException.class));
		assertEquals("Reply received after timeout", exception.get().getCause().getMessage());
		assertArrayEquals(replyMessage.getBody(),
				((ListenerExecutionFailedException) exception.get()).getFailedMessage().getBody());
		assertEquals(0, TestUtils.getPropertyValue(container, "inUseConsumerChannels", Map.class).size());
		executor.shutdownNow();
		template.stop();
		connectionFactory.destroy();
	}

}
