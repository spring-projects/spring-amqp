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

package org.springframework.amqp.rabbit.annotation;

import java.util.Collection;

import com.rabbitmq.client.Channel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.AbstractRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.config.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author Stephane Nicoll
 */
public abstract class AbstractRabbitAnnotationDrivenTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public abstract void sampleConfiguration();

	@Test
	public abstract void fullConfiguration();

	@Test
	public abstract void noRabbitAdminConfiguration();

	@Test
	public abstract void customConfiguration();

	@Test
	public abstract void explicitContainerFactory();

	@Test
	public abstract void defaultContainerFactory();

	@Test
	public abstract void rabbitHandlerMethodFactoryConfiguration() throws Exception;

	/**
	 * Test for {@link SampleBean} discovery. If a factory with the default name
	 * is set, an endpoint will use it automatically
	 */
	public void testSampleConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("simpleFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(1, defaultFactory.getListenerContainers().size());
		assertEquals(1, simpleFactory.getListenerContainers().size());
	}

	@Component
	static class SampleBean {

		@RabbitListener(queues = "myQueue")
		public void defaultHandle(String msg) {
		}

		@RabbitListener(containerFactory = "simpleFactory", queues = "myQueue")
		public void simpleHandle(String msg) {
		}
	}

	/**
	 * Test for {@link FullBean} discovery. In this case, no default is set because
	 * all endpoints provide a default registry. This shows that the default factory
	 * is only retrieved if it needs to be.
	 */
	public void testFullConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("simpleFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(1, simpleFactory.getListenerContainers().size());
		MethodRabbitListenerEndpoint endpoint = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainers().get(0).getEndpoint();
		assertEquals("listener1", endpoint.getId());
		assertQueues(endpoint, "queue1", "queue2");
		assertTrue("No queue instances should be set", endpoint.getQueues().isEmpty());
		assertEquals(true, endpoint.isExclusive());
		assertEquals(new Integer(34), endpoint.getPriority());
		assertEquals("routing-123", endpoint.getResponseRoutingKey());
		assertSame(context.getBean("rabbitAdmin"), endpoint.getAdmin());

		// Resolve the container and invoke a message on it

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		endpoint.setupListenerContainer(container);
		MessagingMessageListenerAdapter listener = (MessagingMessageListenerAdapter) container.getMessageListener();

		MessageProperties properties = new MessageProperties();
		properties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		Message amqpMessage = new Message("Hello".getBytes(), properties);

		try {
			listener.onMessage(amqpMessage, mock(Channel.class));
		}
		catch (Exception e) {
			fail("should not have failed to process simple message but got " + e.getMessage());
		}
	}

	@Component
	static class FullBean {

		@RabbitListener(id = "listener1", containerFactory = "simpleFactory", queues = {"queue1", "queue2"},
				exclusive = true, priority = 34, responseRoutingKey = "routing-123", admin = "rabbitAdmin")
		public void fullHandle(String msg) {

		}
	}

	/**
	 * Test for {@link CustomBean} and an manually endpoint registered
	 * with "myCustomEndpointId". The custom endpoint does not provide
	 * any factory so it's registered with the default one
	 */
	public void testCustomConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		RabbitListenerContainerTestFactory customFactory =
				context.getBean("customFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(1, defaultFactory.getListenerContainers().size());
		assertEquals(1, customFactory.getListenerContainers().size());
		RabbitListenerEndpoint endpoint = defaultFactory.getListenerContainers().get(0).getEndpoint();
		assertEquals("Wrong endpoint type", SimpleRabbitListenerEndpoint.class, endpoint.getClass());
		assertEquals("Wrong listener set in custom endpoint", context.getBean("simpleMessageListener"),
				((SimpleRabbitListenerEndpoint) endpoint).getMessageListener());

		RabbitListenerEndpointRegistry customRegistry =
				context.getBean("customRegistry", RabbitListenerEndpointRegistry.class);
		assertEquals("Wrong number of containers in the registry", 2,
				customRegistry.getListenerContainers().size());
		assertNotNull("Container with custom id on the annotation should be found",
				customRegistry.getListenerContainer("listenerId"));
		assertNotNull("Container created with custom id should be found",
				customRegistry.getListenerContainer("myCustomEndpointId"));
	}

	@Component
	static class CustomBean {

		@RabbitListener(id = "listenerId", containerFactory = "customFactory", queues = "myQueue")
		public void customHandle(String msg) {
		}
	}

	/**
	 * Test for {@link DefaultBean} that does not define the container
	 * factory to use as a default is registered with an explicit
	 * default.
	 */
	public void testExplicitContainerFactoryConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("simpleFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(1, defaultFactory.getListenerContainers().size());
	}

	/**
	 * Test for {@link DefaultBean} that does not define the container
	 * factory to use as a default is registered with the default name.
	 */
	public void testDefaultContainerFactoryConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(1, defaultFactory.getListenerContainers().size());
	}

	static class DefaultBean {

		@RabbitListener(queues = "myQueue")
		public void handleIt(String msg) {
		}
	}

	/**
	 * Test for {@link ValidationBean} with a validator ({@link TestValidator}) specified
	 * in a custom {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory}.
	 *
	 * The test should throw a {@link org.springframework.amqp.rabbit.listener.ListenerExecutionFailedException}
	 */
	public void testRabbitHandlerMethodFactoryConfiguration(ApplicationContext context) throws Exception {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("defaultFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(1, simpleFactory.getListenerContainers().size());
		MethodRabbitListenerEndpoint endpoint = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainers().get(0).getEndpoint();

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		endpoint.setupListenerContainer(container);
		MessagingMessageListenerAdapter listener = (MessagingMessageListenerAdapter) container.getMessageListener();

		MessageProperties properties = new MessageProperties();
		properties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		Message amqpMessage = new Message("failValidation".getBytes(), properties);

		listener.onMessage(amqpMessage, mock(Channel.class));
	}

	@Component
	static class ValidationBean {

		@RabbitListener(containerFactory = "defaultFactory", queues = "myQueue")
		public void defaultHandle(@Validated String msg) {
		}
	}

	private void assertQueues(AbstractRabbitListenerEndpoint actual, String... expectedQueues) {
		Collection<String> actualQueues = actual.getQueueNames();
		for (String expectedQueue : expectedQueues) {
			assertTrue("Queue '" + expectedQueue + "' not found", actualQueues.contains(expectedQueue));
		}
		assertEquals("Wrong number of queues", expectedQueues.length, actualQueues.size());
	}

	private void assertQueues(AbstractRabbitListenerEndpoint actual, Queue... expectedQueues) {
		Collection<Queue> actualQueues = actual.getQueues();
		for (Queue expectedQueue : expectedQueues) {
			assertTrue("Queue '" + expectedQueue + "' not found", actualQueues.contains(expectedQueue));
		}
		assertEquals("Wrong number of queues", expectedQueues.length, actualQueues.size());
	}

	static class TestValidator implements Validator {

		@Override
		public boolean supports(Class<?> clazz) {
			return String.class.isAssignableFrom(clazz);
		}

		@Override
		public void validate(Object target, Errors errors) {
			String value = (String) target;
			if ("failValidation".equals(value)) {
				errors.reject("TEST: expected invalid value");
			}
		}
	}
}
