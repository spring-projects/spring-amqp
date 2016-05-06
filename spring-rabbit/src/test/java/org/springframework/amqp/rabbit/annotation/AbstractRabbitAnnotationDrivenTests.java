/*
 * Copyright 2014-2016 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.AbstractRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

import com.rabbitmq.client.Channel;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public abstract class AbstractRabbitAnnotationDrivenTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public abstract void sampleConfiguration();

	@Test
	public abstract void fullConfiguration();

	@Test
	public abstract void fullConfigurableConfiguration();

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

	@Test
	public abstract void rabbitListeners();

	/**
	 * Test for {@link SampleBean} discovery. If a factory with the default name
	 * is set, an endpoint will use it automatically
	 */
	public void testSampleConfiguration(ApplicationContext context, int expectedDefaultContainers) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("simpleFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(expectedDefaultContainers, defaultFactory.getListenerContainers().size());
		assertEquals(1, simpleFactory.getListenerContainers().size());
		Map<String, org.springframework.amqp.core.Queue> queues = context
				.getBeansOfType(org.springframework.amqp.core.Queue.class);
		for (org.springframework.amqp.core.Queue queue : queues.values()) {
			assertTrue(queue.isIgnoreDeclarationExceptions());
		}
		Map<String, org.springframework.amqp.core.Exchange> exchanges = context
				.getBeansOfType(org.springframework.amqp.core.Exchange.class);
		for (org.springframework.amqp.core.Exchange exchange : exchanges.values()) {
			assertTrue(exchange.isIgnoreDeclarationExceptions());
		}
		Map<String, org.springframework.amqp.core.Binding> bindings = context
				.getBeansOfType(org.springframework.amqp.core.Binding.class);
		for (org.springframework.amqp.core.Binding binding : bindings.values()) {
			assertTrue(binding.isIgnoreDeclarationExceptions());
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
				customRegistry.getListenerContainerIds().size());
		assertEquals("Wrong number of containers in the registry", 2,
				customRegistry.getListenerContainers().size());
		assertNotNull("Container with custom id on the annotation should be found",
				customRegistry.getListenerContainer("listenerId"));
		assertNotNull("Container created with custom id should be found",
				customRegistry.getListenerContainer("myCustomEndpointId"));
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

	/**
	 * Test for {@link ValidationBean} with a validator ({@link TestValidator}) specified
	 * in a custom {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory}.
	 *
	 * The test should throw a {@link org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException}
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

	/**
	 * Test for {@link RabbitListenersBean} that validates that the
	 * {@code @RabbitListener} annotations generate one specific container per annotation.
	 */
	public void testRabbitListenerRepeatable(ApplicationContext context) {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(4, simpleFactory.getListenerContainers().size());

		MethodRabbitListenerEndpoint first = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("first").getEndpoint();
		assertEquals("first", first.getId());
		assertEquals("myQueue", first.getQueueNames().iterator().next());

		MethodRabbitListenerEndpoint second = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("second").getEndpoint();
		assertEquals("second", second.getId());
		assertEquals("anotherQueue", second.getQueueNames().iterator().next());

		MethodRabbitListenerEndpoint third = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("third").getEndpoint();
		assertEquals("third", third.getId());
		assertEquals("class1", third.getQueueNames().iterator().next());

		MethodRabbitListenerEndpoint fourth = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("fourth").getEndpoint();
		assertEquals("fourth", fourth.getId());
		assertEquals("class2", fourth.getQueueNames().iterator().next());
	}

	private void assertQueues(AbstractRabbitListenerEndpoint actual, String... expectedQueues) {
		Collection<String> actualQueues = actual.getQueueNames();
		for (String expectedQueue : expectedQueues) {
			assertTrue("Queue '" + expectedQueue + "' not found", actualQueues.contains(expectedQueue));
		}
		assertEquals("Wrong number of queues", expectedQueues.length, actualQueues.size());
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

	@Component
	static class FullBean {

		@RabbitListener(id = "listener1", containerFactory = "simpleFactory", queues = {"queue1", "queue2"},
				exclusive = true, priority = "34", admin = "rabbitAdmin")
		public void fullHandle(String msg) {

		}
	}

	@Component
	static class FullConfigurableBean {

		@RabbitListener(id = "${rabbit.listener.id}", containerFactory = "${rabbit.listener.containerFactory}",
				queues = {"${rabbit.listener.queue}", "queue2"}, exclusive = true,
				priority = "${rabbit.listener.priority}", admin = "${rabbit.listener.admin}")
		public void fullHandle(String msg) {

		}
	}

	@Component
	static class CustomBean {

		@RabbitListener(id = "listenerId", containerFactory = "customFactory", queues = "myQueue")
		public void customHandle(String msg) {
		}
	}

	static class DefaultBean {

		@RabbitListener(queues = "myQueue")
		public void handleIt(String msg) {
		}
	}

	@Component
	static class ValidationBean {

		@RabbitListener(containerFactory = "defaultFactory", queues = "myQueue")
		public void defaultHandle(@Validated String msg) {
		}
	}

	@Component
	static class RabbitListenersBean {

		@RabbitListeners({
				@RabbitListener(id = "first", queues = "myQueue"),
				@RabbitListener(id = "second", queues = "anotherQueue")
		})
		public void repeatableHandle(String msg) {
		}

	}

	@Component
	@RabbitListeners({
		@RabbitListener(id = "third", queues = "class1"),
		@RabbitListener(id = "fourth", queues = "class2")
	})
	static class ClassLevelListenersBean {

		@RabbitHandler
		public void repeatableHandle(String msg) {
		}

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
