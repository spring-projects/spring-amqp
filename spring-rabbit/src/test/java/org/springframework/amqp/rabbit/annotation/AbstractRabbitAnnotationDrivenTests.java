/*
 * Copyright 2014-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.util.Collection;
import java.util.Map;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Ngoc Nhan
 */
public abstract class AbstractRabbitAnnotationDrivenTests {

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
		assertThat(defaultFactory.getBeanName()).isEqualTo("rabbitListenerContainerFactory");
		assertThat(defaultFactory.getListenerContainers()).hasSize(expectedDefaultContainers);
		assertThat(simpleFactory.getBeanName()).isEqualTo("simpleFactory");
		assertThat(simpleFactory.getListenerContainers()).hasSize(1);
		Map<String, org.springframework.amqp.core.Queue> queues = context
				.getBeansOfType(org.springframework.amqp.core.Queue.class);
		for (org.springframework.amqp.core.Queue queue : queues.values()) {
			assertThat(queue.isIgnoreDeclarationExceptions()).isTrue();
			assertThat(queue.shouldDeclare()).isFalse();
			Collection<?> admins = queue.getDeclaringAdmins();
			checkAdmin(admins);
		}
		Map<String, org.springframework.amqp.core.Exchange> exchanges = context
				.getBeansOfType(org.springframework.amqp.core.Exchange.class);
		for (org.springframework.amqp.core.Exchange exchange : exchanges.values()) {
			assertThat(exchange.isIgnoreDeclarationExceptions()).isTrue();
			assertThat(exchange.shouldDeclare()).isFalse();
			Collection<?> admins = exchange.getDeclaringAdmins();
			checkAdmin(admins);
		}
		Map<String, org.springframework.amqp.core.Binding> bindings = context
				.getBeansOfType(org.springframework.amqp.core.Binding.class);
		for (org.springframework.amqp.core.Binding binding : bindings.values()) {
			assertThat(binding.isIgnoreDeclarationExceptions()).isTrue();
			assertThat(binding.shouldDeclare()).isFalse();
			Collection<?> admins = binding.getDeclaringAdmins();
			checkAdmin(admins);
		}
	}

	private void checkAdmin(Collection<?> admins) {
		assertThat(admins).hasSize(1);
		Object admin = admins.iterator().next();
		assertThat(admin instanceof RabbitAdmin ? ((RabbitAdmin) admin).getBeanName() : admin).isEqualTo("myAdmin");
	}

	/**
	 * Test for {@link FullBean} discovery. In this case, no default is set because
	 * all endpoints provide a default registry. This shows that the default factory
	 * is only retrieved if it needs to be.
	 */
	public void testFullConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("simpleFactory", RabbitListenerContainerTestFactory.class);
		assertThat(simpleFactory.getBeanName()).isEqualTo("simpleFactory");
		assertThat(simpleFactory.getListenerContainers()).hasSize(1);
		MethodRabbitListenerEndpoint endpoint = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainers().get(0).getEndpoint();
		assertThat(endpoint.getId()).isEqualTo("listener1");
		assertQueues(endpoint, "queue1", "queue2");
		assertThat(endpoint.getQueues().isEmpty()).as("No queue instances should be set").isTrue();
		assertThat(endpoint.isExclusive()).isEqualTo(true);
		assertThat(endpoint.getPriority()).isEqualTo(Integer.valueOf(34));
		assertThat(endpoint.getAdmin()).isSameAs(context.getBean("rabbitAdmin"));

		// Resolve the container and invoke a message on it

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setReceiveTimeout(10);
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
	 * Test for {@link CustomBean} and a manually registered endpoint
	 * with "myCustomEndpointId". The custom endpoint does not provide
	 * any factory, so it's registered with the default one
	 */
	public void testCustomConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		RabbitListenerContainerTestFactory customFactory =
				context.getBean("customFactory", RabbitListenerContainerTestFactory.class);
		assertThat(defaultFactory.getBeanName()).isEqualTo("rabbitListenerContainerFactory");
		assertThat(defaultFactory.getListenerContainers()).hasSize(1);
		assertThat(customFactory.getBeanName()).isEqualTo("customFactory");
		assertThat(customFactory.getListenerContainers()).hasSize(1);
		RabbitListenerEndpoint endpoint = defaultFactory.getListenerContainers().get(0).getEndpoint();
		assertThat(endpoint.getClass()).as("Wrong endpoint type").isEqualTo(SimpleRabbitListenerEndpoint.class);
		assertThat(((SimpleRabbitListenerEndpoint) endpoint).getMessageListener())
				.as("Wrong listener set in custom endpoint").isEqualTo(context.getBean("simpleMessageListener"));

		RabbitListenerEndpointRegistry customRegistry =
				context.getBean("customRegistry", RabbitListenerEndpointRegistry.class);
		assertThat(customRegistry.getListenerContainerIds()).hasSize(2);
		assertThat(customRegistry.getListenerContainers()).hasSize(2);
		assertThat(customRegistry.getListenerContainer("listenerId")).isNotNull();
		assertThat(customRegistry.getListenerContainer("myCustomEndpointId")).isNotNull();
	}

	/**
	 * Test for {@link DefaultBean} that does not define the container
	 * factory to use as a default is registered with an explicit
	 * default.
	 */
	public void testExplicitContainerFactoryConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("simpleFactory", RabbitListenerContainerTestFactory.class);
		assertThat(defaultFactory.getBeanName()).isEqualTo("simpleFactory");
		assertThat(defaultFactory.getListenerContainers()).hasSize(1);
	}

	/**
	 * Test for {@link DefaultBean} that does not define the container
	 * factory to use as a default is registered with the default name.
	 */
	public void testDefaultContainerFactoryConfiguration(ApplicationContext context) {
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertThat(defaultFactory.getListenerContainers()).hasSize(1);
	}

	/**
	 * Test for {@link ValidationBean} with a validator ({@link TestValidator}) specified
	 * in a custom {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory}.
	 * The test should throw a {@link org.springframework.amqp.rabbit.support.ListenerExecutionFailedException}
	 */
	public void testRabbitHandlerMethodFactoryConfiguration(ApplicationContext context) throws Exception {
		RabbitListenerContainerTestFactory simpleFactory =
				context.getBean("defaultFactory", RabbitListenerContainerTestFactory.class);
		assertThat(simpleFactory.getListenerContainers()).hasSize(1);
		MethodRabbitListenerEndpoint endpoint = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainers().get(0).getEndpoint();

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setReceiveTimeout(10);
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
		assertThat(simpleFactory.getListenerContainers()).hasSize(4);

		MethodRabbitListenerEndpoint first = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("first").getEndpoint();
		assertThat(first.getId()).isEqualTo("first");
		assertThat(first.getQueueNames().iterator().next()).isEqualTo("myQueue");

		MethodRabbitListenerEndpoint second = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("second").getEndpoint();
		assertThat(second.getId()).isEqualTo("second");
		assertThat(second.getQueueNames().iterator().next()).isEqualTo("anotherQueue");

		MethodRabbitListenerEndpoint third = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("third").getEndpoint();
		assertThat(third.getId()).isEqualTo("third");
		assertThat(third.getQueueNames().iterator().next()).isEqualTo("class1");

		MethodRabbitListenerEndpoint fourth = (MethodRabbitListenerEndpoint)
				simpleFactory.getListenerContainer("fourth").getEndpoint();
		assertThat(fourth.getId()).isEqualTo("fourth");
		assertThat(fourth.getQueueNames().iterator().next()).isEqualTo("class2");
	}

	private void assertQueues(AbstractRabbitListenerEndpoint actual, String... expectedQueues) {
		Collection<String> actualQueues = actual.getQueueNames();
		for (String expectedQueue : expectedQueues) {
			assertThat(actualQueues.contains(expectedQueue)).as("Queue '" + expectedQueue + "' not found").isTrue();
		}
		assertThat(actualQueues.size()).as("Wrong number of queues").isEqualTo(expectedQueues.length);
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

		@RabbitListener(id = "#{'${rabbit.listener.id}'}",
				containerFactory = "#{'${rabbit.listener.containerFactory}'}",
				queues = {"${rabbit.listener.queue}", "queue2"}, exclusive = true,
				priority = "#{'${rabbit.listener.priority}'}", admin = "#{'${rabbit.listener.admin}'}")
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

		@RabbitListener(id = "first", queues = "myQueue")
		@RabbitListener(id = "second", queues = "anotherQueue")
		public void repeatableHandle(String msg) {
		}

	}

	@Component
	@RabbitListener(id = "third", queues = "class1")
	@RabbitListener(id = "fourth", queues = "class2")
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
