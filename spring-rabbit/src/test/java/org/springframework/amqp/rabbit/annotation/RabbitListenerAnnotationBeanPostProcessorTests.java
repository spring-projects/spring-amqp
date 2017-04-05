/*
 * Copyright 2002-2017 the original author or authors.
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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.MessageListenerTestContainer;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.listener.AbstractRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

/**
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Alex Panchenko
 */
public class RabbitListenerAnnotationBeanPostProcessorTests {

	@Test
	public void simpleMessageListener() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, SimpleMessageListenerTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("One container should have been registered", 1, factory.getListenerContainers().size());
		MessageListenerTestContainer container = factory.getListenerContainers().get(0);

		RabbitListenerEndpoint endpoint = container.getEndpoint();
		assertEquals("Wrong endpoint type", MethodRabbitListenerEndpoint.class, endpoint.getClass());
		MethodRabbitListenerEndpoint methodEndpoint = (MethodRabbitListenerEndpoint) endpoint;
		assertNotNull(methodEndpoint.getBean());
		assertNotNull(methodEndpoint.getMethod());

		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
		methodEndpoint.setupListenerContainer(listenerContainer);
		assertNotNull(listenerContainer.getMessageListener());

		assertTrue("Should have been started " + container, container.isStarted());
		context.close(); // Close and stop the listeners
		assertTrue("Should have been stopped " + container, container.isStopped());
	}

	@Test
	public void simpleMessageListenerWithMixedAnnotations() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, SimpleMessageListenerWithMixedAnnotationsTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("One container should have been registered", 1, factory.getListenerContainers().size());
		MessageListenerTestContainer container = factory.getListenerContainers().get(0);

		RabbitListenerEndpoint endpoint = container.getEndpoint();
		assertEquals("Wrong endpoint type", MethodRabbitListenerEndpoint.class, endpoint.getClass());
		MethodRabbitListenerEndpoint methodEndpoint = (MethodRabbitListenerEndpoint) endpoint;
		assertNotNull(methodEndpoint.getBean());
		assertNotNull(methodEndpoint.getMethod());

		Iterator<String> iterator = ((MethodRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertEquals("testQueue", iterator.next());
		assertEquals("secondQueue", iterator.next());

		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
		methodEndpoint.setupListenerContainer(listenerContainer);
		assertNotNull(listenerContainer.getMessageListener());

		assertTrue("Should have been started " + container, container.isStarted());
		context.close(); // Close and stop the listeners
		assertTrue("Should have been stopped " + container, container.isStopped());
	}

	@Test
	public void metaAnnotationIsDiscovered() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, MetaAnnotationTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("one container should have been registered", 1, factory.getListenerContainers().size());
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		assertEquals("metaTestQueue", ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator().next());

		context.close();
	}

	@Test
	public void multipleQueueNamesTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, MultipleQueueNamesTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("one container should have been registered", 1, factory.getListenerContainers().size());
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<String> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertEquals("metaTestQueue", iterator.next());
		assertEquals("testQueue", iterator.next());

		context.close();
	}

	@Test
	public void multipleQueuesTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, MultipleQueuesTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("one container should have been registered", 1, factory.getListenerContainers().size());
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<String> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertEquals("testQueue", iterator.next());
		assertEquals("secondQueue", iterator.next());

		context.close();
	}

	@Test
	public void mixedQueuesAndQueueNamesTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, MixedQueuesAndQueueNamesTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("one container should have been registered", 1, factory.getListenerContainers().size());
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<String> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertEquals("metaTestQueue", iterator.next());
		assertEquals("testQueue", iterator.next());
		assertEquals("secondQueue", iterator.next());

		context.close();
	}

	@Test
	public void propertyResolvingToExpressionTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, PropertyResolvingToExpressionTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertEquals("one container should have been registered", 1, factory.getListenerContainers().size());
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<String> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertEquals("testQueue", iterator.next());
		assertEquals("secondQueue", iterator.next());

		context.close();
	}

	@Test
	public void invalidValueInAnnotationTestBean() {
		try {
			new AnnotationConfigApplicationContext(Config.class, InvalidValueInAnnotationTestBean.class).close();
		}
		catch (BeanCreationException e) {
			assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
			assertThat(e.getMessage(), allOf(
					containsString("@RabbitListener can't resolve"),
					containsString("as either a String or a Queue")
					));
		}
	}

	@Test
	public void multipleRoutingKeysTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(Config.class,
				MultipleRoutingKeysTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat("one container should have been registered", factory.getListenerContainers(), hasSize(1));
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		assertEquals(Collections.singletonList("my_queue"),
				((AbstractRabbitListenerEndpoint) endpoint).getQueueNames());
		final List<Queue> queues = new ArrayList<>(context.getBeansOfType(Queue.class).values());
		queues.sort(Comparator.comparing(Queue::getName));
		assertThat(queues.stream().map(Queue::getName).collect(Collectors.toList()),
				contains("my_queue", "secondQueue", "testQueue"));
		assertEquals(Collections.singletonMap("foo", "bar"), queues.get(0).getArguments());

		assertThat(context.getBeansOfType(org.springframework.amqp.core.Exchange.class).values(), hasSize(1));

		final List<Binding> bindings = new ArrayList<>(context.getBeansOfType(Binding.class).values());
		assertThat(bindings, hasSize(2));
		bindings.sort(Comparator.comparing(Binding::getRoutingKey));
		assertEquals("Binding [destination=my_queue, exchange=my_exchange, routingKey=red]", bindings.get(0).toString());
		assertEquals("Binding [destination=my_queue, exchange=my_exchange, routingKey=yellow]", bindings.get(1).toString());

		context.close();
	}

	@Test
	public void customExhangeTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(Config.class,
				CustomExchangeTestBean.class);

		final Collection<CustomExchange> exchanges = context.getBeansOfType(CustomExchange.class).values();
		assertThat(exchanges, hasSize(1));
		final CustomExchange exchange = exchanges.iterator().next();
		assertEquals("my_custom_exchange", exchange.getName());
		assertEquals("custom_type", exchange.getType());

		context.close();
	}

	@Test
	public void queuesToDeclare() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(Config.class,
				QueuesToDeclareTestBean.class);

		final List<Queue> queues = new ArrayList<>(context.getBeansOfType(Queue.class).values());
		assertThat(queues, hasSize(4));
		queues.sort(Comparator.comparing(Queue::getName));

		final Queue queue0 = queues.get(0);
		assertEquals("my_declared_queue", queue0.getName());
		assertTrue(queue0.isDurable());
		assertFalse(queue0.isAutoDelete());
		assertFalse(queue0.isExclusive());

		final Queue queue2 = queues.get(2);
		assertThat(queue2.getName(), startsWith("spring.gen-"));
		assertFalse(queue2.isDurable());
		assertTrue(queue2.isAutoDelete());
		assertTrue(queue2.isExclusive());

		context.close();
	}

	@Test
	@Ignore("To slow and doesn't have 100% confirmation")
	public void concurrency() throws InterruptedException, ExecutionException {
		final int concurrencyLevel = 8;
		final ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
		try {
			for (int i = 0; i < 1000; ++i) {
				final ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
				try {
					final Callable<?> task = () -> context.getBeanFactory().createBean(BeanForConcurrencyTesting.class);
					final List<? extends Future<?>> futures = executorService
							.invokeAll(Collections.nCopies(concurrencyLevel, task));
					for (Future<?> future : futures) {
						future.get();
					}
				}
				finally {
					context.close();
				}
			}
		}
		finally {
			executorService.shutdown();
		}
	}

	static class BeanForConcurrencyTesting {
		public void a() {
		}

		public void b() {
		}

		public void c() {
		}
	}

	@Component
	static class SimpleMessageListenerTestBean {

		@RabbitListener(queues = "testQueue")
		public void handleIt(String body) {
		}

	}

	@Component
	static class SimpleMessageListenerWithMixedAnnotationsTestBean {

		@RabbitListener(queues = {"testQueue", "#{mySecondQueue}"})
		public void handleIt(String body) {
		}

	}


	@Component
	static class MetaAnnotationTestBean {

		@FooListener
		public void handleIt(String body) {
		}
	}


	@RabbitListener(queues = "metaTestQueue")
	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	static @interface FooListener {
	}

	@Component
	static class MultipleQueueNamesTestBean {

		@RabbitListener(queues = {"metaTestQueue", "#{@myTestQueue.name}"})
		public void handleIt(String body) {
		}
	}

	@Component
	static class MultipleQueuesTestBean {

		@RabbitListener(queues = {"#{@myTestQueue}", "#{@mySecondQueue}"})
		public void handleIt(String body) {
		}
	}

	@Component
	static class MixedQueuesAndQueueNamesTestBean {

		@RabbitListener(queues = {"metaTestQueue", "#{@myTestQueue}", "#{@mySecondQueue.name}"})
		public void handleIt(String body) {
		}
	}

	@Component
	static class PropertyResolvingToExpressionTestBean {

		@RabbitListener(queues = {"${myQueueExpression}", "#{@mySecondQueue}"})
		public void handleIt(String body) {
		}
	}

	@Component
	static class InvalidValueInAnnotationTestBean {

		@RabbitListener(queues = "#{@testFactory}")
		public void handleIt(String body) {
		}
	}

	@Component
	static class MultipleRoutingKeysTestBean {

		@RabbitListener(bindings = @QueueBinding(exchange = @Exchange("my_exchange"),
				value = @org.springframework.amqp.rabbit.annotation.Queue(value = "my_queue", arguments = @Argument(name = "foo", value = "bar")),
				key = {"${xxxxxxx:red}", "yellow"}))
		public void handleIt(String body) {
		}
	}

	@Component
	static class CustomExchangeTestBean {

		@RabbitListener(bindings = @QueueBinding(exchange = @Exchange(value = "my_custom_exchange", type = "custom_type"),
				value = @org.springframework.amqp.rabbit.annotation.Queue,
				key = "test"))
		public void handleIt(String body) {
		}
	}

	@Component
	static class QueuesToDeclareTestBean {

		@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue)
		public void handleAnonymous(String body) {
		}

		@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue("my_declared_queue"))
		public void handleName(String body) {
		}
	}

	@Configuration
	@PropertySource("classpath:/org/springframework/amqp/rabbit/annotation/queue-annotation.properties")
	static class Config {

		@Bean
		public RabbitListenerAnnotationBeanPostProcessor postProcessor() {
			RabbitListenerAnnotationBeanPostProcessor postProcessor = new RabbitListenerAnnotationBeanPostProcessor();
			postProcessor.setEndpointRegistry(rabbitListenerEndpointRegistry());
			postProcessor.setContainerFactoryBeanName("testFactory");
			return postProcessor;
		}

		@Bean
		public RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry() {
			return new RabbitListenerEndpointRegistry();
		}

		@Bean
		public RabbitListenerContainerTestFactory testFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
			return new PropertySourcesPlaceholderConfigurer();
		}

		@Bean
		public Queue myTestQueue() {
			return new Queue("testQueue");
		}

		@Bean
		public Queue mySecondQueue() {
			return new Queue("secondQueue");
		}

	}

}
