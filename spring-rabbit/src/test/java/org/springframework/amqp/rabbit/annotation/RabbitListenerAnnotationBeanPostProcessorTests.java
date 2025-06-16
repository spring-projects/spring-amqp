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

package org.springframework.amqp.rabbit.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Alex Panchenko
 */
public class RabbitListenerAnnotationBeanPostProcessorTests {

	protected Class<?> getConfigClass() {
		return Config.class;
	}

	@Test
	public void simpleMessageListener() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), SimpleMessageListenerTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("One container should have been registered").isEqualTo(1);
		MessageListenerTestContainer container = factory.getListenerContainers().get(0);

		RabbitListenerEndpoint endpoint = container.getEndpoint();
		assertThat(endpoint.getClass()).as("Wrong endpoint type").isEqualTo(MethodRabbitListenerEndpoint.class);
		MethodRabbitListenerEndpoint methodEndpoint = (MethodRabbitListenerEndpoint) endpoint;
		assertThat(methodEndpoint.getBean()).isNotNull();
		assertThat(methodEndpoint.getMethod()).isNotNull();

		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
		listenerContainer.setReceiveTimeout(10);
		methodEndpoint.setupListenerContainer(listenerContainer);
		assertThat(listenerContainer.getMessageListener()).isNotNull();

		assertThat(container.isStarted()).as("Should have been started " + container).isTrue();
		context.close(); // Close and stop the listeners
		assertThat(container.isStopped()).as("Should have been stopped " + container).isTrue();
	}

	@Test
	public void simpleMessageListenerWithMixedAnnotations() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), SimpleMessageListenerWithMixedAnnotationsTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("One container should have been registered").isEqualTo(1);
		MessageListenerTestContainer container = factory.getListenerContainers().get(0);

		RabbitListenerEndpoint endpoint = container.getEndpoint();
		assertThat(endpoint.getClass()).as("Wrong endpoint type").isEqualTo(MethodRabbitListenerEndpoint.class);
		MethodRabbitListenerEndpoint methodEndpoint = (MethodRabbitListenerEndpoint) endpoint;
		assertThat(methodEndpoint.getBean()).isNotNull();
		assertThat(methodEndpoint.getMethod()).isNotNull();

		Iterator<String> iterator = ((MethodRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertThat(iterator.next()).isEqualTo("testQueue");
		assertThat(iterator.next()).isEqualTo("secondQueue");

		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
		listenerContainer.setReceiveTimeout(10);
		methodEndpoint.setupListenerContainer(listenerContainer);
		assertThat(listenerContainer.getMessageListener()).isNotNull();

		assertThat(container.isStarted()).as("Should have been started " + container).isTrue();
		context.close(); // Close and stop the listeners
		assertThat(container.isStopped()).as("Should have been stopped " + container).isTrue();
	}

	@Test
	public void metaAnnotationIsDiscovered() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), MetaAnnotationTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("one container should have been registered").isEqualTo(2);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		assertThat(((AbstractRabbitListenerEndpoint) endpoint).getQueueNames()
				.iterator()
				.next())
				.isEqualTo("metaTestQueue1");
		endpoint = factory.getListenerContainers().get(1).getEndpoint();
		assertThat(((AbstractRabbitListenerEndpoint) endpoint).getQueueNames()
				.iterator()
				.next())
				.isEqualTo("metaTestQueue2");

		context.close();
	}

	@Test
	public void metaAnnotationIsDiscoveredClassLevel() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				Config.class, MetaAnnotationTestBean2.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("one container should have been registered").isEqualTo(2);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		assertThat(((AbstractRabbitListenerEndpoint) endpoint).getQueueNames()
				.iterator()
				.next())
				.isEqualTo("metaTestQueue3");
		endpoint = factory.getListenerContainers().get(1).getEndpoint();
		assertThat(((AbstractRabbitListenerEndpoint) endpoint).getQueueNames()
				.iterator()
				.next())
				.isEqualTo("metaTestQueue4");

		context.close();
	}

	@Test
	public void multipleQueueNamesTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), MultipleQueueNamesTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("one container should have been registered").isEqualTo(1);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<String> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertThat(iterator.next()).isEqualTo("metaTestQueue");
		assertThat(iterator.next()).isEqualTo("testQueue");

		context.close();
	}

	@Test
	public void multipleQueuesTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), MultipleQueuesTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("one container should have been registered").isEqualTo(1);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<Queue> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueues().iterator();
		assertThat(iterator.next().getName()).isEqualTo("testQueue");
		assertThat(iterator.next().getName()).isEqualTo("secondQueue");

		context.close();
	}

	@Test
	public void mixedQueuesAndQueueNamesTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), MixedQueuesAndQueueNamesTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("one container should have been registered").isEqualTo(1);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<String> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueueNames().iterator();
		assertThat(iterator.next()).isEqualTo("metaTestQueue");
		assertThat(iterator.next()).isEqualTo("testQueue");
		assertThat(iterator.next()).isEqualTo("secondQueue");

		context.close();
	}

	@Test
	public void propertyResolvingToExpressionTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				getConfigClass(), PropertyResolvingToExpressionTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers().size()).as("one container should have been registered").isEqualTo(1);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		final Iterator<Queue> iterator = ((AbstractRabbitListenerEndpoint) endpoint).getQueues().iterator();
		assertThat(iterator.next().getName()).isEqualTo("testQueue");
		assertThat(iterator.next().getName()).isEqualTo("secondQueue");

		context.close();
	}

	@Test
	public void invalidValueInAnnotationTestBean() {
		try {
			new AnnotationConfigApplicationContext(getConfigClass(), InvalidValueInAnnotationTestBean.class).close();
		}
		catch (BeanCreationException e) {
			assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
			assertThat(e.getMessage()).contains("@RabbitListener.queues can't resolve")
					.contains("as a String[] or a String or a Queue");
		}
	}

	@Test
	public void multipleRoutingKeysTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(getConfigClass(),
				MultipleRoutingKeysTestBean.class);

		RabbitListenerContainerTestFactory factory = context.getBean(RabbitListenerContainerTestFactory.class);
		assertThat(factory.getListenerContainers()).as("one container should have been registered").hasSize(1);
		RabbitListenerEndpoint endpoint = factory.getListenerContainers().get(0).getEndpoint();
		assertThat(((AbstractRabbitListenerEndpoint) endpoint).getQueueNames())
				.isEqualTo(Collections.singletonList("my_queue"));
		final List<Queue> queues = new ArrayList<>(context.getBeansOfType(Queue.class).values());
		queues.sort(Comparator.comparing(Queue::getName));
		assertThat(queues.stream().map(Queue::getName).collect(Collectors.toList())).containsExactly("my_queue",
				"secondQueue", "testQueue");
		assertThat(queues.get(0).getArguments()).isEqualTo(Collections.singletonMap("foo", "bar"));

		assertThat(context.getBeansOfType(org.springframework.amqp.core.Exchange.class).values()).hasSize(1);

		final List<Binding> bindings = new ArrayList<>(context.getBeansOfType(Binding.class).values());
		assertThat(bindings).hasSize(3);
		bindings.sort(Comparator.comparing(Binding::getRoutingKey));
		assertThat(bindings.get(0).toString())
				.isEqualTo("Binding [destination=my_queue, exchange=my_exchange, routingKey=green, arguments={}]");
		assertThat(bindings.get(1).toString())
				.isEqualTo("Binding [destination=my_queue, exchange=my_exchange, routingKey=red, arguments={}]");
		assertThat(bindings.get(2).toString())
				.isEqualTo("Binding [destination=my_queue, exchange=my_exchange, routingKey=yellow, arguments={}]");

		context.close();
	}

	@Test
	public void customExchangeTestBean() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(getConfigClass(),
				CustomExchangeTestBean.class);

		final Collection<CustomExchange> exchanges = context.getBeansOfType(CustomExchange.class).values();
		assertThat(exchanges).hasSize(1);
		final CustomExchange exchange = exchanges.iterator().next();
		assertThat(exchange.getName()).isEqualTo("my_custom_exchange");
		assertThat(exchange.getType()).isEqualTo("custom_type");

		context.close();
	}

	@Test
	public void queuesToDeclare() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(getConfigClass(),
				QueuesToDeclareTestBean.class);

		final List<Queue> queues = new ArrayList<>(context.getBeansOfType(Queue.class).values());
		assertThat(queues).hasSize(4);
		queues.sort(Comparator.comparing(Queue::getName));

		final Queue queue0 = queues.get(0);
		assertThat(queue0.getName()).isEqualTo("my_declared_queue");
		assertThat(queue0.isDurable()).isTrue();
		assertThat(queue0.isAutoDelete()).isFalse();
		assertThat(queue0.isExclusive()).isFalse();

		final Queue queue2 = queues.get(2);
		assertThat(queue2.getName()).startsWith("spring.gen-");
		assertThat(queue2.isDurable()).isFalse();
		assertThat(queue2.isAutoDelete()).isTrue();
		assertThat(queue2.isExclusive()).isTrue();

		context.close();
	}

	@Test
	@Disabled("Too slow and doesn't have 100% confirmation")
	public void concurrency() throws InterruptedException, ExecutionException {
		final int concurrencyLevel = 8;
		final ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
		try {
			for (int i = 0; i < 1000; ++i) {
				final ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(getConfigClass());
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

		@FooListener("metaTestQueue1")
		@FooListener("metaTestQueue2")
		public void handleIt(String body) {
		}

	}

	@Component
	@FooListener("metaTestQueue3")
	@FooListener("metaTestQueue4")
	static class MetaAnnotationTestBean2 {

		@RabbitHandler
		public void handleIt(String body) {
		}

	}


	@RabbitListener(autoStartup = "false")
	@Target({ ElementType.METHOD, ElementType.TYPE })
	@Retention(RetentionPolicy.RUNTIME)
	@Repeatable(FooListeners.class)
	@interface FooListener {

		@AliasFor(annotation = RabbitListener.class, attribute = "queues")
		String[] value() default {};

	}

	@Target({ ElementType.METHOD, ElementType.TYPE })
	@Retention(RetentionPolicy.RUNTIME)
	@interface FooListeners {

		FooListener[] value();

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
				value = @org.springframework.amqp.rabbit.annotation.Queue(value = "my_queue",
					arguments = @Argument(name = "foo", value = "bar")),
				key = {"${xxxxxxx:red}", "#{'yellow,green'.split(',')}"}))
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
