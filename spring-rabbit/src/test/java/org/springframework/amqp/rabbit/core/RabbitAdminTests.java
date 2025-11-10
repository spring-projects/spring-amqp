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

package org.springframework.amqp.rabbit.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Artem Yakshin
 * @author Ngoc Nhan
 *
 * @since 1.4.1
 *
 */
@RabbitAvailable(management = true)
public class RabbitAdminTests extends NeedsManagementTests {

	@Test
	public void testSettingOfNullConnectionFactory() {
		ConnectionFactory connectionFactory = null;
		assertThatIllegalArgumentException().isThrownBy(() -> new RabbitAdmin(connectionFactory))
				.withMessage("ConnectionFactory must not be null");
	}

	@Test
	public void testNoFailOnStartupWithMissingBroker() {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("foo");
		connectionFactory.setPort(434343);
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo", new Queue("queue"));
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(applicationContext);
		rabbitAdmin.setAutoStartup(true);
		rabbitAdmin.afterPropertiesSet();
		connectionFactory.destroy();
	}

	@Test
	public void testFailOnFirstUseWithMissingBroker() {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("localhost");
		connectionFactory.setPort(434343);
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo", new Queue("queue"));
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(applicationContext);
		rabbitAdmin.setAutoStartup(true);
		rabbitAdmin.afterPropertiesSet();
		assertThatIllegalArgumentException().isThrownBy(rabbitAdmin::declareQueue);
		connectionFactory.destroy();
	}

	@Test
	public void testProperties() throws Exception {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory();
		connectionFactory.setHost("localhost");
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		String queueName = "test.properties." + System.currentTimeMillis();
		try {
			rabbitAdmin.declareQueue(new Queue(queueName));
			new RabbitTemplate(connectionFactory).convertAndSend(queueName, "foo");
			await("Message count = 0").until(() -> messageCount(rabbitAdmin, queueName) > 0);
			Channel channel = connectionFactory.createConnection().createChannel(false);
			DefaultConsumer consumer = new DefaultConsumer(channel);
			channel.basicConsume(queueName, true, consumer);
			await("Message count > 0").until(() -> messageCount(rabbitAdmin, queueName) == 0);
			Properties props = rabbitAdmin.getQueueProperties(queueName);
			assertThat(props).containsEntry(RabbitAdmin.QUEUE_CONSUMER_COUNT, 1);
			channel.close();
		}
		finally {
			rabbitAdmin.deleteQueue(queueName);
			connectionFactory.destroy();
		}
	}

	private long messageCount(RabbitAdmin rabbitAdmin, String queueName) {
		QueueInformation info = rabbitAdmin.getQueueInfo(queueName);
		assertThat(info).isNotNull();
		return info.getMessageCount();
	}

	@Test
	public void testTemporaryLogs() {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory();
		connectionFactory.setHost("localhost");
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		try {
			ApplicationContext ctx = mock();
			Map<String, Queue> queues = new HashMap<>();
			queues.put("nonDurQ", new Queue("testq.nonDur", false, false, false));
			queues.put("adQ", new Queue("testq.ad", true, false, true));
			queues.put("exclQ", new Queue("testq.excl", true, true, false));
			queues.put("allQ", new Queue("testq.all", false, true, true));
			given(ctx.getBeansOfType(Queue.class, false, false)).willReturn(queues);
			Map<String, Exchange> exchanges = new HashMap<>();
			exchanges.put("nonDurEx", new DirectExchange("testex.nonDur", false, false));
			exchanges.put("adEx", new DirectExchange("testex.ad", true, true));
			exchanges.put("allEx", new DirectExchange("testex.all", false, true));
			given(ctx.getBeansOfType(Exchange.class, false, false)).willReturn(exchanges);
			rabbitAdmin.setApplicationContext(ctx);
			rabbitAdmin.afterPropertiesSet();
			Log logger = spy(TestUtils.getPropertyValue(rabbitAdmin, "logger", Log.class));
			willReturn(true).given(logger).isInfoEnabled();
			willDoNothing().given(logger).info(anyString());
			new DirectFieldAccessor(rabbitAdmin).setPropertyValue("logger", logger);
			connectionFactory.createConnection().close(); // force declarations
			ArgumentCaptor<String> log = ArgumentCaptor.forClass(String.class);
			verify(logger, times(7)).info(log.capture());
			List<String> logs = log.getAllValues();
			Collections.sort(logs);
			assertThat(logs.get(0)).contains("(testex.ad) durable:true, auto-delete:true");
			assertThat(logs.get(1)).contains("(testex.all) durable:false, auto-delete:true");
			assertThat(logs.get(2)).contains("(testex.nonDur) durable:false, auto-delete:false");
			assertThat(logs.get(3)).contains("(testq.ad) durable:true, auto-delete:true, exclusive:false");
			assertThat(logs.get(4)).contains("(testq.all) durable:false, auto-delete:true, exclusive:true");
			assertThat(logs.get(5)).contains("(testq.excl) durable:true, auto-delete:false, exclusive:true");
			assertThat(logs.get(6)).contains("(testq.nonDur) durable:false, auto-delete:false, exclusive:false");
		}
		finally {
			cleanQueuesAndExchanges(rabbitAdmin);
			connectionFactory.destroy();
		}
	}

	private void cleanQueuesAndExchanges(RabbitAdmin rabbitAdmin) {
		rabbitAdmin.deleteQueue("testq.nonDur");
		rabbitAdmin.deleteQueue("testq.ad");
		rabbitAdmin.deleteQueue("testq.excl");
		rabbitAdmin.deleteQueue("testq.all");
		rabbitAdmin.deleteExchange("testex.nonDur");
		rabbitAdmin.deleteExchange("testex.ad");
		rabbitAdmin.deleteExchange("testex.all");
	}

	@Test
	@LogLevels(classes = RabbitAdmin.class, level = "DEBUG")
	public void testMultiEntities() {
		ConfigurableApplicationContext ctx = new AnnotationConfigApplicationContext(Config.class);
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		template.convertAndSend("e1", "k1", "foo");
		template.convertAndSend("e2", "k2", "bar");
		template.convertAndSend("e3", "k3", "baz");
		template.convertAndSend("e4", "k4", "qux");
		assertThat(template.receiveAndConvert("q1")).isEqualTo("foo");
		assertThat(template.receiveAndConvert("q2")).isEqualTo("bar");
		assertThat(template.receiveAndConvert("q3")).isEqualTo("baz");
		assertThat(template.receiveAndConvert("q4")).isEqualTo("qux");
		RabbitAdmin admin = ctx.getBean(RabbitAdmin.class);
		admin.deleteQueue("q1");
		admin.deleteQueue("q2");
		admin.deleteQueue("q3");
		admin.deleteQueue("q4");
		admin.deleteExchange("e1");
		admin.deleteExchange("e2");
		admin.deleteExchange("e3");
		admin.deleteExchange("e4");
		assertThat(admin.getQueueProperties(ctx.getBean(Config.class).prototypeQueueName)).isNull();
		Declarables mixedDeclarables = ctx.getBean("ds", Declarables.class);
		assertThat(mixedDeclarables.getDeclarablesByType(Queue.class))
				.hasSize(1)
				.extracting(Queue::getName)
				.contains("q4");
		assertThat(mixedDeclarables.getDeclarablesByType(Exchange.class))
				.hasSize(1)
				.extracting(Exchange::getName)
				.contains("e4");
		assertThat(mixedDeclarables.getDeclarablesByType(Binding.class))
				.hasSize(1)
				.extracting(Binding::getDestination)
				.contains("q4");
		ctx.close();
	}

	@Test
	public void testAvoidHangAMQP_508() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		RabbitAdmin admin = new RabbitAdmin(cf);
		String longName = new String(new byte[300]).replace('\u0000', 'x');
		assertThatException().isThrownBy(() -> admin.declareQueue(new Queue(longName)));
		String goodName = "foobar";
		admin.declareQueue(new Queue(goodName));
		assertThat(admin.getQueueProperties(longName)).isNull();
		assertThat(admin.getQueueProperties(goodName)).isNotNull();
		admin.deleteQueue(goodName);
		cf.destroy();
	}

	@Test
	public void testIgnoreDeclarationExceptionsTimeout() throws Exception {
		com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory = mock();
		TimeoutException toBeThrown = new TimeoutException("test");
		willThrow(toBeThrown).given(rabbitConnectionFactory).newConnection(any(ExecutorService.class), anyString());
		CachingConnectionFactory ccf = new CachingConnectionFactory(rabbitConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		RabbitAdmin admin = new RabbitAdmin(ccf);
		List<DeclarationExceptionEvent> events = new ArrayList<>();
		admin.setApplicationEventPublisher(new EventPublisher(events));
		admin.setIgnoreDeclarationExceptions(true);
		admin.declareQueue(new AnonymousQueue());
		admin.declareQueue();
		admin.declareExchange(new DirectExchange("foo"));
		admin.declareBinding(new Binding("foo", DestinationType.QUEUE, "bar", "baz", null));
		assertThat(events).hasSize(4);
		assertThat(events.get(0).getSource()).isSameAs(admin);
		assertThat(events.get(0).getDeclarable()).isInstanceOf(AnonymousQueue.class);
		assertThat(events.get(0).getThrowable().getCause()).isSameAs(toBeThrown);
		assertThat(events.get(1).getDeclarable()).isNull();
		assertThat(events.get(1).getThrowable().getCause()).isSameAs(toBeThrown);
		assertThat(events.get(2).getDeclarable()).isInstanceOf(DirectExchange.class);
		assertThat(events.get(2).getThrowable().getCause()).isSameAs(toBeThrown);
		assertThat(events.get(3).getDeclarable()).isInstanceOf(Binding.class);
		assertThat(events.get(3).getThrowable().getCause()).isSameAs(toBeThrown);

		assertThat(admin.getLastDeclarationExceptionEvent()).isSameAs(events.get(3));
	}

	@Test
	public void testWithinInvoke() throws Exception {
		ConnectionFactory connectionFactory = mock();
		Connection connection = mock();
		given(connectionFactory.createConnection()).willReturn(connection);
		Channel channel1 = mock();
		Channel channel2 = mock();
		given(connection.createChannel(false)).willReturn(channel1, channel2);
		DeclareOk declareOk = mock();
		given(channel1.queueDeclare()).willReturn(declareOk);
		given(declareOk.getQueue()).willReturn("foo");
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		RabbitAdmin admin = new RabbitAdmin(template);
		template.invoke(o -> {
			admin.declareQueue();
			admin.declareQueue();
			admin.declareQueue();
			admin.declareQueue();
			return null;
		});
		verify(connection, times(1)).createChannel(false);
		verify(channel1, times(4)).queueDeclare();
		verify(channel1, times(1)).close();
		verifyNoInteractions(channel2);
	}

	@Test
	@SuppressWarnings("removal")
	public void testRetry() throws Exception {
		com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory = mock();
		com.rabbitmq.client.Connection connection = mock();
		given(rabbitConnectionFactory.newConnection((ExecutorService) isNull(), anyString())).willReturn(connection);
		Channel channel = mock();
		given(connection.createChannel()).willReturn(channel);
		given(channel.isOpen()).willReturn(true);
		willThrow(new RuntimeException()).given(channel)
				.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());
		CachingConnectionFactory ccf = new CachingConnectionFactory(rabbitConnectionFactory);
		RabbitAdmin admin = new RabbitAdmin(ccf);
		RetryTemplate rtt = new RetryTemplate();
		rtt.setRetryPolicy(RetryPolicy.builder().maxRetries(2).delay(Duration.ZERO).build());
		admin.setRetryTemplate(rtt);
		GenericApplicationContext ctx = new GenericApplicationContext();
		ctx.getBeanFactory().registerSingleton("foo", new AnonymousQueue());
		ctx.getBeanFactory().registerSingleton("admin", admin);
		admin.setApplicationContext(ctx);
		ctx.getBeanFactory().initializeBean(admin, "admin");
		ctx.refresh();
		assertThatThrownBy(() -> ccf.createConnection())
				.isInstanceOf(UncategorizedAmqpException.class);
		ctx.close();
		verify(channel, times(3)).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());
	}

	@Test
	public void testLeaderLocator() {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitAdmin admin = new RabbitAdmin(cf);
		AnonymousQueue queue = new AnonymousQueue();
		admin.declareQueue(queue);
		AnonymousQueue queue1 = queue;
		Map<String, Object> info = await().until(() -> queueInfo(queue1.getName()), inf -> inf != null);
		assertThat(arguments(info).get(Queue.X_QUEUE_LEADER_LOCATOR)).isEqualTo("client-local");

		queue = new AnonymousQueue();
		queue.setLeaderLocator(null);
		admin.declareQueue(queue);
		AnonymousQueue queue2 = queue;
		info = await().until(() -> queueInfo(queue2.getName()), inf -> inf != null);
		assertThat(arguments(info).get(Queue.X_QUEUE_LEADER_LOCATOR)).isNull();
		cf.destroy();
	}

	@Test
	void manualDeclarations() {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitAdmin admin = new RabbitAdmin(cf);
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		admin.setApplicationContext(applicationContext);
		admin.setRedeclareManualDeclarations(true);
		applicationContext.registerBean("admin", RabbitAdmin.class, () -> admin);
		applicationContext.registerBean("beanQueue", Queue.class,
				() -> new Queue("thisOneShouldntBeInTheManualDecs", false, true, true));
		applicationContext.registerBean("beanEx", DirectExchange.class,
				() -> new DirectExchange("thisOneShouldntBeInTheManualDecs", false, true));
		applicationContext.registerBean("beanBinding", Binding.class,
				() -> new Binding("thisOneShouldntBeInTheManualDecs", DestinationType.QUEUE,
						"thisOneShouldntBeInTheManualDecs", "test", null));
		applicationContext.refresh();
		Set<?> declarables = TestUtils.getPropertyValue(admin, "manualDeclarables", Set.class);
		assertThat(declarables).hasSize(0);
		// check the auto-configured Declarables
		RabbitTemplate template = new RabbitTemplate(cf);
		template.convertAndSend("thisOneShouldntBeInTheManualDecs", "test", "foo");
		Object received = template.receiveAndConvert("thisOneShouldntBeInTheManualDecs", 5000);
		assertThat(received).isEqualTo("foo");
		// manual declarations
		admin.declareExchange(new DirectExchange("test1", false, true));
		admin.declareQueue(new Queue("test1", false, true, true));
		admin.declareQueue(new Queue("test2", false, true, true));
		admin.declareBinding(new Binding("test1", DestinationType.QUEUE, "test1", "test", null));
		admin.deleteQueue("test2");
		template.execute(chan -> {
			chan.queueDelete("test1");
			chan.exchangeDelete("test1");
			return null;
		});
		cf.resetConnection();
		admin.initialize();
		assertThat(admin.getQueueProperties("test1")).isNotNull();
		assertThat(admin.getQueueProperties("test2")).isNull();
		assertThat(declarables).hasSize(3);
		// verify the exchange and binding were recovered too
		template.convertAndSend("test1", "test", "foo");
		received = template.receiveAndConvert("test1", 5000);
		assertThat(received).isEqualTo("foo");
		admin.resetAllManualDeclarations();
		assertThat(declarables).hasSize(0);
		cf.resetConnection();
		admin.initialize();
		assertThat(admin.getQueueProperties("test1")).isNull();
		admin.declareQueue(new Queue("test1", false, true, true));
		admin.declareExchange(new DirectExchange("ex1", false, true));
		admin.declareBinding(new Binding("test1", DestinationType.QUEUE, "ex1", "test", null));
		admin.declareExchange(new DirectExchange("ex2", false, true));
		admin.declareBinding(new Binding("test1", DestinationType.QUEUE, "ex2", "test", null));
		admin.declareBinding(new Binding("ex1", DestinationType.EXCHANGE, "ex2", "ex1", null));
		assertThat(declarables).hasSize(6);
		admin.deleteExchange("ex2");
		assertThat(declarables).hasSize(3);
		admin.deleteQueue("test1");
		assertThat(declarables).hasSize(1);
		admin.deleteExchange("ex1");
		assertThat(declarables).hasSize(0);
		cf.destroy();
	}

	@Test
	void manualDeclarationsWithoutApplicationContext() {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitAdmin admin = new RabbitAdmin(cf);
		admin.setRedeclareManualDeclarations(true);
		Set<?> declarables = TestUtils.getPropertyValue(admin, "manualDeclarables", Set.class);
		assertThat(declarables).hasSize(0);
		RabbitTemplate template = new RabbitTemplate(cf);
		// manual declarations
		admin.declareQueue(new Queue("test1", false, true, true));
		admin.declareQueue(new Queue("test2", false, true, true));
		admin.declareExchange(new DirectExchange("ex1", false, true));
		admin.declareBinding(new Binding("test1", DestinationType.QUEUE, "ex1", "test", null));
		admin.deleteQueue("test2");
		template.execute(chan -> chan.queueDelete("test1"));
		cf.resetConnection();
		admin.initialize();
		assertThat(admin.getQueueProperties("test1")).isNotNull();
		assertThat(admin.getQueueProperties("test2")).isNull();
		assertThat(declarables).hasSize(3);
		// verify the exchange and binding were recovered too
		template.convertAndSend("ex1", "test", "foo");
		Object received = template.receiveAndConvert("test1", 5000);
		assertThat(received).isEqualTo("foo");
		admin.resetAllManualDeclarations();
		assertThat(declarables).hasSize(0);
		cf.resetConnection();
		admin.initialize();
		assertThat(admin.getQueueProperties("test1")).isNull();
		admin.declareQueue(new Queue("test1", false, true, true));
		admin.declareExchange(new DirectExchange("ex1", false, true));
		admin.declareBinding(new Binding("test1", DestinationType.QUEUE, "ex1", "test", null));
		admin.declareExchange(new DirectExchange("ex2", false, true));
		admin.declareBinding(new Binding("test1", DestinationType.QUEUE, "ex2", "test", null));
		admin.declareBinding(new Binding("ex1", DestinationType.EXCHANGE, "ex2", "ex1", null));
		assertThat(declarables).hasSize(6);
		admin.deleteExchange("ex2");
		assertThat(declarables).hasSize(3);
		admin.deleteQueue("test1");
		assertThat(declarables).hasSize(1);
		admin.deleteExchange("ex1");
		assertThat(declarables).hasSize(0);
		cf.destroy();
	}

	@Configuration
	public static class Config {

		public String prototypeQueueName = UUID.randomUUID().toString();

		@Bean
		public ConnectionFactory cf() {
			return new CachingConnectionFactory("localhost");
		}

		@Bean
		public RabbitAdmin admin(ConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

		@Bean
		public RabbitTemplate template(ConnectionFactory cf) {
			return new RabbitTemplate(cf);
		}

		@Bean
		public DirectExchange e1() {
			return new DirectExchange("e1", false, true);
		}

		@Bean
		public Queue q1() {
			return new Queue("q1", false, false, true);
		}

		@Bean
		public Binding b1() {
			return BindingBuilder.bind(q1()).to(e1()).with("k1");
		}

		@Bean
		public Declarables es() {
			return new Declarables(
					new DirectExchange("e2", false, true),
					new DirectExchange("e3", false, true));
		}

		@Bean
		public Declarables qs() {
			return new Declarables(
					new Queue("q2", false, false, true),
					new Queue("q3", false, false, true));
		}

		@Bean
		@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
		public Declarables prototypes() {
			return new Declarables(new Queue(this.prototypeQueueName, false, false, true));
		}

		@Bean
		public Declarables bs() {
			return new Declarables(
					new Binding("q2", DestinationType.QUEUE, "e2", "k2", null),
					new Binding("q3", DestinationType.QUEUE, "e3", "k3", null));
		}

		@Bean
		public Declarables ds() {
			return new Declarables(
					new DirectExchange("e4", false, true),
					new Queue("q4", false, false, true),
					new Binding("q4", DestinationType.QUEUE, "e4", "k4", null));
		}

	}

	private static final class EventPublisher implements ApplicationEventPublisher {

		private final List<DeclarationExceptionEvent> events;

		EventPublisher(List<DeclarationExceptionEvent> events) {
			this.events = events;
		}

		@Override
		public void publishEvent(ApplicationEvent event) {
			events.add((DeclarationExceptionEvent) event);
		}

		@Override
		public void publishEvent(Object event) {

		}

	}

}
