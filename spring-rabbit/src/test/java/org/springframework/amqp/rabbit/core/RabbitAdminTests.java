/*
 * Copyright 2002-2018 the original author or authors.
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
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

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Artem Yakshin
 *
 * @since 1.4.1
 *
 */
public class RabbitAdminTests {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Test
	public void testSettingOfNullConnectionFactory() {
		ConnectionFactory connectionFactory = null;
		try {
			new RabbitAdmin(connectionFactory);
			fail("should have thrown IllegalArgumentException when ConnectionFactory is null.");
		}
		catch (IllegalArgumentException e) {
			assertEquals("ConnectionFactory must not be null", e.getMessage());
		}
	}

	@Test
	public void testNoFailOnStartupWithMissingBroker() throws Exception {
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
	public void testFailOnFirstUseWithMissingBroker() throws Exception {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("localhost");
		connectionFactory.setPort(434343);
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo", new Queue("queue"));
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(applicationContext);
		rabbitAdmin.setAutoStartup(true);
		rabbitAdmin.afterPropertiesSet();
		exception.expect(IllegalArgumentException.class);
		rabbitAdmin.declareQueue();
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
			int n = 0;
			while (n++ < 100 && messageCount(rabbitAdmin, queueName) == 0) {
				Thread.sleep(100);
			}
			assertTrue("Message count = 0", n < 100);
			Channel channel = connectionFactory.createConnection().createChannel(false);
			DefaultConsumer consumer = new DefaultConsumer(channel);
			channel.basicConsume(queueName, true, consumer);
			n = 0;
			while (n++ < 100 && messageCount(rabbitAdmin, queueName) > 0) {
				Thread.sleep(100);
			}
			assertTrue("Message count > 0", n < 100);
			Properties props = rabbitAdmin.getQueueProperties(queueName);
			assertNotNull(props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT));
			assertEquals(1, props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT));
			channel.close();
		}
		finally {
			rabbitAdmin.deleteQueue(queueName);
			connectionFactory.destroy();
		}
	}

	private int messageCount(RabbitAdmin rabbitAdmin, String queueName) {
		Properties props = rabbitAdmin.getQueueProperties(queueName);
		assertNotNull(props);
		assertNotNull(props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT));
		return (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
	}

	@Test
	public void testTemporaryLogs() throws Exception {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory();
		connectionFactory.setHost("localhost");
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		try {
			ApplicationContext ctx = mock(ApplicationContext.class);
			Map<String, Queue> queues = new HashMap<String, Queue>();
			queues.put("nonDurQ", new Queue("testq.nonDur", false, false, false));
			queues.put("adQ", new Queue("testq.ad", true, false, true));
			queues.put("exclQ", new Queue("testq.excl", true, true, false));
			queues.put("allQ", new Queue("testq.all", false, true, true));
			when(ctx.getBeansOfType(Queue.class)).thenReturn(queues);
			Map<String, Exchange> exchanges = new HashMap<String, Exchange>();
			exchanges.put("nonDurEx", new DirectExchange("testex.nonDur", false, false));
			exchanges.put("adEx", new DirectExchange("testex.ad", true, true));
			exchanges.put("allEx", new DirectExchange("testex.all", false, true));
			when(ctx.getBeansOfType(Exchange.class)).thenReturn(exchanges);
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
			assertThat(logs.get(0), containsString("(testex.ad) durable:true, auto-delete:true"));
			assertThat(logs.get(1), containsString("(testex.all) durable:false, auto-delete:true"));
			assertThat(logs.get(2), containsString("(testex.nonDur) durable:false, auto-delete:false"));
			assertThat(logs.get(3), containsString("(testq.ad) durable:true, auto-delete:true, exclusive:false"));
			assertThat(logs.get(4), containsString("(testq.all) durable:false, auto-delete:true, exclusive:true"));
			assertThat(logs.get(5), containsString("(testq.excl) durable:true, auto-delete:false, exclusive:true"));
			assertThat(logs.get(6), containsString("(testq.nonDur) durable:false, auto-delete:false, exclusive:false"));
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
	public void testMultiEntities() {
		ConfigurableApplicationContext ctx = new AnnotationConfigApplicationContext(Config.class);
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		template.convertAndSend("e1", "k1", "foo");
		template.convertAndSend("e2", "k2", "bar");
		template.convertAndSend("e3", "k3", "baz");
		template.convertAndSend("e4", "k4", "qux");
		assertEquals("foo", template.receiveAndConvert("q1"));
		assertEquals("bar", template.receiveAndConvert("q2"));
		assertEquals("baz", template.receiveAndConvert("q3"));
		assertEquals("qux", template.receiveAndConvert("q4"));
		RabbitAdmin admin = ctx.getBean(RabbitAdmin.class);
		admin.deleteQueue("q1");
		admin.deleteQueue("q2");
		admin.deleteQueue("q3");
		admin.deleteQueue("q4");
		admin.deleteExchange("e1");
		admin.deleteExchange("e2");
		admin.deleteExchange("e3");
		admin.deleteExchange("e4");
		assertNull(admin.getQueueProperties(ctx.getBean(Config.class).prototypeQueueName));
		ctx.close();
	}

	@Test
	public void testAvoidHangAMQP_508() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		RabbitAdmin admin = new RabbitAdmin(cf);
		String longName = new String(new byte[300]).replace('\u0000', 'x');
		try {
			admin.declareQueue(new Queue(longName));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		String goodName = "foobar";
		admin.declareQueue(new Queue(goodName));
		assertNull(admin.getQueueProperties(longName));
		assertNotNull(admin.getQueueProperties(goodName));
		admin.deleteQueue(goodName);
		cf.destroy();
	}

	@Test
	public void testIgnoreDeclarationExceptionsTimeout() throws Exception {
		com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory = mock(
				com.rabbitmq.client.ConnectionFactory.class);
		TimeoutException toBeThrown = new TimeoutException("test");
		willThrow(toBeThrown).given(rabbitConnectionFactory).newConnection(any(ExecutorService.class), anyString());
		CachingConnectionFactory ccf = new CachingConnectionFactory(rabbitConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		RabbitAdmin admin = new RabbitAdmin(ccf);
		List<DeclarationExceptionEvent> events = new ArrayList<DeclarationExceptionEvent>();
		admin.setApplicationEventPublisher(new EventPublisher(events));
		admin.setIgnoreDeclarationExceptions(true);
		admin.declareQueue(new AnonymousQueue());
		admin.declareQueue();
		admin.declareExchange(new DirectExchange("foo"));
		admin.declareBinding(new Binding("foo", DestinationType.QUEUE, "bar", "baz", null));
		assertThat(events.size(), equalTo(4));
		assertThat(events.get(0).getSource(), sameInstance(admin));
		assertThat(events.get(0).getDeclarable(), instanceOf(AnonymousQueue.class));
		assertSame(toBeThrown, events.get(0).getThrowable().getCause());
		assertNull(events.get(1).getDeclarable());
		assertSame(toBeThrown, events.get(1).getThrowable().getCause());
		assertThat(events.get(2).getDeclarable(), instanceOf(DirectExchange.class));
		assertSame(toBeThrown, events.get(2).getThrowable().getCause());
		assertThat(events.get(3).getDeclarable(), instanceOf(Binding.class));
		assertSame(toBeThrown, events.get(3).getThrowable().getCause());

		assertSame(events.get(3), admin.getLastDeclarationExceptionEvent());
	}

	@Test
	public void testWithinInvoke() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		Channel channel1 = mock(Channel.class);
		Channel channel2 = mock(Channel.class);
		given(connection.createChannel(false)).willReturn(channel1, channel2);
		DeclareOk declareOk = mock(DeclareOk.class);
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
		verifyZeroInteractions(channel2);
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
		public List<Exchange> es() {
			return Arrays.<Exchange>asList(
					new DirectExchange("e2", false, true),
					new DirectExchange("e3", false, true)
			);
		}

		@Bean
		public List<Queue> qs() {
			return Arrays.asList(
					new Queue("q2", false, false, true),
					new Queue("q3", false, false, true)
			);
		}

		@Bean
		@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
		public List<Queue> prototypes() {
			return Arrays.asList(
					new Queue(this.prototypeQueueName, false, false, true)
			);
		}

		@Bean
		public List<Binding> bs() {
			return Arrays.asList(
					new Binding("q2", DestinationType.QUEUE, "e2", "k2", null),
					new Binding("q3", DestinationType.QUEUE, "e3", "k3", null)
			);
		}

		@Bean
		public List<Declarable> ds() {
			return Arrays.<Declarable>asList(
					new DirectExchange("e4", false, true),
					new Queue("q4", false, false, true),
					new Binding("q4", DestinationType.QUEUE, "e4", "k4", null)
			);
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
