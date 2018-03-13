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

package org.springframework.amqp.rabbit.core;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReceiveAndReplyMessageCallback;
import org.springframework.amqp.core.ReplyToAddressCallback;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelListener;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.GUnzipPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;

/**
 * @author Dave Syer
 * @author Mark Fisher
 * @author Tomas Lukosius
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class RabbitTemplateIntegrationTests {

	private static final Log logger = LogFactory.getLog(RabbitTemplateIntegrationTests.class);

	private static final String ROUTE = "test.queue";

	private static final Queue REPLY_QUEUE = new Queue("test.reply.queue");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE, REPLY_QUEUE.getName());

	@Rule
	public LogLevelAdjuster logAdjuster = new LogLevelAdjuster(Level.DEBUG, RabbitTemplate.class,
			RabbitAdmin.class, RabbitTemplateIntegrationTests.class, BrokerRunning.class);

	@Rule
	public TestName testName = new TestName();

	private CachingConnectionFactory connectionFactory;

	protected RabbitTemplate template;

	@Autowired
	protected RabbitTemplate routingTemplate;

	@Autowired
	private ConnectionFactory cf1;

	@Autowired
	private ConnectionFactory cf2;

	@Before
	public void create() {
		this.connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactory.setPublisherReturns(true);
		this.template = new RabbitTemplate(connectionFactory);
		this.template.setSendConnectionFactorySelectorExpression(new LiteralExpression("foo"));
		BeanFactory bf = mock(BeanFactory.class);
		ConnectionFactory cf = mock(ConnectionFactory.class);
		when(cf.getUsername()).thenReturn("guest");
		when(bf.getBean("cf")).thenReturn(cf);
		this.template.setBeanFactory(bf);
		template.setBeanName(this.testName.getMethodName() + "RabbitTemplate");
	}

	@After
	public void cleanup() throws Exception {
		this.template.stop();
		((DisposableBean) template.getConnectionFactory()).destroy();
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testChannelCloseInTx() throws Exception {
		this.connectionFactory.setPublisherReturns(false);
		Channel channel = this.connectionFactory.createConnection().createChannel(true);
		RabbitResourceHolder holder = new RabbitResourceHolder(channel, true);
		TransactionSynchronizationManager.bindResource(this.connectionFactory, holder);
		try {
			this.template.setChannelTransacted(true);
			this.template.convertAndSend(ROUTE, "foo");
			this.template.convertAndSend(UUID.randomUUID().toString(), ROUTE, "xxx"); // force channel close
			int n = 0;
			while (n++ < 100 && channel.isOpen()) {
				Thread.sleep(100);
			}
			assertFalse(channel.isOpen());
			try {
				this.template.convertAndSend(ROUTE, "bar");
				fail("Expected Exception");
			}
			catch (UncategorizedAmqpException e) {
				if (e.getCause() instanceof IllegalStateException) {
					assertThat(e.getCause().getMessage(), equalTo("Channel closed during transaction"));
				}
				else {
					fail("Expected IllegalStateException not" + e.getCause());
				}
			}
			catch (AmqpConnectException e) {
				assertThat(e.getCause(), instanceOf(AlreadyClosedException.class));
			}
		}
		finally {
			TransactionSynchronizationManager.unbindResource(this.connectionFactory);
		}
		channel.close();
	}

	@Test
	@DirtiesContext
	public void testTemplateUsesPublisherConnectionUnlessInTx() throws Exception {
		this.connectionFactory.destroy();
		this.template.setUsePublisherConnection(true);
		this.template.convertAndSend("dummy", "foo");
		assertNull(TestUtils.getPropertyValue(this.connectionFactory, "connection.target"));
		assertNotNull(TestUtils.getPropertyValue(
				this.connectionFactory, "publisherConnectionFactory.connection.target"));
		this.connectionFactory.destroy();
		assertNull(TestUtils.getPropertyValue(this.connectionFactory, "connection.target"));
		assertNull(TestUtils.getPropertyValue(
				this.connectionFactory, "publisherConnectionFactory.connection.target"));
		Channel channel = this.connectionFactory.createConnection().createChannel(true);
		assertNotNull(TestUtils.getPropertyValue(this.connectionFactory, "connection.target"));
		RabbitResourceHolder holder = new RabbitResourceHolder(channel, true);
		TransactionSynchronizationManager.bindResource(this.connectionFactory, holder);
		try {
			this.template.setChannelTransacted(true);
			this.template.convertAndSend("dummy", "foo");
			assertNotNull(TestUtils.getPropertyValue(this.connectionFactory, "connection.target"));
			assertNull(TestUtils.getPropertyValue(
					this.connectionFactory, "publisherConnectionFactory.connection.target"));
		}
		finally {
			TransactionSynchronizationManager.unbindResource(this.connectionFactory);
		}
		channel.close();
	}

	@Test
	public void testReceiveNonBlocking() throws Exception {
		this.template.convertAndSend(ROUTE, "nonblock");
		int n = 0;
		String out = (String) this.template.receiveAndConvert(ROUTE);
		while (n++ < 100 && out == null) {
			Thread.sleep(100);
			out = (String) this.template.receiveAndConvert(ROUTE);
		}
		assertNotNull(out);
		assertEquals("nonblock", out);
		assertNull(this.template.receive(ROUTE));
	}

	@Test(expected = ConsumerCancelledException.class)
	public void testReceiveConsumerCanceled() throws Exception {
		ConnectionFactory connectionFactory = new SingleConnectionFactory("localhost", BrokerTestUtils.getPort());

		class MockConsumer implements Consumer {

			private final Consumer delegate;

			MockConsumer(Consumer delegate) {
				this.delegate = delegate;
			}

			@Override
			public void handleConsumeOk(String consumerTag) {
				this.delegate.handleConsumeOk(consumerTag);
				try {
					handleCancel(consumerTag);
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			@Override
			public void handleCancelOk(String consumerTag) {
				this.delegate.handleCancelOk(consumerTag);
			}

			@Override
			public void handleCancel(String consumerTag) throws IOException {
				this.delegate.handleCancel(consumerTag);
			}

			@Override
			public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
				this.delegate.handleShutdownSignal(consumerTag, sig);
			}

			@Override
			public void handleRecoverOk(String consumerTag) {
				this.delegate.handleRecoverOk(consumerTag);
			}

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				this.delegate.handleDelivery(consumerTag, envelope, properties, body);
			}

		}

		class MockChannel extends PublisherCallbackChannelImpl {

			MockChannel(Channel delegate) {
				super(delegate);
			}

			@Override
			public String basicConsume(String queue, Consumer callback) throws IOException {
				return super.basicConsume(queue, new MockConsumer(callback));
			}

		}

		Connection connection = spy(connectionFactory.createConnection());
		when(connection.createChannel(anyBoolean())).then(
				invocation -> new MockChannel((Channel) invocation.callRealMethod()));

		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		dfa.setPropertyValue("connection", connection);

		this.template = new RabbitTemplate(connectionFactory);
		this.template.setReceiveTimeout(10000);
		this.template.receive(ROUTE);
	}

	@Test
	public void testReceiveBlocking() throws Exception {
		this.template.setUserIdExpressionString("@cf.username");
		this.template.convertAndSend(ROUTE, "block");
		Message received = this.template.receive(ROUTE, 10000);
		assertNotNull(received);
		assertEquals("block", new String(received.getBody()));
		assertThat(received.getMessageProperties().getReceivedUserId(), equalTo("guest"));
		this.template.setReceiveTimeout(0);
		assertNull(this.template.receive(ROUTE));
	}

	@Test
	public void testReceiveBlockingNoTimeout() throws Exception {
		this.template.convertAndSend(ROUTE, "blockNoTO");
		String out = (String) this.template.receiveAndConvert(ROUTE, -1);
		assertNotNull(out);
		assertEquals("blockNoTO", out);
		this.template.setReceiveTimeout(1); // test the no message after timeout path
		try {
			assertNull(this.template.receive(ROUTE));
		}
		catch (ConsumeOkNotReceivedException e) {
			// we're expecting no result, this could happen, depending on timing.
		}
	}

	@Test
	public void testReceiveBlockingTx() throws Exception {
		this.template.convertAndSend(ROUTE, "blockTX");
		this.template.setChannelTransacted(true);
		this.template.setReceiveTimeout(10000);
		String out = (String) this.template.receiveAndConvert(ROUTE);
		assertNotNull(out);
		assertEquals("blockTX", out);
		this.template.setReceiveTimeout(0);
		assertNull(this.template.receive(ROUTE));
	}

	@Test
	public void testReceiveBlockingGlobalTx() throws Exception {
		template.convertAndSend(ROUTE, "blockGTXNoTO");
		RabbitResourceHolder resourceHolder = ConnectionFactoryUtils
				.getTransactionalResourceHolder(this.template.getConnectionFactory(), true);
		TransactionSynchronizationManager.setActualTransactionActive(true);
		ConnectionFactoryUtils.bindResourceToTransaction(resourceHolder, this.template.getConnectionFactory(), true);
		template.setReceiveTimeout(-1);
		template.setChannelTransacted(true);
		String out = (String) template.receiveAndConvert(ROUTE);
		resourceHolder.commitAll();
		resourceHolder.closeAll();
		assertSame(resourceHolder, TransactionSynchronizationManager.unbindResource(template.getConnectionFactory()));
		assertNotNull(out);
		assertEquals("blockGTXNoTO", out);
		this.template.setReceiveTimeout(0);
		assertNull(this.template.receive(ROUTE));
	}

	@Test
	public void testSendToNonExistentAndThenReceive() throws Exception {
		// If transacted then the commit fails on send, so we get a nice synchronous
		// exception
		template.setChannelTransacted(true);
		try {
			template.convertAndSend("", "no.such.route", "message");
			// fail("Expected AmqpException");
		}
		catch (AmqpException e) {
			// e.printStackTrace();
		}
		// Now send the real message, and all should be well...
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveWithPostProcessor() throws Exception {
		final String[] strings = new String[] { "1", "2" };
		template.convertAndSend(ROUTE, (Object) "message", message -> {
			message.getMessageProperties().setContentType("text/other");
			// message.getMessageProperties().setUserId("foo");
			MessageProperties props = message.getMessageProperties();
			props.getHeaders().put("strings", strings);
			props.getHeaders().put("objects", new Object[] { new Foo(), new Foo() });
			props.getHeaders().put("bytes", "abc".getBytes());
			return message;
		});
		template.setAfterReceivePostProcessors(message -> {
			assertEquals(Arrays.asList(strings), message.getMessageProperties().getHeaders().get("strings"));
			assertEquals(Arrays.asList(new String[] { "FooAsAString", "FooAsAString" }),
					message.getMessageProperties().getHeaders().get("objects"));
			assertArrayEquals("abc".getBytes(), (byte[]) message.getMessageProperties().getHeaders().get("bytes"));
			return message;
		});
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceive() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveUndeliverable() throws Exception {
		this.connectionFactory.setBeanName("testSendAndReceiveUndeliverable");
		template.setMandatory(true);
		try {
			template.convertSendAndReceive(ROUTE + "xxxxxxxx", "undeliverable");
			fail("expected exception");
		}
		catch (AmqpMessageReturnedException e) {
			assertEquals("undeliverable", new String(e.getReturnedMessage().getBody()));
			assertEquals("NO_ROUTE", e.getReplyText());
		}
		assertEquals(0, TestUtils.getPropertyValue(template, "replyHolder", Map.class).size());
	}

	@Test
	public void testSendAndReceiveTransacted() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveTransactedWithUncachedConnection() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
		singleConnectionFactory.destroy();
	}

	@Test
	public void testSendAndReceiveTransactedWithImplicitRollback() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		// Rollback of manual receive is implicit because the channel is
		// closed...
		try {
			template.execute(channel -> {
				// Switch off the auto-ack so the message is rolled back...
				channel.basicGet(ROUTE, false);
				// This is the way to rollback with a cached channel (it is
				// the way the ConnectionFactoryUtils
				// handles it via a synchronization):
				channel.basicRecover(true);
				throw new PlannedException();
			});
			fail("Expected PlannedException");
		}
		catch (Exception e) {
			assertTrue(e.getCause() instanceof PlannedException);
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveInCallback() throws Exception {
		template.convertAndSend(ROUTE, "message");
		final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
		String result = template.execute(channel -> {
			// We need noAck=false here for the message to be explicitly
			// acked
			GetResponse response = channel.basicGet(ROUTE, false);
			MessageProperties messageProps = messagePropertiesConverter.toMessageProperties(response.getProps(),
					response.getEnvelope(), "UTF-8");
			// Explicit ack
			channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
			return (String) new SimpleMessageConverter().fromMessage(new Message(response.getBody(), messageProps));
		});
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInExternalTransaction() throws Exception {
		template.convertAndSend(ROUTE, "message");
		template.setChannelTransacted(true);
		String result = new TransactionTemplate(new TestTransactionManager())
				.execute(status -> (String) template.receiveAndConvert(ROUTE));
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInExternalTransactionAutoAck() throws Exception {
		template.convertAndSend(ROUTE, "message");
		// Should just result in auto-ack (not synched with external tx)
		template.setChannelTransacted(true);
		String result = new TransactionTemplate(new TestTransactionManager())
				.execute(status -> (String) template.receiveAndConvert(ROUTE));
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInExternalTransactionWithRollback() throws Exception {
		// Makes receive (and send in principle) transactional
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		try {
			new TransactionTemplate(new TestTransactionManager()).execute(status -> {
				template.receiveAndConvert(ROUTE);
				throw new PlannedException();
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInExternalTransactionWithNoRollback() throws Exception {
		// Makes receive non-transactional
		template.setChannelTransacted(false);
		template.convertAndSend(ROUTE, "message");
		try {
			new TransactionTemplate(new TestTransactionManager()).execute(status -> {
				template.receiveAndConvert(ROUTE);
				throw new PlannedException();
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		// No rollback
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInExternalTransaction() throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(status -> {
			template.convertAndSend(ROUTE, "message");
			return null;
		});
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInExternalTransactionWithRollback() throws Exception {
		template.setChannelTransacted(true);
		try {
			new TransactionTemplate(new TestTransactionManager()).execute(status -> {
				template.convertAndSend(ROUTE, "message");
				throw new PlannedException();
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testAtomicSendAndReceive() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive();
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertEquals(null, reply);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveUserCorrelation() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		final AtomicReference<String> remoteCorrelationId = new AtomicReference<>();
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(() -> {
			Message message = null;
			message = template.receive(10000);
			assertNotNull("No message received", message);
			remoteCorrelationId.set(message.getMessageProperties().getCorrelationId());
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		RabbitAdmin admin = new RabbitAdmin(cachingConnectionFactory);
		Queue replyQueue = admin.declareQueue();
		template.setReplyAddress(replyQueue.getName());
		template.setUserCorrelationId(true);
		template.setReplyTimeout(10000);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cachingConnectionFactory);
		container.setQueues(replyQueue);
		container.setMessageListener(template);
		container.afterPropertiesSet();
		container.start();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setCorrelationId("myCorrelationId");
		Message message = new Message("test-message".getBytes(), messageProperties);
		Message reply = template.sendAndReceive(message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertThat(remoteCorrelationId.get(), equalTo("myCorrelationId"));
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertEquals(null, reply);
		template.stop();
		container.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveExternalExecutor() throws Exception {
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		final String execName = "make-sure-exec-passed-in";
		exec.setBeanName(execName);
		exec.afterPropertiesSet();
		connectionFactory.setExecutor(exec);
		final Field[] fields = new Field[1];
		ReflectionUtils.doWithFields(RabbitTemplate.class, field -> {
			field.setAccessible(true);
			fields[0] = field;
		}, field -> field.getName().equals("logger"));
		Log logger = Mockito.mock(Log.class);
		when(logger.isTraceEnabled()).thenReturn(true);

		final AtomicBoolean execConfiguredOk = new AtomicBoolean();

		doAnswer(invocation -> {
			String log = invocation.getArgument(0);
			if (log.startsWith("Message received") && Thread.currentThread().getName().startsWith(execName)) {
				execConfiguredOk.set(true);
			}
			return null;
		}).when(logger).trace(anyString());
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(connectionFactory);
		ReflectionUtils.setField(fields[0], template, logger);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive();
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertEquals(null, reply);

		assertTrue(execConfiguredOk.get());
		template.stop();
		connectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithRoutingKey() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive(ROUTE);
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(ROUTE, message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive(ROUTE);
		assertEquals(null, reply);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithExchangeAndRoutingKey() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive(ROUTE);
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive("", ROUTE, message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive(ROUTE);
		assertEquals(null, reply);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversion() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive();
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		String result = (String) template.convertSendAndReceive("message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert();
		assertEquals(null, result);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionUsingRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive(ROUTE);
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		String result = (String) template.convertSendAndReceive(ROUTE, "message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
		template.stop();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionUsingExchangeAndRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive(ROUTE);
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		String result = (String) template.convertSendAndReceive("", ROUTE, "message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
		template.stop();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessor() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive();
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		String result = (String) template.convertSendAndReceive((Object) "message", message -> {
			try {
				byte[] newBody = new String(message.getBody(), "UTF-8").toUpperCase().getBytes("UTF-8");
				return new Message(newBody, message.getMessageProperties());
			}
			catch (Exception e) {
				throw new AmqpException("unexpected failure in test", e);
			}
		});
		assertEquals("MESSAGE", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("MESSAGE", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert();
		assertEquals(null, result);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessorUsingRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive(ROUTE);
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		String result = (String) template.convertSendAndReceive(ROUTE, (Object) "message", message -> {
			try {
				byte[] newBody = new String(message.getBody(), "UTF-8").toUpperCase().getBytes("UTF-8");
				return new Message(newBody, message.getMessageProperties());
			}
			catch (Exception e) {
				throw new AmqpException("unexpected failure in test", e);
			}
		});
		assertEquals("MESSAGE", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("MESSAGE", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
		template.stop();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessorUsingExchangeAndRoutingKey()
			throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(() -> {
			Message message = null;
			for (int i = 0; i < 10; i++) {
				message = template.receive(ROUTE);
				if (message != null) {
					break;
				}
				Thread.sleep(100L);
			}
			assertNotNull("No message received", message);
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		String result = (String) template.convertSendAndReceive("", ROUTE, "message", message -> {
			try {
				byte[] newBody = new String(message.getBody(), "UTF-8").toUpperCase().getBytes("UTF-8");
				return new Message(newBody, message.getMessageProperties());
			}
			catch (Exception e) {
				throw new AmqpException("unexpected failure in test", e);
			}
		});
		assertEquals("MESSAGE", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("MESSAGE", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
		template.stop();
	}

	@Test
	public void testReceiveAndReplyNonStandardCorrelationNotBytes() {
		this.template.setQueue(ROUTE);
		this.template.setRoutingKey(ROUTE);
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.getHeaders().put("baz", "bar");
		Message message = new Message("foo".getBytes(), messageProperties);
		this.template.send(ROUTE, message);

		this.template.setCorrelationKey("baz");
		boolean received = this.template.receiveAndReply(
				message1 -> new Message("fuz".getBytes(), new MessageProperties()));
		assertTrue(received);
		message = this.template.receive();
		assertNotNull(message);
		assertEquals("bar", message.getMessageProperties().getHeaders().get("baz"));
	}

	@Test
	public void testReceiveAndReplyBlocking() throws Exception {
		testReceiveAndReply(10000);
	}

	@Test
	public void testReceiveAndReplyNonBlocking() throws Exception {
		testReceiveAndReply(0);
	}

	private void testReceiveAndReply(long timeout) throws Exception {
		this.template.setQueue(ROUTE);
		this.template.setRoutingKey(ROUTE);
		this.template.convertAndSend(ROUTE, "test");
		template.setReceiveTimeout(timeout);

		boolean received = receiveAndReply();
		int n = 0;
		while (timeout == 0 && !received && n++ < 100) {
			Thread.sleep(100);
			received = receiveAndReply();
		}
		assertTrue(received);

		Message receive = this.template.receive();
		assertNotNull(receive);
		assertEquals("bar", receive.getMessageProperties().getHeaders().get("foo"));

		this.template.convertAndSend(ROUTE, 1);

		received = this.template.receiveAndReply(ROUTE,
				(ReceiveAndReplyCallback<Integer, Integer>) payload -> payload + 1);
		assertTrue(received);

		Object result = this.template.receiveAndConvert(ROUTE);
		assertTrue(result instanceof Integer);
		assertEquals(2, result);

		this.template.convertAndSend(ROUTE, 2);

		received = this.template.receiveAndReply(ROUTE,
				(ReceiveAndReplyCallback<Integer, Integer>) payload -> payload * 2, "", ROUTE);
		assertTrue(received);

		result = this.template.receiveAndConvert(ROUTE);
		assertTrue(result instanceof Integer);
		assertEquals(4, result);

		received = false;
		if (timeout > 0) {
			this.template.setReceiveTimeout(1);
		}
		try {
			received = this.template.receiveAndReply(message -> message);
		}
		catch (ConsumeOkNotReceivedException e) {
			// we're expecting no result, this could happen, depending on timing.
		}
		assertFalse(received);

		this.template.convertAndSend(ROUTE, "test");
		this.template.setReceiveTimeout(timeout);
		received = this.template.receiveAndReply(message -> null);
		assertTrue(received);

		this.template.setReceiveTimeout(0);
		result = this.template.receive();
		assertNull(result);

		this.template.convertAndSend(ROUTE, "TEST");
		this.template.setReceiveTimeout(timeout);
		received = this.template.receiveAndReply((ReceiveAndReplyMessageCallback) message -> {
			MessageProperties messageProperties = new MessageProperties();
			messageProperties.setContentType(message.getMessageProperties().getContentType());
			messageProperties.setHeader("testReplyTo", new Address("", ROUTE));
			return new Message(message.getBody(), messageProperties);
		}, (request, reply) -> (Address) reply.getMessageProperties().getHeaders().get("testReplyTo"));
		assertTrue(received);
		result = this.template.receiveAndConvert(ROUTE);
		assertEquals("TEST", result);

		this.template.setReceiveTimeout(0);
		assertEquals(null, this.template.receive(ROUTE));

		this.template.setChannelTransacted(true);

		this.template.convertAndSend(ROUTE, "TEST");
		this.template.setReceiveTimeout(timeout);
		result = new TransactionTemplate(new TestTransactionManager()).execute(status -> {
			final AtomicReference<String> payloadReference = new AtomicReference<String>();
			boolean received1 = template.receiveAndReply((ReceiveAndReplyCallback<String, Void>) payload -> {
				payloadReference.set(payload);
				return null;
			});
			assertTrue(received1);
			return payloadReference.get();
		});
		assertEquals("TEST", result);
		this.template.setReceiveTimeout(0);
		assertEquals(null, this.template.receive(ROUTE));

		this.template.convertAndSend(ROUTE, "TEST");
		this.template.setReceiveTimeout(timeout);
		try {
			new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallbackWithoutResult() {

				@Override
				public void doInTransactionWithoutResult(TransactionStatus status) {
					template.receiveAndReply((ReceiveAndReplyMessageCallback) message -> message,
							(ReplyToAddressCallback<Message>) (request, reply) -> {
						throw new PlannedException();
					});
				}
			});
			fail("Expected PlannedException");
		}
		catch (Exception e) {
			assertTrue(e.getCause() instanceof PlannedException);
		}

		assertEquals("TEST", this.template.receiveAndConvert(ROUTE));
		this.template.setReceiveTimeout(0);
		assertEquals(null, this.template.receive(ROUTE));

		template.convertAndSend("test");
		this.template.setReceiveTimeout(timeout);
		try {
			this.template.receiveAndReply(new ReceiveAndReplyCallback<Double, Void>() {

				@Override
				public Void handle(Double message) {
					return null;
				}
			});
			fail("IllegalArgumentException expected");
		}
		catch (Exception e) {
			assertTrue(e.getCause() instanceof IllegalArgumentException);
			assertTrue(e.getCause().getCause() instanceof ClassCastException);
		}

	}

	private boolean receiveAndReply() {
		return this.template.receiveAndReply((ReceiveAndReplyMessageCallback) message -> {
			message.getMessageProperties().setHeader("foo", "bar");
			return message;
		});
	}

	@Test
	public void testSymmetricalReceiveAndReply() throws InterruptedException, UnsupportedEncodingException {
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		template.setQueue(ROUTE);
		template.setRoutingKey(ROUTE);
		template.setReplyAddress(REPLY_QUEUE.getName());
		template.setReplyTimeout(20000);
		template.setReceiveTimeout(20000);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setQueues(REPLY_QUEUE);
		container.setMessageListener(template);
		container.start();

		int count = 10;

		final Map<Double, Object> results = new HashMap<Double, Object>();

		ExecutorService executor = Executors.newFixedThreadPool(10);

		template.setCorrelationKey("CorrelationKey");

		for (int i = 0; i < count; i++) {
			executor.execute(() -> {
				Double request = Math.random() * 100;
				Object reply = template.convertSendAndReceive(request);
				results.put(request, reply);
			});
		}

		for (int i = 0; i < count; i++) {
			executor.execute(() -> {
				Double request = Math.random() * 100;
				MessageProperties messageProperties = new MessageProperties();
				messageProperties.setContentType(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT);
				Message reply = template
						.sendAndReceive(new Message(SerializationUtils.serialize(request), messageProperties));
				results.put(request, SerializationUtils.deserialize(reply.getBody()));
			});
		}

		final AtomicInteger receiveCount = new AtomicInteger();

		long start = System.currentTimeMillis();
		do {
			template.receiveAndReply((ReceiveAndReplyCallback<Double, Double>) payload -> {
				receiveCount.incrementAndGet();
				return payload * 3;
			});
			if (System.currentTimeMillis() > start + 10000) {
				fail("Something wrong with RabbitMQ");
			}
		} while (receiveCount.get() < count * 2);

		executor.shutdown();
		assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
		container.stop();

		assertEquals(count * 2, results.size());

		for (Map.Entry<Double, Object> entry : results.entrySet()) {
			assertEquals(entry.getKey() * 3, entry.getValue());
		}

		String messageId = UUID.randomUUID().toString();

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setMessageId(messageId);
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		messageProperties.setReplyTo(REPLY_QUEUE.getName());

		template.send(new Message("test".getBytes(), messageProperties));

		template.receiveAndReply((ReceiveAndReplyCallback<String, String>) String::toUpperCase);

		this.template.setReceiveTimeout(20000);

		Message result = this.template.receive(REPLY_QUEUE.getName());
		assertNotNull(result);
		assertEquals("TEST", new String(result.getBody()));
		assertEquals(messageId, result.getMessageProperties().getCorrelationId());
		template.stop();
	}

	@Test
	public void testSendAndReceiveFastImplicit() {
		sendAndReceiveFastGuts(false, false, false);
	}

	@Test
	public void testSendAndReceiveFastExplicit() {
		sendAndReceiveFastGuts(false, true, false);
	}

	@Test
	public void testSendAndReceiveNeverFast() {
		sendAndReceiveFastGuts(true, false, true);
	}

	@Test
	public void testSendAndReceiveNeverFastWitReplyQueue() {
		sendAndReceiveFastGuts(true, true, false);
	}

	private void sendAndReceiveFastGuts(boolean tempQueue, boolean setDirectReplyToExplicitly, boolean expectUsedTemp) {
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		try {
			template.execute(channel -> {
				channel.queueDeclarePassive(Address.AMQ_RABBITMQ_REPLY_TO);
				return null;
			});
			template.setUseTemporaryReplyQueues(tempQueue);
			if (setDirectReplyToExplicitly) {
				template.setReplyAddress(Address.AMQ_RABBITMQ_REPLY_TO);
			}
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
			container.setConnectionFactory(template.getConnectionFactory());
			container.setQueueNames(ROUTE);
			final AtomicReference<String> replyToWas = new AtomicReference<String>();
			MessageListenerAdapter messageListenerAdapter = new MessageListenerAdapter(new Object() {

				@SuppressWarnings("unused")
				public Message handleMessage(Message message) {
					replyToWas.set(message.getMessageProperties().getReplyTo());
					return new Message(new String(message.getBody()).toUpperCase().getBytes(),
							message.getMessageProperties());
				}
			});
			messageListenerAdapter.setMessageConverter(null);
			container.setMessageListener(messageListenerAdapter);
			container.start();
			template.setQueue(ROUTE);
			template.setRoutingKey(ROUTE);
			Object result = template.convertSendAndReceive("foo");
			container.stop();
			assertEquals("FOO", result);
			if (expectUsedTemp) {
				assertThat(replyToWas.get(), not(startsWith(Address.AMQ_RABBITMQ_REPLY_TO)));
			}
			else {
				assertThat(replyToWas.get(), startsWith(Address.AMQ_RABBITMQ_REPLY_TO));
			}
		}
		catch (Exception e) {
			assertThat(e.getCause().getCause().getMessage(), containsString("404"));
			logger.info("Broker does not support fast replies; test skipped " + e.getMessage());
		}
		finally {
			template.stop();
		}
	}

	@Test
	public void testReplyCompressionWithContainer() {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(
				this.template.getConnectionFactory());
		container.setQueueNames(ROUTE);
		MessageListenerAdapter messageListener = new MessageListenerAdapter(new Object() {

			@SuppressWarnings("unused")
			public String handleMessage(String message) {
				return message.toUpperCase();
			}
		});
		messageListener.setBeforeSendReplyPostProcessors(new GZipPostProcessor());
		container.setMessageListener(messageListener);
		container.setReceiveTimeout(100);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.template.getConnectionFactory());
		try {
			MessageProperties props = new MessageProperties();
			props.setContentType("text/plain");
			Message message = new Message("foo".getBytes(), props);
			Message reply = template.sendAndReceive("", ROUTE, message);
			assertNotNull(reply);
			assertEquals("gzip:UTF-8", reply.getMessageProperties().getContentEncoding());
			GUnzipPostProcessor unzipper = new GUnzipPostProcessor();
			reply = unzipper.postProcessMessage(reply);
			assertEquals("FOO", new String(reply.getBody()));
		}
		finally {
			template.stop();
			container.stop();
		}
	}

	protected RabbitTemplate createSendAndReceiveRabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setUseDirectReplyToContainer(false);
		return template;
	}

	@Test
	public void testDegugLogOnPassiveDeclaration() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		Log logger = spy(TestUtils.getPropertyValue(connectionFactory, "logger", Log.class));
		doReturn(true).when(logger).isDebugEnabled();
		new DirectFieldAccessor(connectionFactory).setPropertyValue("logger", logger);
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		final String queueName = UUID.randomUUID().toString();
		final String exchangeName = UUID.randomUUID().toString();
		try {
			template.execute(channel -> {
				channel.queueDeclarePassive(queueName);
				return null;
			});
			fail("Expected exception");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(AmqpIOException.class));
			assertThat(e.getCause(), instanceOf(IOException.class));
			assertThat(e.getCause().getCause(), instanceOf(ShutdownSignalException.class));
			assertThat(e.getCause().getCause().getMessage(), containsString("404"));
		}
		try {
			template.execute(channel -> {
				channel.exchangeDeclarePassive(exchangeName);
				return null;
			});
			fail("Expected exception");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(AmqpIOException.class));
			assertThat(e.getCause(), instanceOf(IOException.class));
			assertThat(e.getCause().getCause(), instanceOf(ShutdownSignalException.class));
			assertThat(e.getCause().getCause().getMessage(), containsString("404"));
		}
		verify(logger, never()).error(any());
		ArgumentCaptor<Object> logs = ArgumentCaptor.forClass(Object.class);
		verify(logger, atLeast(2)).debug(logs.capture());
		boolean queue = false;
		boolean exchange = false;
		for (Object log : logs.getAllValues()) {
			String logMessage = (String) log;
			queue |= (logMessage.contains(queueName) && logMessage.contains("404"));
			exchange |= (logMessage.contains(queueName) && logMessage.contains("404"));
		}
		assertTrue(queue);
		assertTrue(exchange);
		connectionFactory.destroy();
	}

	@Test
	public void testRouting() throws Exception {
		Connection connection1 = mock(Connection.class);
		when(this.cf1.createConnection()).thenReturn(connection1);
		Channel channel1 = mock(Channel.class);
		when(connection1.createChannel(false)).thenReturn(channel1);
		this.routingTemplate.convertAndSend("exchange", "routingKey", "xyz", message -> {
			message.getMessageProperties().setHeader("cfKey", "foo");
			return message;
		});
		verify(channel1).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class),
				any(byte[].class));

		Connection connection2 = mock(Connection.class);
		when(this.cf2.createConnection()).thenReturn(connection2);
		Channel channel2 = mock(Channel.class);
		when(connection2.createChannel(false)).thenReturn(channel2);
		this.routingTemplate.convertAndSend("exchange", "routingKey", "xyz", message -> {
			message.getMessageProperties().setHeader("cfKey", "bar");
			return message;
		});
		verify(channel1).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class),
				any(byte[].class));
	}

	@Test
	public void testSendInGlobalTransactionCommit() throws Exception {
		testSendInGlobalTransactionGuts(false);

		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		assertNull(template.receive(ROUTE));
	}

	@Test
	public void testSendInGlobalTransactionRollback() throws Exception {
		testSendInGlobalTransactionGuts(true);

		assertNull(template.receive(ROUTE));
	}

	private void testSendInGlobalTransactionGuts(final boolean rollback) throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(status -> {

			template.convertAndSend(ROUTE, "message");

			if (rollback) {
				TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {

					@Override
					public void afterCommit() {
						TransactionSynchronizationUtils
								.triggerAfterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
					}

				});
			}

			return null;
		});
	}

	@Test
	public void testSendToMissingExchange() throws Exception {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		final AtomicReference<ShutdownSignalException> shutdown = new AtomicReference<>();
		this.connectionFactory.addChannelListener(new ChannelListener() {

			@Override
			public void onCreate(Channel channel, boolean transactional) {
			}

			@Override
			public void onShutDown(ShutdownSignalException signal) {
				shutdown.set(signal);
				shutdownLatch.countDown();
			}

		});
		final CountDownLatch connLatch = new CountDownLatch(1);
		this.connectionFactory.addConnectionListener(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
			}

			@Override
			public void onShutDown(ShutdownSignalException signal) {
				shutdown.set(signal);
				connLatch.countDown();
			}

		});
		this.template.convertAndSend(UUID.randomUUID().toString(), "foo", "bar");
		assertTrue(shutdownLatch.await(10, TimeUnit.SECONDS));
		this.template.setChannelTransacted(true);
		try {
			this.template.convertAndSend(UUID.randomUUID().toString(), "foo", "bar");
			fail("expected exception");
		}
		catch (AmqpException e) {
			Method shutdownReason = shutdown.get().getReason();
			assertThat(shutdownReason, instanceOf(AMQP.Channel.Close.class));
			assertThat(((AMQP.Channel.Close) shutdownReason).getReplyCode(), equalTo(AMQP.NOT_FOUND));
		}
		this.connectionFactory.shutdownCompleted(
				new ShutdownSignalException(true, false, new AMQImpl.Connection.Close(
						AMQP.CONNECTION_FORCED, "CONNECTION_FORCED", 10, 0), null));
		assertTrue(connLatch.await(10, TimeUnit.SECONDS));
		Method shutdownReason = shutdown.get().getReason();
		assertThat(shutdownReason, instanceOf(AMQP.Connection.Close.class));
		assertThat(((AMQP.Connection.Close) shutdownReason).getReplyCode(), equalTo(AMQP.CONNECTION_FORCED));
	}

	@Test
	public void testInvoke() {
		this.template.invoke(t -> {
			t.execute(c -> {
				t.execute(channel -> {
					assertSame(c, channel);
					return null;
				});
				return null;
			});
			return null;
		});
		ThreadLocal<?> tl = TestUtils.getPropertyValue(this.template, "dedicatedChannels", ThreadLocal.class);
		assertNull(tl.get());
	}

	@Test
	public void waitForConfirms() {
		this.connectionFactory.setPublisherConfirms(true);
		Collection<?> messages = getMessagesToSend();
		Boolean result = this.template.invoke(t -> {
			messages.forEach(m -> t.convertAndSend(ROUTE, m));
			t.waitForConfirmsOrDie(10_000);
			return true;
		});
		assertTrue(result);
	}

	private Collection<String> getMessagesToSend() {
		return Arrays.asList("foo", "bar");
	}

	@SuppressWarnings("serial")
	private class PlannedException extends RuntimeException {

		PlannedException() {
			super("Planned");
		}
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

	public static class RCFConfig {

		@Bean
		public ConnectionFactory cf1() {
			return mock(ConnectionFactory.class);
		}

		@Bean
		public ConnectionFactory cf2() {
			return mock(ConnectionFactory.class);
		}

		@Bean
		public ConnectionFactory defaultCF() {
			return mock(ConnectionFactory.class);
		}

	}

	private static class Foo {

		Foo() {
			super();
		}

		@Override
		public String toString() {
			return "FooAsAString";
		}

	}

}
