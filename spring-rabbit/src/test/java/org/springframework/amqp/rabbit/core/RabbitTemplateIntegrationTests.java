/*
 * Copyright 2002-2025 the original author or authors.
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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
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
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.ChannelListener;
import org.springframework.amqp.rabbit.connection.ClosingRecoveryListener;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannelImpl;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.GUnzipPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Dave Syer
 * @author Mark Fisher
 * @author Tomas Lukosius
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 */
@SpringJUnitConfig
@RabbitAvailable({ RabbitTemplateIntegrationTests.ROUTE, RabbitTemplateIntegrationTests.REPLY_QUEUE_NAME,
	RabbitTemplateIntegrationTests.NO_CORRELATION })
@LogLevels(classes = { RabbitTemplate.class, DirectMessageListenerContainer.class,
			DirectReplyToMessageListenerContainer.class,
			RabbitAdmin.class, RabbitTemplateIntegrationTests.class, BrokerRunning.class,
			ClosingRecoveryListener.class },
		level = "DEBUG")
@DirtiesContext
public class RabbitTemplateIntegrationTests {

	private static final Log logger = LogFactory.getLog(RabbitTemplateIntegrationTests.class);

	public static final String ROUTE = "test.queue.RabbitTemplateIntegrationTests";

	public static final String REPLY_QUEUE_NAME = "test.reply.queue.RabbitTemplateIntegrationTests";

	public static final Queue REPLY_QUEUE = new Queue(REPLY_QUEUE_NAME);

	public static final String NO_CORRELATION = "no.corr.request";

	private CachingConnectionFactory connectionFactory;

	protected RabbitTemplate template;

	protected String testName;

	@Autowired
	protected RabbitTemplate routingTemplate;

	@Autowired
	private ConnectionFactory cf1;

	@Autowired
	private ConnectionFactory cf2;

	@BeforeEach
	public void create(TestInfo info) {
		this.connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactory.setPublisherReturns(true);
		this.template = new RabbitTemplate(connectionFactory);
		this.template.setSendConnectionFactorySelectorExpression(new LiteralExpression("foo"));
		BeanFactory bf = mock(BeanFactory.class);
		ConnectionFactory cf = mock(ConnectionFactory.class);
		given(cf.getUsername()).willReturn("guest");
		given(bf.getBean("cf")).willReturn(cf);
		this.template.setBeanFactory(bf);
		this.template.setBeanName(info.getDisplayName() + ".RabbitTemplate");
		this.testName = info.getDisplayName();
		this.template.setReplyTimeout(10_000);
	}

	@AfterEach
	public void cleanup() {
		this.template.stop();
		this.connectionFactory.destroy();
		RabbitAvailableCondition.getBrokerRunning().purgeTestQueues();
	}

	@Test
	@LogLevels(classes = RabbitTemplate.class, categories = "foo", level = "DEBUG")
	public void testChannelCloseInTx() throws Exception {
		this.connectionFactory.setPublisherReturns(false);
		Channel channel = this.connectionFactory.createConnection().createChannel(true);
		RabbitResourceHolder holder = new RabbitResourceHolder(channel, true);
		TransactionSynchronizationManager.bindResource(this.connectionFactory, holder);
		try {
			this.template.setChannelTransacted(true);
			this.template.convertAndSend(ROUTE, "foo");
			this.template.convertAndSend(UUID.randomUUID().toString(), ROUTE, "xxx"); // force channel close
			await().until(() -> !channel.isOpen());
			try {
				this.template.convertAndSend(ROUTE, "bar");
				fail("Expected Exception");
			}
			catch (UncategorizedAmqpException e) {
				if (e.getCause() instanceof IllegalStateException) {
					assertThat(e.getCause().getMessage()).isEqualTo("Channel closed during transaction");
				}
				else {
					fail("Expected IllegalStateException not" + e.getCause());
				}
			}
			catch (AmqpConnectException e) {
				assertThat(e.getCause()).isInstanceOf(AlreadyClosedException.class);
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
		assertThat(TestUtils.getPropertyValue(this.connectionFactory, "connection.target")).isNull();
		assertThat(TestUtils.getPropertyValue(
				this.connectionFactory, "publisherConnectionFactory.connection.target")).isNotNull();
		this.connectionFactory.destroy();
		assertThat(TestUtils.getPropertyValue(this.connectionFactory, "connection.target")).isNull();
		assertThat(TestUtils.getPropertyValue(
				this.connectionFactory, "publisherConnectionFactory.connection.target")).isNull();
		Channel channel = this.connectionFactory.createConnection().createChannel(true);
		assertThat(TestUtils.getPropertyValue(this.connectionFactory, "connection.target")).isNotNull();
		RabbitResourceHolder holder = new RabbitResourceHolder(channel, true);
		TransactionSynchronizationManager.bindResource(this.connectionFactory, holder);
		try {
			this.template.setChannelTransacted(true);
			this.template.convertAndSend("dummy", "foo");
			assertThat(TestUtils.getPropertyValue(this.connectionFactory, "connection.target")).isNotNull();
			assertThat(TestUtils.getPropertyValue(
					this.connectionFactory, "publisherConnectionFactory.connection.target")).isNull();
		}
		finally {
			TransactionSynchronizationManager.unbindResource(this.connectionFactory);
		}
		channel.close();
	}

	@Test
	public void testReceiveNonBlocking() throws Exception {
		this.template.convertAndSend(ROUTE, "nonblock");
		String out = await().until(() -> (String) this.template.receiveAndConvert(ROUTE), str -> str != null);
		assertThat(out).isEqualTo("nonblock");
		assertThat(this.template.receive(ROUTE)).isNull();
	}

	@Test
	public void testReceiveConsumerCanceled() {
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

		ExecutorService executorService = Executors.newSingleThreadExecutor();

		class MockChannel extends PublisherCallbackChannelImpl {

			MockChannel(Channel delegate) {
				super(delegate, executorService);
			}

			@Override
			public String basicConsume(String queue, boolean autoAck, Map<String, Object> args, Consumer callback)
					throws IOException {

				return super.basicConsume(queue, autoAck, args, new MockConsumer(callback));
			}

		}

		Connection connection = spy(connectionFactory.createConnection());
		given(connection.createChannel(anyBoolean())).willAnswer(
				invocation -> new MockChannel((Channel) invocation.callRealMethod()));

		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		dfa.setPropertyValue("connection", connection);

		this.template = new RabbitTemplate(connectionFactory);
		this.template.setReceiveTimeout(10000);
		assertThatThrownBy(() -> this.template.receive(ROUTE))
				.isInstanceOf(ConsumerCancelledException.class);
		executorService.shutdown();
	}

	@Test
	public void testReceiveBlocking() throws Exception {
		this.template.setUserIdExpressionString("@cf.username");
		this.template.convertAndSend(ROUTE, "block");
		Message received = this.template.receive(ROUTE, 10000);
		assertThat(received).isNotNull();
		assertThat(new String(received.getBody())).isEqualTo("block");
		assertThat(received.getMessageProperties().getReceivedUserId()).isEqualTo("guest");
		this.template.setReceiveTimeout(0);
		assertThat(this.template.receive(ROUTE)).isNull();
	}

	@Test
	public void testReceiveBlockingNoTimeout() throws Exception {
		this.template.convertAndSend(ROUTE, "blockNoTO");
		String out = (String) this.template.receiveAndConvert(ROUTE, -1);
		assertThat(out).isNotNull();
		assertThat(out).isEqualTo("blockNoTO");
		this.template.setReceiveTimeout(1); // test the no message after timeout path
		try {
			assertThat(this.template.receive(ROUTE)).isNull();
		}
		catch (ConsumeOkNotReceivedException e) {
			// we're expecting no result, this could happen, depending on timing.
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReceiveTimeoutRequeue() {
		try {
			assertThat(this.template.receiveAndConvert(ROUTE, 10)).isNull();
		}
		catch (ConsumeOkNotReceivedException e) {
			// empty - race for consumeOk
		}
		assertThat(TestUtils.getPropertyValue(this.connectionFactory, "cachedChannelsNonTransactional", List.class)
				).hasSize(0);
	}

	@Test
	public void testReceiveBlockingTx() throws Exception {
		this.template.convertAndSend(ROUTE, "blockTX");
		this.template.setChannelTransacted(true);
		this.template.setReceiveTimeout(10000);
		String out = (String) this.template.receiveAndConvert(ROUTE);
		assertThat(out).isNotNull();
		assertThat(out).isEqualTo("blockTX");
		this.template.setReceiveTimeout(0);
		assertThat(this.template.receive(ROUTE)).isNull();
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
		assertThat(TransactionSynchronizationManager.unbindResource(template.getConnectionFactory())).isSameAs(resourceHolder);
		assertThat(out).isNotNull();
		assertThat(out).isEqualTo("blockGTXNoTO");
		this.template.setReceiveTimeout(0);
		assertThat(this.template.receive(ROUTE)).isNull();
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
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
			assertThat(message.getMessageProperties().getHeaders().get("strings")).isEqualTo(Arrays.asList(strings));
			assertThat(message.getMessageProperties().getHeaders().get("objects")).isEqualTo(Arrays.asList(new String[]{"FooAsAString", "FooAsAString"}));
			assertThat((byte[]) message.getMessageProperties().getHeaders().get("bytes")).isEqualTo("abc".getBytes());
			return message;
		});
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testSendAndReceive() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSendAndReceiveUndeliverable() throws Exception {
		this.connectionFactory.setBeanName("testSendAndReceiveUndeliverable");
		template.setMandatory(true);
		try {
			template.convertSendAndReceive(ROUTE + "xxxxxxxx", "undeliverable");
			fail("expected exception");
		}
		catch (AmqpMessageReturnedException e) {
			assertThat(new String(e.getReturnedMessage().getBody())).isEqualTo("undeliverable");
			assertThat(e.getReplyText()).isEqualTo("NO_ROUTE");
		}
		assertThat(TestUtils.getPropertyValue(template, "replyHolder", Map.class)).hasSize(0);
	}

	@Test
	public void testSendAndReceiveTransacted() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testSendAndReceiveTransactedWithUncachedConnection() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
			assertThat(e.getCause() instanceof PlannedException).isTrue();
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testReceiveInExternalTransaction() throws Exception {
		template.convertAndSend(ROUTE, "message");
		template.setChannelTransacted(true);
		String result = new TransactionTemplate(new TestTransactionManager())
				.execute(status -> (String) template.receiveAndConvert(ROUTE));
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testReceiveInExternalTransactionAutoAck() throws Exception {
		template.convertAndSend(ROUTE, "message");
		// Should just result in auto-ack (not synched with external tx)
		template.setChannelTransacted(true);
		String result = new TransactionTemplate(new TestTransactionManager())
				.execute(status -> (String) template.receiveAndConvert(ROUTE));
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testSendInExternalTransaction() throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(status -> {
			template.convertAndSend(ROUTE, "message");
			return null;
		});
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
		assertThat(result).isEqualTo(null);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAtomicSendAndReceive() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setDefaultReceiveQueue(ROUTE);
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(message);
		assertThat(new String(received.get(1000, TimeUnit.MILLISECONDS).getBody())).isEqualTo(new String(message.getBody()));
		assertThat(reply).as("Reply is expected").isNotNull();
		assertThat(new String(reply.getBody())).isEqualTo(new String(message.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertThat(reply).isEqualTo(null);
		assertThat(TestUtils.getPropertyValue(template, "replyHolder", Map.class)).hasSize(0);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAtomicSendAndReceiveUserCorrelation() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setDefaultReceiveQueue(ROUTE);
		final AtomicReference<String> remoteCorrelationId = new AtomicReference<>();
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(() -> {
			Message message = null;
			message = template.receive(10000);
			assertThat(message).as("No message received").isNotNull();
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
		container.setReceiveTimeout(10);
		container.setMessageListener(template);
		container.afterPropertiesSet();
		container.start();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setCorrelationId("myCorrelationId");
		Message message = new Message("test-message".getBytes(), messageProperties);
		Message reply = template.sendAndReceive(message);
		assertThat(received.get(1000, TimeUnit.MILLISECONDS).getBody()).isEqualTo(message.getBody());
		assertThat(reply).as("Reply is expected").isNotNull();
		assertThat(remoteCorrelationId.get()).isEqualTo("myCorrelationId");
		assertThat(reply.getBody()).isEqualTo(message.getBody());
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertThat(reply).isEqualTo(null);
		assertThat(TestUtils.getPropertyValue(template, "replyHolder", Map.class)).hasSize(0);
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
		given(logger.isTraceEnabled()).willReturn(true);

		final AtomicBoolean execConfiguredOk = new AtomicBoolean();

		willAnswer(invocation -> {
			String log = invocation.getArgument(0);
			if (log.startsWith("Message received") && Thread.currentThread().getName().startsWith(execName)) {
				execConfiguredOk.set(true);
			}
			return null;
		}).given(logger).trace(anyString());
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(connectionFactory);
		ReflectionUtils.setField(fields[0], template, logger);
		template.setRoutingKey(ROUTE);
		template.setDefaultReceiveQueue(ROUTE);
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(message);
		assertThat(new String(received.get(1000, TimeUnit.MILLISECONDS).getBody())).isEqualTo(new String(message.getBody()));
		assertThat(reply).as("Reply is expected").isNotNull();
		assertThat(new String(reply.getBody())).isEqualTo(new String(message.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertThat(reply).isEqualTo(null);

		assertThat(execConfiguredOk.get()).isTrue();
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(ROUTE, message);
		assertThat(new String(received.get(1000, TimeUnit.MILLISECONDS).getBody())).isEqualTo(new String(message.getBody()));
		assertThat(reply).as("Reply is expected").isNotNull();
		assertThat(new String(reply.getBody())).isEqualTo(new String(message.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive(ROUTE);
		assertThat(reply).isEqualTo(null);
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return message;
		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive("", ROUTE, message);
		assertThat(new String(received.get(1000, TimeUnit.MILLISECONDS).getBody())).isEqualTo(new String(message.getBody()));
		assertThat(reply).as("Reply is expected").isNotNull();
		assertThat(new String(reply.getBody())).isEqualTo(new String(message.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive(ROUTE);
		assertThat(reply).isEqualTo(null);
		template.stop();
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversion() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setDefaultReceiveQueue(ROUTE);
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		String result = (String) template.convertSendAndReceive("message");
		assertThat(received.get(1000, TimeUnit.MILLISECONDS)).isEqualTo("message");
		assertThat(result).isEqualTo("message");
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert();
		assertThat(result).isEqualTo(null);
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		String result = (String) template.convertSendAndReceive(ROUTE, "message");
		assertThat(received.get(1000, TimeUnit.MILLISECONDS)).isEqualTo("message");
		assertThat(result).isEqualTo("message");
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
			assertThat(message).as("No message received").isNotNull();
			template.send(message.getMessageProperties().getReplyTo(), message);
			return (String) template.getMessageConverter().fromMessage(message);
		});
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		String result = (String) template.convertSendAndReceive("", ROUTE, "message");
		assertThat(received.get(10_000, TimeUnit.MILLISECONDS)).isEqualTo("message");
		assertThat(result).isEqualTo("message");
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
		template.stop();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessor() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		cachingConnectionFactory.setHost("localhost");
		final RabbitTemplate template = createSendAndReceiveRabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setDefaultReceiveQueue(ROUTE);
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
			assertThat(message).as("No message received").isNotNull();
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
		assertThat(received.get(10_000, TimeUnit.MILLISECONDS)).isEqualTo("MESSAGE");
		assertThat(result).isEqualTo("MESSAGE");
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert();
		assertThat(result).isEqualTo(null);
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
			assertThat(message).as("No message received").isNotNull();
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
		assertThat(received.get(10_000, TimeUnit.MILLISECONDS)).isEqualTo("MESSAGE");
		assertThat(result).isEqualTo("MESSAGE");
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
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
			assertThat(message).as("No message received").isNotNull();
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
		assertThat(received.get(10_000, TimeUnit.MILLISECONDS)).isEqualTo("MESSAGE");
		assertThat(result).isEqualTo("MESSAGE");
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
		template.stop();
	}

	@Test
	public void testReceiveAndReplyNonStandardCorrelationNotBytes() {
		this.template.setDefaultReceiveQueue(ROUTE);
		this.template.setRoutingKey(ROUTE);
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.getHeaders().put("baz", "bar");
		Message message = new Message("foo".getBytes(), messageProperties);
		this.template.send(ROUTE, message);

		this.template.setCorrelationKey("baz");
		boolean received = this.template.receiveAndReply(
				message1 -> new Message("fuz".getBytes(), new MessageProperties()));
		assertThat(received).isTrue();
		message = this.template.receive();
		assertThat(message).isNotNull();
		assertThat(message.getMessageProperties().getHeaders().get("baz")).isEqualTo("bar");
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
		this.template.setDefaultReceiveQueue(ROUTE);
		this.template.setRoutingKey(ROUTE);
		this.template.convertAndSend(ROUTE, "test");
		template.setReceiveTimeout(timeout);

		boolean received = await().until(() -> receiveAndReply(), b -> b);

		Message receive = this.template.receive();
		assertThat(receive).isNotNull();
		assertThat(receive.getMessageProperties().getHeaders().get("foo")).isEqualTo("bar");

		this.template.convertAndSend(ROUTE, 1);

		received = this.template.receiveAndReply(ROUTE,
				(ReceiveAndReplyCallback<Integer, Integer>) payload -> payload + 1);
		assertThat(received).isTrue();

		Object result = this.template.receiveAndConvert(ROUTE);
		assertThat(result instanceof Integer).isTrue();
		assertThat(result).isEqualTo(2);

		this.template.convertAndSend(ROUTE, 2);

		received = this.template.receiveAndReply(ROUTE,
				(ReceiveAndReplyCallback<Integer, Integer>) payload -> payload * 2, "", ROUTE);
		assertThat(received).isTrue();

		result = this.template.receiveAndConvert(ROUTE);
		assertThat(result instanceof Integer).isTrue();
		assertThat(result).isEqualTo(4);

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
		assertThat(received).isFalse();

		this.template.convertAndSend(ROUTE, "test");
		this.template.setReceiveTimeout(timeout);
		received = this.template.receiveAndReply(message -> null);
		assertThat(received).isTrue();

		this.template.setReceiveTimeout(0);
		result = this.template.receive();
		assertThat(result).isNull();

		this.template.convertAndSend(ROUTE, "TEST");
		this.template.setReceiveTimeout(timeout);
		received = this.template.receiveAndReply((ReceiveAndReplyMessageCallback) message -> {
			MessageProperties messageProperties = new MessageProperties();
			messageProperties.setContentType(message.getMessageProperties().getContentType());
			messageProperties.setHeader("testReplyTo", new Address("", ROUTE));
			return new Message(message.getBody(), messageProperties);
		}, (request, reply) -> (Address) reply.getMessageProperties().getHeaders().get("testReplyTo"));
		assertThat(received).isTrue();
		result = this.template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("TEST");

		this.template.setReceiveTimeout(0);
		assertThat(this.template.receive(ROUTE)).isEqualTo(null);

		this.template.setChannelTransacted(true);

		this.template.convertAndSend(ROUTE, "TEST");
		this.template.setReceiveTimeout(timeout);
		result = new TransactionTemplate(new TestTransactionManager()).execute(status -> {
			final AtomicReference<String> payloadReference = new AtomicReference<String>();
			boolean received1 = template.receiveAndReply((ReceiveAndReplyCallback<String, Void>) payload -> {
				payloadReference.set(payload);
				return null;
			});
			assertThat(received1).isTrue();
			return payloadReference.get();
		});
		assertThat(result).isEqualTo("TEST");
		this.template.setReceiveTimeout(0);
		assertThat(this.template.receive(ROUTE)).isEqualTo(null);

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
			assertThat(e.getCause() instanceof PlannedException).isTrue();
		}

		assertThat(this.template.receiveAndConvert(ROUTE)).isEqualTo("TEST");
		this.template.setReceiveTimeout(0);
		assertThat(this.template.receive(ROUTE)).isEqualTo(null);

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
			assertThat(e.getCause() instanceof IllegalArgumentException).isTrue();
			assertThat(e.getCause().getCause() instanceof ClassCastException).isTrue();
		}

	}

	private boolean receiveAndReply() {
		return this.template.receiveAndReply((ReceiveAndReplyMessageCallback) message -> {
			message.getMessageProperties().setHeader("foo", "bar");
			return message;
		});
	}

	@Test
	public void testSymmetricalReceiveAndReply() throws InterruptedException {
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.connectionFactory);
		template.setDefaultReceiveQueue(ROUTE);
		template.setRoutingKey(ROUTE);
		template.setReplyAddress(REPLY_QUEUE.getName());
		template.setReplyTimeout(20000);
		template.setReceiveTimeout(20000);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setQueues(REPLY_QUEUE);
		container.setReceiveTimeout(10);
		container.setMessageListener(template);
		container.start();

		int count = 10;

		final Map<Double, Object> results = new ConcurrentHashMap<>();

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
		assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
		container.stop();

		assertThat(results).hasSize(count * 2);

		for (Map.Entry<Double, Object> entry : results.entrySet()) {
			assertThat(entry.getValue()).isEqualTo(entry.getKey() * 3);
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
		assertThat(result).isNotNull();
		assertThat(new String(result.getBody())).isEqualTo("TEST");
		assertThat(result.getMessageProperties().getCorrelationId()).isEqualTo(messageId);
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

	@SuppressWarnings("unchecked")
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
			container.setReceiveTimeout(10);
			final AtomicReference<String> replyToWas = new AtomicReference<>();
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
			template.setDefaultReceiveQueue(ROUTE);
			template.setRoutingKey(ROUTE);
			Object result = template.convertSendAndReceive("foo");
			container.stop();
			assertThat(result).isEqualTo("FOO");
			if (expectUsedTemp) {
				assertThat(replyToWas.get()).doesNotStartWith(Address.AMQ_RABBITMQ_REPLY_TO);
			}
			else {
				assertThat(replyToWas.get()).startsWith(Address.AMQ_RABBITMQ_REPLY_TO);
			}
			assertThat(TestUtils.getPropertyValue(template, "replyHolder", Map.class)).hasSize(0);
		}
		catch (Exception e) {
			assertThat(e.getCause().getCause().getMessage()).contains("404");
			logger.info("Broker does not support fast replies; test skipped " + e.getMessage());
		}
		finally {
			template.stop();
		}
	}

	@SuppressWarnings("unchecked")
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
		container.setReceiveTimeout(10);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = createSendAndReceiveRabbitTemplate(this.template.getConnectionFactory());
		try {
			MessageProperties props = new MessageProperties();
			props.setContentType("text/plain");
			Message message = new Message("foo".getBytes(), props);
			Message reply = template.sendAndReceive("", ROUTE, message);
			assertThat(reply).isNotNull();
			assertThat(reply.getMessageProperties().getContentEncoding()).isEqualTo("gzip, UTF-8");
			GUnzipPostProcessor unzipper = new GUnzipPostProcessor();
			reply = unzipper.postProcessMessage(reply);
			assertThat(new String(reply.getBody())).isEqualTo("FOO");
			assertThat(TestUtils.getPropertyValue(template, "replyHolder", Map.class)).hasSize(0);
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
	public void testDebugLogOnPassiveDeclaration() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		Log logger = spy(TestUtils.getPropertyValue(connectionFactory, "logger", Log.class));
		willReturn(true).given(logger).isDebugEnabled();
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
			assertThat(e).isInstanceOf(AmqpIOException.class);
			assertThat(e.getCause()).isInstanceOf(IOException.class);
			assertThat(e.getCause().getCause()).isInstanceOf(ShutdownSignalException.class);
			assertThat(e.getCause().getCause().getMessage()).contains("404");
		}
		try {
			template.execute(channel -> {
				channel.exchangeDeclarePassive(exchangeName);
				return null;
			});
			fail("Expected exception");
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(AmqpIOException.class);
			assertThat(e.getCause()).isInstanceOf(IOException.class);
			assertThat(e.getCause().getCause()).isInstanceOf(ShutdownSignalException.class);
			assertThat(e.getCause().getCause().getMessage()).contains("404");
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
		assertThat(queue).isTrue();
		assertThat(exchange).isTrue();
		connectionFactory.destroy();
	}

	@Test
	public void testRouting() throws Exception {
		Connection connection1 = mock(Connection.class);
		given(this.cf1.createConnection()).willReturn(connection1);
		Channel channel1 = mock(Channel.class);
		given(connection1.createChannel(false)).willReturn(channel1);
		this.routingTemplate.convertAndSend("exchange", "routingKey", "xyz", message -> {
			message.getMessageProperties().setHeader("cfKey", "foo");
			return message;
		});
		verify(channel1).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class),
				any(byte[].class));

		Connection connection2 = mock(Connection.class);
		given(this.cf2.createConnection()).willReturn(connection2);
		Channel channel2 = mock(Channel.class);
		given(connection2.createChannel(false)).willReturn(channel2);
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
		assertThat(result).isEqualTo("message");
		assertThat(template.receive(ROUTE)).isNull();
	}

	@Test
	public void testSendInGlobalTransactionRollback() throws Exception {
		testSendInGlobalTransactionGuts(true);

		assertThat(template.receive(ROUTE)).isNull();
	}

	private void testSendInGlobalTransactionGuts(final boolean rollback) throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(status -> {

			template.convertAndSend(ROUTE, "message");

			if (rollback) {
				TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

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
		assertThat(shutdownLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.template.setChannelTransacted(true);
		try {
			this.template.convertAndSend(UUID.randomUUID().toString(), "foo", "bar");
			fail("expected exception");
		}
		catch (AmqpException e) {
			Method shutdownReason = shutdown.get().getReason();
			assertThat(shutdownReason).isInstanceOf(AMQP.Channel.Close.class);
			assertThat(((AMQP.Channel.Close) shutdownReason).getReplyCode()).isEqualTo(AMQP.NOT_FOUND);
		}
		this.connectionFactory.shutdownCompleted(
				new ShutdownSignalException(true, false, new AMQImpl.Connection.Close(
						AMQP.CONNECTION_FORCED, "CONNECTION_FORCED", 10, 0), null));
		assertThat(connLatch.await(10, TimeUnit.SECONDS)).isTrue();
		Method shutdownReason = shutdown.get().getReason();
		assertThat(shutdownReason).isInstanceOf(AMQP.Connection.Close.class);
		assertThat(((AMQP.Connection.Close) shutdownReason).getReplyCode()).isEqualTo(AMQP.CONNECTION_FORCED);
	}

	@Test
	public void testInvoke() {
		this.template.invoke(t -> {
			t.execute(c -> {
				t.execute(channel -> {
					assertThat(channel).isSameAs(c);
					return null;
				});
				return null;
			});
			return null;
		});
		ThreadLocal<?> tl = TestUtils.getPropertyValue(this.template, "dedicatedChannels", ThreadLocal.class);
		assertThat(tl.get()).isNull();
	}

	@Test
	public void waitForConfirms() {
		this.connectionFactory.setPublisherConfirmType(ConfirmType.CORRELATED);
		Collection<?> messages = getMessagesToSend();
		Boolean result = this.template.invoke(t -> {
			messages.forEach(m -> t.convertAndSend(ROUTE, m));
			t.waitForConfirmsOrDie(10_000);
			return true;
		});
		assertThat(result).isTrue();
	}

	@Test
	@Disabled("Not an automated test - requires broker restart")
	public void testReceiveNoAutoRecovery() throws Exception {
		CachingConnectionFactory ccf = new CachingConnectionFactory("localhost");
		ccf.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);
		RabbitAdmin admin = new RabbitAdmin(ccf);
		ExecutorService exec = Executors.newSingleThreadExecutor();
		final RabbitTemplate template = new RabbitTemplate(ccf);
		template.setReceiveTimeout(30_000);
		exec.execute(() -> {
			while (true) {
				try {
					Thread.sleep(2000);
					template.receive(ROUTE);
				}
				catch (AmqpException e) {
					e.printStackTrace();
					if (e.getCause() != null
						&& e.getCause().getClass().equals(InterruptedException.class)) {
						Thread.currentThread().interrupt();
						return;
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		});
		System .out .println("Wait for consumer; then bounce broker; then enter after it's back up");
		System.in.read();
		for (int i = 0; i < 20; i++) {
			Properties queueProperties = admin.getQueueProperties(ROUTE);
			System .out .println(queueProperties);
			Thread.sleep(1000);
		}
		exec.shutdownNow();
		ccf.destroy();
	}

	@Test
	void directReplyToNoCorrelation() throws InterruptedException {
		ConnectionFactory cf = new CachingConnectionFactory("localhost");
		SimpleAsyncTaskExecutor exec = new SimpleAsyncTaskExecutor();
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setUseChannelForCorrelation(true);
		BlockingQueue<Object> q = new LinkedBlockingQueue<>();
		exec.execute(() -> {
			Object reply = template.convertSendAndReceive(NO_CORRELATION, "test");
			if (reply != null) {
				q.add(reply);
			}
		});
		Message received = template.receive(NO_CORRELATION, 10_000);
		assertThat(received).isNotNull();
		String replyTo = received.getMessageProperties().getReplyTo();
		template.convertAndSend(received.getMessageProperties().getReplyTo(), "TEST");
		assertThat(q.poll(10, TimeUnit.SECONDS)).isEqualTo("TEST");
		exec.execute(() -> {
			Object reply = template.convertSendAndReceive(NO_CORRELATION, "test");
			if (reply != null) {
				q.add(reply);
			}
		});
		received = template.receive(NO_CORRELATION, 10_000);
		assertThat(received).isNotNull();
		template.convertAndSend(received.getMessageProperties().getReplyTo(), "TEST");
		assertThat(q.poll(10, TimeUnit.SECONDS)).isEqualTo("TEST");
		assertThat(received.getMessageProperties().getReplyTo()).isEqualTo(replyTo);
	}

	@Test
	void directReplyToNoCorrelationNoReply() throws InterruptedException {
		ConnectionFactory cf = new CachingConnectionFactory("localhost");
		SimpleAsyncTaskExecutor exec = new SimpleAsyncTaskExecutor();
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setReplyTimeout(100);
		template.setUseChannelForCorrelation(true);
		CountDownLatch latch = new CountDownLatch(1);
		exec.execute(() -> {
			Object reply = template.convertSendAndReceive(NO_CORRELATION, "test");
			latch.countDown();
		});
		Message received = template.receive(NO_CORRELATION, 10_000);
		assertThat(received).isNotNull();
		String replyTo = received.getMessageProperties().getReplyTo();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		BlockingQueue<Object> q = new LinkedBlockingQueue<>();
		template.setReplyTimeout(10000);
		exec.execute(() -> {
			Object reply = template.convertSendAndReceive(NO_CORRELATION, "test");
			if (reply != null) {
				q.add(reply);
			}
		});
		received = template.receive(NO_CORRELATION, 10_000);
		assertThat(received).isNotNull();
		template.convertAndSend(received.getMessageProperties().getReplyTo(), "TEST");
		assertThat(q.poll(10, TimeUnit.SECONDS)).isEqualTo("TEST");
		assertThat(received.getMessageProperties().getReplyTo()).isNotEqualTo(replyTo);
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
	private final class TestTransactionManager extends AbstractPlatformTransactionManager {

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
		}

		@Override
		public String toString() {
			return "FooAsAString";
		}

	}

}
