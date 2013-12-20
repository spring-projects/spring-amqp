/*
 * Copyright 2010-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.core.ReceiveAndReplyMessageCallback;
import org.springframework.amqp.core.ReplyToAddressCallback;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;
import org.springframework.util.ReflectionUtils.FieldFilter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

/**
 * @author Dave Syer
 * @author Mark Fisher
 * @author Tomas Lukosius
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 */
public class RabbitTemplateIntegrationTests {

	private static final String ROUTE = "test.queue";

	private RabbitTemplate template;

	@Before
	public void create() {
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template = new RabbitTemplate(connectionFactory);
	}

	@After
	public void cleanup() throws Exception {
		((DisposableBean) template.getConnectionFactory()).destroy();
	}

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Test
	public void testSendToNonExistentAndThenReceive() throws Exception {
		// If transacted then the commit fails on send, so we get a nice synchronous exception
		template.setChannelTransacted(true);
		try {
			template.convertAndSend("", "no.such.route", "message");
			// fail("Expected AmqpException");
		} catch (AmqpException e) {
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
		template.convertAndSend(ROUTE, (Object)"message", new MessagePostProcessor() {
			public Message postProcessMessage(Message message) throws AmqpException {
				message.getMessageProperties().setContentType("text/other");
				// message.getMessageProperties().setUserId("foo");
				return message;
			}
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
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory();
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
			template.execute(new ChannelCallback<String>() {
				public String doInRabbit(Channel channel) throws Exception {
					// Switch off the auto-ack so the message is rolled back...
					channel.basicGet(ROUTE, false);
					// This is the way to rollback with a cached channel (it is
					// the way the ConnectionFactoryUtils
					// handles it via a synchronization):
					channel.basicRecover(true);
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		} catch (Exception e) {
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
		String result = template.execute(new ChannelCallback<String>() {
			public String doInRabbit(Channel channel) throws Exception {
				// We need noAck=false here for the message to be expicitly
				// acked
				GetResponse response = channel.basicGet(ROUTE, false);
				MessageProperties messageProps = messagePropertiesConverter.toMessageProperties(
						response.getProps(), response.getEnvelope(), "UTF-8");
				// Explicit ack
				channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
				return (String) new SimpleMessageConverter().fromMessage(new Message(response.getBody(), messageProps));
			}
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
				.execute(new TransactionCallback<String>() {
					public String doInTransaction(TransactionStatus status) {
						return (String) template.receiveAndConvert(ROUTE);
					}
				});
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
				.execute(new TransactionCallback<String>() {
					public String doInTransaction(TransactionStatus status) {
						return (String) template.receiveAndConvert(ROUTE);
					}
				});
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
			new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<String>() {
				public String doInTransaction(TransactionStatus status) {
					template.receiveAndConvert(ROUTE);
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		} catch (PlannedException e) {
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
			new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<String>() {
				public String doInTransaction(TransactionStatus status) {
					template.receiveAndConvert(ROUTE);
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		} catch (PlannedException e) {
			// Expected
		}
		// No rollback
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInExternalTransaction() throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<Void>() {
			public Void doInTransaction(TransactionStatus status) {
				template.convertAndSend(ROUTE, "message");
				return null;
			}
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
			new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<Void>() {
				public Void doInTransaction(TransactionStatus status) {
					template.convertAndSend(ROUTE, "message");
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		} catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testAtomicSendAndReceive() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		final RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(new Callable<Message>() {

			public Message call() throws Exception {
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
			}

		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive();
		assertEquals(null, reply);
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveExternalExecutor() throws Exception {
		final CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		final String execName = "make-sure-exec-passed-in";
		exec.setBeanName(execName);
		exec.afterPropertiesSet();
		connectionFactory.setExecutor(exec);
		final Field[] fields = new Field[1];
		ReflectionUtils.doWithFields(RabbitTemplate.class, new FieldCallback() {
			public void doWith(Field field) throws IllegalArgumentException,
					IllegalAccessException {
				field.setAccessible(true);
				fields[0] = field;
			}
		}, new FieldFilter() {
			public boolean matches(Field field) {
				return field.getName().equals("logger");
			}
		});
		Log logger = Mockito.mock(Log.class);
		when(logger.isTraceEnabled()).thenReturn(true);

		final AtomicBoolean execConfiguredOk = new AtomicBoolean();

		doAnswer(new Answer<Object>(){
			public Object answer(InvocationOnMock invocation) throws Throwable {
				String log = (String) invocation.getArguments()[0];
				if (log.startsWith("Message received") &&
						Thread.currentThread().getName().startsWith(execName)) {
					execConfiguredOk.set(true);
				}
				return null;
			}
		}).when(logger).trace(Mockito.anyString());
		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		ReflectionUtils.setField(fields[0], template, logger);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(new Callable<Message>() {

			public Message call() throws Exception {
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
			}

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
		connectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithRoutingKey() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		final RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(new Callable<Message>() {

			public Message call() throws Exception {
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
			}

		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive(ROUTE, message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive(ROUTE);
		assertEquals(null, reply);
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithExchangeAndRoutingKey() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		final RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<Message> received = executor.submit(new Callable<Message>() {

			public Message call() throws Exception {
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
			}

		});
		Message message = new Message("test-message".getBytes(), new MessageProperties());
		Message reply = template.sendAndReceive("", ROUTE, message);
		assertEquals(new String(message.getBody()), new String(received.get(1000, TimeUnit.MILLISECONDS).getBody()));
		assertNotNull("Reply is expected", reply);
		assertEquals(new String(message.getBody()), new String(reply.getBody()));
		// Message was consumed so nothing left on queue
		reply = template.receive(ROUTE);
		assertEquals(null, reply);
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversion() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		final RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
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
			}

		});
		String result = (String) template.convertSendAndReceive("message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert();
		assertEquals(null, result);
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionUsingRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
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
			}

		});
		String result = (String) template.convertSendAndReceive(ROUTE, "message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionUsingExchangeAndRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
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
			}

		});
		String result = (String) template.convertSendAndReceive("", ROUTE, "message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessor() throws Exception {
		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
		final RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
		template.setRoutingKey(ROUTE);
		template.setQueue(ROUTE);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
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
			}

		});
		String result = (String) template.convertSendAndReceive((Object) "message", new MessagePostProcessor() {
			public Message postProcessMessage(Message message) throws AmqpException {
				try {
					byte[] newBody = new String(message.getBody(), "UTF-8").toUpperCase().getBytes("UTF-8");
					return new Message(newBody, message.getMessageProperties());
				}
				catch (Exception e) {
					throw new AmqpException("unexpected failure in test", e);
				}
			}
		});
		assertEquals("MESSAGE", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("MESSAGE", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert();
		assertEquals(null, result);
		cachingConnectionFactory.destroy();
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessorUsingRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
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
			}

		});
		String result = (String) template.convertSendAndReceive(ROUTE, (Object) "message", new MessagePostProcessor() {
			public Message postProcessMessage(Message message) throws AmqpException {
				try {
					byte[] newBody = new String(message.getBody(), "UTF-8").toUpperCase().getBytes("UTF-8");
					return new Message(newBody, message.getMessageProperties());
				}
				catch (Exception e) {
					throw new AmqpException("unexpected failure in test", e);
				}
			}
		});
		assertEquals("MESSAGE", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("MESSAGE", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testAtomicSendAndReceiveWithConversionAndMessagePostProcessorUsingExchangeAndRoutingKey() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to our producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
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
			}

		});
		String result = (String) template.convertSendAndReceive("", ROUTE, "message", new MessagePostProcessor() {

			public Message postProcessMessage(Message message) throws AmqpException {
				try {
					byte[] newBody = new String(message.getBody(), "UTF-8").toUpperCase().getBytes("UTF-8");
					return new Message(newBody, message.getMessageProperties());
				}
				catch (Exception e) {
					throw new AmqpException("unexpected failure in test", e);
				}
			}
		});
		assertEquals("MESSAGE", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("MESSAGE", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveAndReply() {
		this.template.setQueue(ROUTE);
		this.template.setRoutingKey(ROUTE);
		this.template.convertAndSend(ROUTE, "test");

		boolean received = this.template.receiveAndReply(new ReceiveAndReplyMessageCallback() {

			@Override
			public Message handle(Message message) {
				message.getMessageProperties().setHeader("foo", "bar");
				return message;
			}
		});
		assertTrue(received);

		Message receive = this.template.receive();
		assertEquals("bar", receive.getMessageProperties().getHeaders().get("foo"));

		this.template.convertAndSend(ROUTE, 1);

		received = this.template.receiveAndReply(ROUTE, new ReceiveAndReplyCallback<Integer, Integer>() {

			@Override
			public Integer handle(Integer payload) {
				return payload + 1;
			}
		});
		assertTrue(received);

		Object result = this.template.receiveAndConvert(ROUTE);
		assertTrue(result instanceof Integer);
		assertEquals(2, result);

		this.template.convertAndSend(ROUTE, 2);

		received = this.template.receiveAndReply(ROUTE, new ReceiveAndReplyCallback<Integer, Integer>() {

			@Override
			public Integer handle(Integer payload) {
				return payload * 2;
			}
		}, "", ROUTE);
		assertTrue(received);

		result = this.template.receiveAndConvert(ROUTE);
		assertTrue(result instanceof Integer);
		assertEquals(4, result);

		received = this.template.receiveAndReply(new ReceiveAndReplyMessageCallback() {

			@Override
			public Message handle(Message message) {
				return message;
			}
		});
		assertFalse(received);

		this.template.convertAndSend(ROUTE, "test");
		received = this.template.receiveAndReply(new ReceiveAndReplyMessageCallback() {

			@Override
			public Message handle(Message message) {
				return null;
			}
		});
		assertTrue(received);

		result = this.template.receive();
		assertNull(result);

		this.template.convertAndSend(ROUTE, "TEST");
		received = this.template.receiveAndReply(new ReceiveAndReplyMessageCallback() {

			@Override
			public Message handle(Message message) {
				MessageProperties messageProperties = new MessageProperties();
				messageProperties.setContentType(message.getMessageProperties().getContentType());
				messageProperties.setHeader("testReplyTo", new Address("", "", ROUTE));
				return new Message(message.getBody(), messageProperties);
			}

		}, new ReplyToAddressCallback<Message>() {

			@Override
			public Address getReplyToAddress(Message request, Message reply) {
				return (Address) reply.getMessageProperties().getHeaders().get("testReplyTo");
			}

		});
		assertTrue(received);
		result = this.template.receiveAndConvert(ROUTE);
		assertEquals("TEST", result);

		assertEquals(null, template.receive(ROUTE));

		template.setChannelTransacted(true);

		this.template.convertAndSend(ROUTE, "TEST");
		result = new TransactionTemplate(new TestTransactionManager())
				.execute(new TransactionCallback<String>() {
					public String doInTransaction(TransactionStatus status) {
						final AtomicReference<String> payloadReference = new AtomicReference<String>();
						boolean received = template.receiveAndReply(new ReceiveAndReplyCallback<String, Void>() {

							@Override
							public Void handle(String payload) {
								payloadReference.set(payload);
								return null;
							}
						});
						assertTrue(received);
						return payloadReference.get();
					}
				});
		assertEquals("TEST", result);
		assertEquals(null, template.receive(ROUTE));

		this.template.convertAndSend(ROUTE, "TEST");
		try {
			new TransactionTemplate(new TestTransactionManager())
					.execute(new TransactionCallbackWithoutResult() {

						public void doInTransactionWithoutResult(TransactionStatus status) {
							template.receiveAndReply(new ReceiveAndReplyMessageCallback() {

														 @Override
														 public Message handle(Message message) {
															 return message;
														 }
													 }, new ReplyToAddressCallback<Message>() {

														 @Override
														 public Address getReplyToAddress(Message request, Message reply) {
															 throw new PlannedException();
														 }
													 });
						}
					});
			fail("Expected PlannedException");
		}
		catch (Exception e) {
			assertTrue(e.getCause() instanceof PlannedException);
		}

		assertEquals("TEST", template.receiveAndConvert(ROUTE));
		assertEquals(null, template.receive(ROUTE));

	}

	@SuppressWarnings("serial")
	private class PlannedException extends RuntimeException {
		public PlannedException() {
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

}
