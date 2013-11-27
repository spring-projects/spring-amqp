/*
 * Copyright 2002-2013 the original author or authors.
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
package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

/**
 * @author David Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 *
 */
public class SimpleMessageListenerContainerTests {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testInconsistentTransactionConfiguration() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(false);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setTransactionManager(new TestTransactionManager());
		expectedException.expect(IllegalStateException.class);
		container.afterPropertiesSet();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testInconsistentAcknowledgeConfiguration() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(true);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		expectedException.expect(IllegalStateException.class);
		container.afterPropertiesSet();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testDefaultConsumerCount() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setAutoStartup(false);
		container.afterPropertiesSet();
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		singleConnectionFactory.destroy();
	}

	@Test
	public void testChangeConsumerCount() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setAutoStartup(false);
		container.setConcurrentConsumers(2);
		container.afterPropertiesSet();
		assertEquals(2, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		container.start();
		waitForNConsumers(container, 2);
		container.setConcurrentConsumers(1);
		waitForNConsumers(container, 1);
		container.setMaxConcurrentConsumers(3);
		RabbitTemplate template = new RabbitTemplate(singleConnectionFactory);
		for (int i = 0; i < 20; i++) {
			template.convertAndSend("foo", "foo");
		}
		waitForNConsumers(container, 2); // increase due to work
		waitForNConsumers(container, 1, 20000); // will decay after 10 seconds idle
		container.setConcurrentConsumers(3);
		waitForNConsumers(container, 3);
		container.stop();
		waitForNConsumers(container, 0);
		singleConnectionFactory.destroy();
	}

	public void handleMessage(String foo) {
		System.out.println(foo);
	}

	private void waitForNConsumers(SimpleMessageListenerContainer container, int n) throws InterruptedException {
		this.waitForNConsumers(container, n, 10000);
	}

	private void waitForNConsumers(SimpleMessageListenerContainer container, int n, int howLong) throws InterruptedException {
		int i = 0;
		while (true) {
			Map<?, ?> consumers = (Map<?, ?>) TestUtils.getPropertyValue(container, "consumers");
			if (n == 0 && consumers == null) {
				break;
			}
			if (consumers != null && consumers.size() == n) {
				break;
			}
			Thread.sleep(100);
			if (i++ > howLong / 100) {
				fail("Never reached " + n + " consumers");
			}
		}
	}

	@Test
	public void testLazyConsumerCount() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory) {
			@Override
			protected void doStart() throws Exception {
				// do nothing
			}
		};
		container.start();
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		singleConnectionFactory.destroy();
	}

	/*
	 * txSize = 2; 4 messages; should get 2 acks (#2 and #4)
	 */
	@Test
	public void testTxSizeAcks() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				consumer.set((Consumer) invocation.getArguments()[2]);
				consumer.get().handleConsumeOk("1");
				return null;
			}
		}).when(channel).basicConsume(anyString(), anyBoolean(), any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(2);
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return null;
			}
		}).when(channel).basicAck(anyLong(), anyBoolean());

		final List<Message> messages = new ArrayList<Message>();
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setTxSize(2);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				messages.add(message);
			}
		});
		container.start();
		BasicProperties props = new BasicProperties();
		byte[] payload = "baz".getBytes();
		Envelope envelope = new Envelope(1L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(2L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(3L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(4L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		assertTrue(latch.await(5, TimeUnit.SECONDS));
		assertEquals(4, messages.size());
		Executors.newSingleThreadExecutor().execute(new Runnable() {

			@Override
			public void run() {
				container.stop();
			}
		});
		consumer.get().handleCancelOk("1");
		verify(channel, times(2)).basicAck(anyLong(), anyBoolean());
		verify(channel).basicAck(2, true);
		verify(channel).basicAck(4, true);
	}

	/*
	 * txSize = 2; 3 messages; should get 2 acks (#2 and #3)
	 * after timeout.
	 */
	@Test
	public void testTxSizeAcksWIthShortSet() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				consumer.set((Consumer) invocation.getArguments()[2]);
				consumer.get().handleConsumeOk("1");
				return null;
			}
		}).when(channel).basicConsume(anyString(), anyBoolean(), any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(2);
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return null;
			}
		}).when(channel).basicAck(anyLong(), anyBoolean());

		final List<Message> messages = new ArrayList<Message>();
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setTxSize(2);
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				messages.add(message);
			}
		});
		container.start();
		BasicProperties props = new BasicProperties();
		byte[] payload = "baz".getBytes();
		Envelope envelope = new Envelope(1L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(2L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(3L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		assertTrue(latch.await(5, TimeUnit.SECONDS));
		assertEquals(3, messages.size());
		Executors.newSingleThreadExecutor().execute(new Runnable() {

			@Override
			public void run() {
				container.stop();
			}
		});
		consumer.get().handleCancelOk("1");
		verify(channel, times(2)).basicAck(anyLong(), anyBoolean());
		verify(channel).basicAck(2, true);
		// second set was short
		verify(channel).basicAck(3, true);
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
