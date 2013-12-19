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
import java.util.Collections;
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
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
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
		container.stop();
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
		container.stop();
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
		container.stop();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testConsumerArgs() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final AtomicReference<Map<?, ?>> args = new AtomicReference<Map<?,?>>();
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				consumer.set((Consumer) invocation.getArguments()[6]);
				consumer.get().handleConsumeOk((String) invocation.getArguments()[2]);
				args.set((Map<?, ?>) invocation.getArguments()[5]);
				return null;
			}
		}).when(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), any(Map.class), any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
			}
		});
		container.setConsumerArguments(Collections. <String, Object> singletonMap("x-priority", Integer.valueOf(10)));
		container.afterPropertiesSet();
		container.start();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), any(Map.class), any(Consumer.class));
		assertTrue(args.get() != null);
		assertEquals(10, args.get().get("x-priority"));
		consumer.get().handleCancelOk("foo");
		container.stop();
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
