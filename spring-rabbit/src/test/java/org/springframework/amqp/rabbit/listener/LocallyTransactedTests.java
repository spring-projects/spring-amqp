/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.DirectFieldAccessor;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

/**
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class LocallyTransactedTests {

	/**
	 * Verifies that an up-stack transactional RabbitTemplate uses the listener's
	 * channel (MessageListener).
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMessageListener() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		given(onlyChannel.isOpen()).willReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return onlyChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				given(channel.isOpen()).willReturn(true);
				return channel;
			}
		}).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgumentAt(6, Consumer.class));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(onlyChannel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final AtomicReference<CountDownLatch> commitLatch = new AtomicReference<>(new CountDownLatch(1));
		willAnswer(invocation -> {
			commitLatch.get().countDown();
			return null;
		}).given(onlyChannel).txCommit();
		final AtomicReference<CountDownLatch> rollbackLatch = new AtomicReference<>(new CountDownLatch(1));
		willAnswer(invocation -> {
			rollbackLatch.get().countDown();
			return null;
		}).given(onlyChannel).txRollback();
		willAnswer(invocation -> {
			return null;
		}).given(onlyChannel).basicAck(anyLong(), anyBoolean());

		final CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cachingConnectionFactory);
		container.setMessageListener((MessageListener) message -> {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cachingConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, times(1)).createChannel();
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel).txCommit();
		verify(onlyChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		DirectFieldAccessor dfa = new DirectFieldAccessor(cachingConnectionFactory);
		List<?> channels = (List<?>) dfa.getPropertyValue("cachedChannelsTransactional");
		assertEquals(0, channels.size());

		container.setMessageListener((MessageListener) m -> {
			throw new RuntimeException();
		});
		commitLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		assertTrue(rollbackLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel).basicReject(anyLong(), anyBoolean());
		verify(onlyChannel, times(1)).txRollback();

		// ImmediateAck tests

		container.setMessageListener((MessageListener) m -> {
			throw new AmqpRejectAndDontRequeueException("foo", new ImmediateAcknowledgeAmqpException("bar"));
		});
		commitLatch.set(new CountDownLatch(1));
		rollbackLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(rollbackLatch.get().await(10, TimeUnit.SECONDS));
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel, times(2)).basicReject(anyLong(), anyBoolean());
		verify(onlyChannel, times(2)).txRollback();

		container.setMessageListener((MessageListener) m -> {
			throw new ImmediateAcknowledgeAmqpException("foo");
		});
		commitLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel, times(2)).basicAck(anyLong(), anyBoolean());
		verify(onlyChannel, times(4)).txCommit();

		container.stop();
	}

	/**
	 * Verifies that an up-stack non-transactional RabbitTemplate does not use the
	 * listener's channel (MessageListener).
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testSeparateTx() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final Channel channel1 = mock(Channel.class);
		given(channel1.isOpen()).willReturn(true);
		final Channel channel2 = mock(Channel.class);
		given(channel2.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(new Answer<Channel>() {

			int n;

			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				n++;
				if (n == 1) {
					return channel1;
				}
				else if (n == 2) {
					return channel2;
				}
				else {
					tooManyChannels.set(new Exception("More than two channels requested"));
					return channel2;
				}
			}
		}).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();

		willAnswer(new Answer<String>() {

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				consumer.set((Consumer) invocation.getArguments()[6]);
				return "consumerTag";
			}

		}).given(channel1).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
				any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(new Answer<String>() {

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				commitLatch.countDown();
				return null;
			}

		}).given(channel1).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cachingConnectionFactory);
		container.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				RabbitTemplate rabbitTemplate = new RabbitTemplate(cachingConnectionFactory);
				rabbitTemplate.setChannelTransacted(false);
				// should NOT use same channel as container
				rabbitTemplate.convertAndSend("foo", "bar", "baz");
				latch.countDown();
			}
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.afterPropertiesSet();
		container.start();

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, times(2)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(channel1).txCommit();
		verify(channel1, never()).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		verify(channel2).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		// verify close() was never called on the channel
		DirectFieldAccessor dfa = new DirectFieldAccessor(cachingConnectionFactory);
		List<?> channels = (List<?>) dfa.getPropertyValue("cachedChannelsNonTransactional");
		assertEquals(1, channels.size());

		container.stop();

	}

	/**
	 * Verifies that an up-stack RabbitTemplate uses the listener's
	 * channel (ChannelAwareMessageListener).
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testChannelAwareMessageListener() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		given(onlyChannel.isOpen()).willReturn(true);

		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(mockConnectionFactory);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return onlyChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				given(channel.isOpen()).willReturn(true);
				return channel;
			}
		}).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();

		willAnswer(new Answer<String>() {

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				consumer.set((Consumer) invocation.getArguments()[6]);
				return "consumerTag";
			}
		}).given(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(new Answer<String>() {

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				commitLatch.countDown();
				return null;
			}
		}).given(onlyChannel).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Channel> exposed = new AtomicReference<Channel>();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new ChannelAwareMessageListener() {
			@Override
			public void onMessage(Message message, Channel channel) {
				exposed.set(channel);
				RabbitTemplate rabbitTemplate = new RabbitTemplate(singleConnectionFactory);
				rabbitTemplate.setChannelTransacted(true);
				// should use same channel as container
				rabbitTemplate.convertAndSend("foo", "bar", "baz");
				latch.countDown();
			}

		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.afterPropertiesSet();
		container.start();

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, Mockito.times(1)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(onlyChannel).txCommit();
		verify(onlyChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		// verify close() was never called on the channel
		verify(onlyChannel, Mockito.never()).close();

		container.stop();

		assertSame(onlyChannel, exposed.get());
	}

	/**
	 * Verifies that the listener channel is not exposed when so configured and
	 * up-stack RabbitTemplate uses the additional channel.
	 * created when exposeListenerChannel is false (ChannelAwareMessageListener).
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testChannelAwareMessageListenerDontExpose() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel firstChannel = mock(Channel.class);
		given(firstChannel.isOpen()).willReturn(true);
		final Channel secondChannel = mock(Channel.class);
		given(secondChannel.isOpen()).willReturn(true);

		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(mockConnectionFactory);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return firstChannel;
				}
				return secondChannel;
			}
		}).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();

		willAnswer(new Answer<String>() {

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				consumer.set((Consumer) invocation.getArguments()[6]);
				return "consumerTag";
			}
		}).given(firstChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(new Answer<String>() {

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				commitLatch.countDown();
				return null;
			}
		}).given(firstChannel).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Channel> exposed = new AtomicReference<Channel>();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new ChannelAwareMessageListener() {
			@Override
			public void onMessage(Message message, Channel channel) {
				exposed.set(channel);
				RabbitTemplate rabbitTemplate = new RabbitTemplate(singleConnectionFactory);
				rabbitTemplate.setChannelTransacted(true);
				// should use same channel as container
				rabbitTemplate.convertAndSend("foo", "bar", "baz");
				latch.countDown();
			}

		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setExposeListenerChannel(false);
		container.setShutdownTimeout(100);
		container.afterPropertiesSet();
		container.start();

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		// once for listener, once for exposed + 0 for template (used bound)
		verify(mockConnection, Mockito.times(2)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(firstChannel).txCommit();
		verify(secondChannel).txCommit();
		verify(secondChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		assertSame(secondChannel, exposed.get());

		verify(firstChannel, Mockito.never()).close();
		verify(secondChannel, Mockito.times(1)).close();
		container.stop();
	}
}
