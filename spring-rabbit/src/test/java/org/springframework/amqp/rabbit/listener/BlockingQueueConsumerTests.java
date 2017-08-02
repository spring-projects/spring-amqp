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

package org.springframework.amqp.rabbit.listener;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Johno Crawford
 *
 * @since 1.0.1
 *
 */
public class BlockingQueueConsumerTests {

	@Rule
	public LogLevelAdjuster adjuster = new LogLevelAdjuster(Level.ERROR, BlockingQueueConsumer.class);

	@Test
	public void testRequeue() throws Exception {
		Exception ex = new RuntimeException();
		testRequeueOrNotDefaultYes(ex, true);
	}

	@Test
	public void testRequeueNullException() throws Exception {
		testRequeueOrNotDefaultYes(null, true);
	}

	@Test
	public void testDontRequeue() throws Exception {
		testRequeueOrNotDefaultYes(new AmqpRejectAndDontRequeueException("fail"), false);
	}

	@Test
	public void testDontRequeueNested() throws Exception {
		Exception ex = new RuntimeException(new RuntimeException(new AmqpRejectAndDontRequeueException("fail")));
		testRequeueOrNotDefaultYes(ex, false);
	}

	@Test
	public void testRequeueDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(new RuntimeException(), false);
	}

	@Test
	public void testRequeueNullExceptionDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(null, false);
	}

	@Test
	public void testDontRequeueDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(new AmqpRejectAndDontRequeueException("fail"), false);
	}

	@Test
	public void testDontRequeueNestedDefaultNot() throws Exception {
		Exception ex = new RuntimeException(new RuntimeException(new AmqpRejectAndDontRequeueException("fail")));
		testRequeueOrNotDefaultNo(ex, false);
	}

	/**
	 * We should always requeue if the exception is a
	 * {@link MessageRejectedWhileStoppingException}.
	 */
	@Test
	public void testDoRequeueStoppingDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(new MessageRejectedWhileStoppingException(), true);
	}

	@Test
	public void testPrefetchIsSetOnFailedPassiveDeclaration() throws IOException {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);

		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(anyBoolean())).thenReturn(channel);
		when(channel.isOpen()).thenReturn(true);
		when(channel.queueDeclarePassive(anyString()))
				.then(invocation -> {
					String arg = invocation.getArgument(0);
					if ("good".equals(arg)) {
						return any(AMQP.Queue.DeclareOk.class);
					}
					else {
						throw new IOException();
					}
				});
		when(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class))).thenReturn("consumerTag");

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 20, "good", "bad");

		blockingQueueConsumer.setDeclarationRetries(1);
		blockingQueueConsumer.setRetryDeclarationInterval(10);
		blockingQueueConsumer.setFailedDeclarationRetryInterval(10);
		blockingQueueConsumer.start();

		verify(channel).basicQos(20);
	}

	@Test
	public void testNoLocalConsumerConfiguration() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);

		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(anyBoolean())).thenReturn(channel);
		when(channel.isOpen()).thenReturn(true);

		final String queue = "testQ";
		final boolean noLocal = true;

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, true, null, noLocal, false, queue);
		blockingQueueConsumer.start();
		verify(channel)
				.basicConsume(eq(queue), eq(AcknowledgeMode.AUTO.isAutoAck()), eq(""), eq(noLocal),
						eq(false), anyMap(), any(Consumer.class));
		blockingQueueConsumer.stop();
	}

	@Test
	public void testRecoverAfterDeletedQueueAndLostConnection() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		given(connection.isOpen()).willReturn(true);
		given(channel.isOpen()).willReturn(true);
		final AtomicInteger n = new AtomicInteger();
		ArgumentCaptor<Consumer> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
		final CountDownLatch consumerLatch = new CountDownLatch(2);
		willAnswer(invocation -> {
			consumerLatch.countDown();
			return "consumer" + n.incrementAndGet();
		}).given(channel).basicConsume(anyString(),
				anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), consumerCaptor.capture());
		willThrow(new IOException("Intentional cancel fail")).given(channel).basicCancel("consumer2");

		final BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, false, 1, "testQ1", "testQ2");
		final CountDownLatch latch = new CountDownLatch(1);
		Executors.newSingleThreadExecutor().execute(() -> {
			blockingQueueConsumer.start();
			while (true) {
				try {
					blockingQueueConsumer.nextMessage(1000);
				}
				catch (ConsumerCancelledException e) {
					latch.countDown();
					break;
				}
				catch (ShutdownSignalException e) {
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));
		Consumer consumer = consumerCaptor.getValue();
		consumer.handleCancel("consumer1");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
	}

	private void testRequeueOrNotDefaultYes(Exception ex, boolean expectedRequeue) throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel channel = mock(Channel.class);
		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, "testQ");
		testRequeueOrNotGuts(ex, expectedRequeue, channel, blockingQueueConsumer);
	}

	private void testRequeueOrNotDefaultNo(Exception ex, boolean expectedRequeue) throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel channel = mock(Channel.class);
		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, false, "testQ");
		testRequeueOrNotGuts(ex, expectedRequeue, channel, blockingQueueConsumer);
	}

	private void testRequeueOrNotGuts(Exception ex, boolean expectedRequeue,
			Channel channel, BlockingQueueConsumer blockingQueueConsumer) throws Exception {
		DirectFieldAccessor dfa = new DirectFieldAccessor(blockingQueueConsumer);
		dfa.setPropertyValue("channel", channel);
		Set<Long> deliveryTags = new HashSet<Long>();
		deliveryTags.add(1L);
		dfa.setPropertyValue("deliveryTags", deliveryTags);
		blockingQueueConsumer.rollbackOnExceptionIfNecessary(ex);
		verify(channel).basicNack(1L, true, expectedRequeue);
	}

	@Test
	public void testAlwaysCancelAutoRecoverConsumer() throws IOException {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel = mock(AutorecoveringChannel.class);
		when(channel.getTargetChannel()).thenReturn(rabbitChannel);

		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(anyBoolean())).thenReturn(channel);
		final AtomicBoolean isOpen = new AtomicBoolean(true);
		doReturn(isOpen.get()).when(channel).isOpen();
		when(channel.queueDeclarePassive(anyString()))
				.then(invocation -> mock(AMQP.Queue.DeclareOk.class));
		when(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class))).thenReturn("consumerTag");

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<>(),
				AcknowledgeMode.AUTO, true, 2, "test");

		blockingQueueConsumer.setDeclarationRetries(1);
		blockingQueueConsumer.setRetryDeclarationInterval(10);
		blockingQueueConsumer.setFailedDeclarationRetryInterval(10);
		blockingQueueConsumer.start();

		verify(channel).basicQos(2);
		isOpen.set(false);
		blockingQueueConsumer.stop();
		verify(channel).basicCancel("consumerTag");
	}

	@Test
	public void testDrainAndReject() throws IOException {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		ChannelProxy channel = mock(ChannelProxy.class);
		Channel rabbitChannel = mock(AutorecoveringChannel.class);
		when(channel.getTargetChannel()).thenReturn(rabbitChannel);

		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(anyBoolean())).thenReturn(channel);
		final AtomicBoolean isOpen = new AtomicBoolean(true);
		doReturn(isOpen.get()).when(channel).isOpen();
		when(channel.queueDeclarePassive(anyString()))
				.then(invocation -> mock(AMQP.Queue.DeclareOk.class));
		when(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class))).thenReturn("consumerTag");

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 2, "test");

		blockingQueueConsumer.setDeclarationRetries(1);
		blockingQueueConsumer.setRetryDeclarationInterval(10);
		blockingQueueConsumer.setFailedDeclarationRetryInterval(10);
		blockingQueueConsumer.start();

		verify(channel).basicQos(2);
		Consumer consumer = TestUtils.getPropertyValue(blockingQueueConsumer, "consumer", Consumer.class);
		isOpen.set(false);
		blockingQueueConsumer.stop();
		verify(channel).basicCancel("consumerTag");

		Envelope envelope = new Envelope(1, false, "foo", "bar");
		BasicProperties props = mock(BasicProperties.class);
		consumer.handleDelivery("consumerTag", envelope, props, new byte[0]);
		envelope = new Envelope(2, false, "foo", "bar");
		consumer.handleDelivery("consumerTag", envelope, props, new byte[0]);
		assertThat(TestUtils.getPropertyValue(blockingQueueConsumer, "queue", BlockingQueue.class).size(), equalTo(2));
		envelope = new Envelope(3, false, "foo", "bar");
		consumer.handleDelivery("consumerTag", envelope, props, new byte[0]);
		assertThat(TestUtils.getPropertyValue(blockingQueueConsumer, "queue", BlockingQueue.class).size(), equalTo(0));
		verify(channel).basicNack(3, true, true);
		verify(channel, times(2)).basicCancel("consumerTag");
	}


}
