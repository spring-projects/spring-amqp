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

package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory.AddressShuffleMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Mark Pollack
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Steve Powell
 */
public class CachingConnectionFactoryTests extends AbstractConnectionFactoryTests {

	@Override
	protected AbstractConnectionFactory createConnectionFactory(ConnectionFactory connectionFactory) {
		CachingConnectionFactory ccf = new CachingConnectionFactory(connectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		return ccf;
	}

	@Test
	void stringRepresentation() {
		CachingConnectionFactory ccf = new CachingConnectionFactory("someHost", 1234);
		assertThat(ccf.toString()).contains(", host=someHost, port=1234")
				.doesNotContain("addresses");
		ccf.setAddresses("h1:1234,h2:1235");
		assertThat(ccf.toString()).contains(", addresses=[h1:1234, h2:1235]")
				.doesNotContain("host")
				.doesNotContain("port");
		ccf.setAddressResolver(() -> List.of(new Address("h3", 1236), new Address("h4", 1237)));
		assertThat(ccf.toString()).contains(", addresses=[h3:1236, h4:1237]")
				.doesNotContain("host")
				.doesNotContain("port");
		ccf.setAddressResolver(() -> {
			throw new IOException("test");
		});
		ccf.setPort(0);
		assertThat(ccf.toString()).contains(", host=AddressResolver threw exception: test")
				.doesNotContain("addresses")
				.doesNotContain("port");
	}

	@Test
	public void testWithConnectionFactoryDefaults() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		Connection con = ccf.createConnection();

		Channel channel = con.createChannel(false);
		channel.close(); // should be ignored, and placed into channel cache.
		con.close(); // should be ignored

		Connection con2 = ccf.createConnection();
		/*
		 * will retrieve same channel object that was just put into channel cache
		 */
		Channel channel2 = con2.createChannel(false);
		channel2.close(); // should be ignored
		con2.close(); // should be ignored

		assertThat(con2).isSameAs(con);
		assertThat(channel2).isSameAs(channel);
		verify(mockConnection, never()).close();
		verify(mockChannel, never()).close();
	}

	@Test
	public void testPublisherConnection() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		Connection con = ccf.getPublisherConnectionFactory().createConnection();

		Channel channel = con.createChannel(false);
		channel.close(); // should be ignored, and placed into channel cache.
		con.close(); // should be ignored

		Connection con2 = ccf.getPublisherConnectionFactory().createConnection();
		/*
		 * will retrieve same channel object that was just put into channel cache
		 */
		Channel channel2 = con2.createChannel(false);
		channel2.close(); // should be ignored
		con2.close(); // should be ignored

		assertThat(con2).isSameAs(con);
		assertThat(channel2).isSameAs(channel);
		verify(mockConnection, never()).close();
		verify(mockChannel, never()).close();

		assertThat(TestUtils.getPropertyValue(ccf, "connection.target")).isNull();
		assertThat(TestUtils.getPropertyValue(ccf, "publisherConnectionFactory.connection.target")).isNotNull();
		assertThat(TestUtils.getPropertyValue(ccf, "publisherConnectionFactory.connection")).isSameAs(con);
	}

	@Test
	public void testWithConnectionFactoryCacheSize() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockTxChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel1, mockChannel2, mockTxChannel);

		given(mockChannel1.basicGet("foo", false)).willReturn(new GetResponse(null, null, null, 1));
		given(mockChannel2.basicGet("bar", false)).willReturn(new GetResponse(null, null, null, 1));
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(2);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		Channel channel2 = con.createChannel(false);

		ChannelProxy txChannel = (ChannelProxy) con.createChannel(true);
		assertThat(txChannel.isTransactional()).isTrue();
		verify(mockTxChannel).txSelect();
		txChannel.close();

		channel1.basicGet("foo", true);
		channel2.basicGet("bar", true);

		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // should be ignored, and add last into channel cache.

		Channel ch1 = con.createChannel(false); // remove first entry in cache
		// (channel1)
		Channel ch2 = con.createChannel(false); // remove first entry in cache
		// (channel2)

		assertThat(ch2).isNotSameAs(ch1);
		assertThat(channel1).isSameAs(ch1);
		assertThat(channel2).isSameAs(ch2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(3)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, never()).close();

	}

	@Test
	public void testCacheSizeExceeded() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockChannel3 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel1).willReturn(mockChannel2).willReturn(mockChannel3);
		given(mockConnection.isOpen()).willReturn(true);

		// Called during physical close
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);
		given(mockChannel3.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		// cache size is 1, but the other connection is not released yet so this
		// creates a new one
		Channel channel2 = con.createChannel(false);
		assertThat(channel2).isNotSameAs(channel1);

		// should be ignored, and added last into channel cache.
		channel1.close();
		// should be physically closed
		channel2.close();

		// remove first entry in cache (channel1)
		Channel ch1 = con.createChannel(false);
		// create a new channel
		Channel ch2 = con.createChannel(false);

		assertThat(ch2).isNotSameAs(ch1);
		assertThat(channel1).isSameAs(ch1);
		assertThat(channel2).isNotSameAs(ch2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(3)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, atLeastOnce()).close();
		verify(mockChannel3, atLeastOnce()).close();

	}

	@Test
	public void testCheckoutLimit() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel1);
		given(mockConnection.isOpen()).willReturn(true);

		// Called during physical close
		given(mockChannel1.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);

		try {
			con.createChannel(false);
			fail("Exception expected");
		}
		catch (AmqpTimeoutException e) {
		}

		// should be ignored, and added last into channel cache.
		channel1.close();

		// remove first entry in cache (channel1)
		Channel ch1 = con.createChannel(false);

		assertThat(channel1).isSameAs(ch1);

		ch1.close();

		verify(mockConnection, times(1)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();

		ccf.destroy();
	}

	@Test
	public void testCheckoutLimitWithFailures() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		final com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		final AtomicBoolean brokerDown = new AtomicBoolean();

		willAnswer(i -> {
			if (brokerDown.get()) {
				throw new AmqpConnectException(null);
			}
			return mockConnection;
		}).given(mockConnectionFactory).newConnection((ExecutorService) isNull(), anyString());

		given(mockConnection.createChannel()).willReturn(mockChannel1);

		willAnswer(i -> !brokerDown.get()).given(mockConnection).isOpen();

		// Called during physical close
		willAnswer(i -> !brokerDown.get()).given(mockChannel1).isOpen();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);

		try {
			con.createChannel(false);
			fail("Exception expected");
		}
		catch (AmqpTimeoutException e) {
		}

		// should be ignored, and added last into channel cache.
		channel1.close();

		// remove first entry in cache (channel1)
		Channel ch1 = con.createChannel(false);

		assertThat(channel1).isSameAs(ch1);

		ch1.close();

		brokerDown.set(true);
		try {
			con.createChannel(false);
			fail("Exception expected");
		}
		catch (AmqpConnectException e) {
		}
		brokerDown.set(false);
		ch1 = con.createChannel(false);
		ch1.close();

		ccf.destroy();
	}

	@Test
	public void testConnectionLimit() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setConnectionCacheSize(1);
		ccf.setConnectionLimit(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con1 = ccf.createConnection();

		try {
			ccf.createConnection();
			fail("Exception expected");
		}
		catch (AmqpTimeoutException e) {
		}

		// should be ignored, and added to cache
		con1.close();

		Connection con2 = ccf.createConnection();
		assertThat(con2).isSameAs(con1);

		final CountDownLatch latch2 = new CountDownLatch(1);
		final CountDownLatch latch1 = new CountDownLatch(1);
		final AtomicReference<Connection> connection = new AtomicReference<Connection>();
		ccf.setChannelCheckoutTimeout(30000);
		Executors.newSingleThreadExecutor().execute(() -> {
			latch1.countDown();
			connection.set(ccf.createConnection());
			latch2.countDown();
		});

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(100);
		con2.close();
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(connection.get()).isSameAs(con2);

		ccf.destroy();
	}

	@Test
	public void testCheckoutsWithRefreshedConnectionModeChannel() throws Exception {
		testCheckoutsWithRefreshedConnectionGuts(CacheMode.CHANNEL);
	}

	@Test
	public void testCheckoutsWithRefreshedConnectionModeConnection() throws Exception {
		testCheckoutsWithRefreshedConnectionGuts(CacheMode.CONNECTION);
	}

	private void testCheckoutsWithRefreshedConnectionGuts(CacheMode mode) throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockChannel3 = mock(Channel.class);
		Channel mockChannel4 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(mockConnection1, mockConnection2);
		given(mockConnection1.createChannel()).willReturn(mockChannel1, mockChannel2);
		given(mockConnection1.isOpen()).willReturn(true);
		given(mockConnection2.createChannel()).willReturn(mockChannel3, mockChannel4);
		given(mockConnection2.isOpen()).willReturn(true);

		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);
		given(mockChannel3.isOpen()).willReturn(true);
		given(mockChannel4.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(2);
		ccf.setChannelCheckoutTimeout(10);
		ccf.setCacheMode(mode);

		ccf.addConnectionListener(connection -> {
			try {
				// simulate admin
				connection.createChannel(false).close();
			}
			catch (Exception e) {
				fail(e.getMessage());
			}
		});

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);
		channel1.close();
		con.close();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(2);

		given(mockConnection1.isOpen()).willReturn(false);
		given(mockChannel1.isOpen()).willReturn(false);
		given(mockChannel2.isOpen()).willReturn(false);

		con.createChannel(false).close();
		con = ccf.createConnection();
		con.createChannel(false).close();
		con.createChannel(false).close();
		con.createChannel(false).close();
		con.createChannel(false).close();
		con.createChannel(false).close();

		verify(mockConnection1, times(1)).createChannel();
		verify(mockConnection2, times(2)).createChannel();

		con.close();

		verify(mockConnection2, never()).close();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(2);

		ccf.destroy();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(2);

	}

	@Test
	public void testCheckoutLimitWithRelease() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection = mock();
		Channel mockChannel1 = mock();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel1);
		given(mockConnection.isOpen()).willReturn(true);

		// Called during physical close
		given(mockChannel1.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10000);

		final Connection con = ccf.createConnection();

		final AtomicReference<Channel> channelOne = new AtomicReference<Channel>();
		final CountDownLatch latch = new CountDownLatch(1);

		new Thread(() -> {
			Channel channel1 = con.createChannel(false);
			latch.countDown();
			channelOne.set(channel1);
			try {
				Thread.sleep(100);
				channel1.close();
			}
			catch (InterruptedException e1) {
				Thread.currentThread().interrupt();
			}
			catch (IOException e2) {
			}
			catch (TimeoutException e3) {
			}
		}).start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		Channel channel2 = con.createChannel(false);
		assertThat(channel2).isSameAs(channelOne.get());

		channel2.close();

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();

		ccf.destroy();
	}

	@Test
	public void testCheckoutLimitWithPublisherConfirmsLogical() throws Exception {
		testCheckoutLimitWithPublisherConfirms(false);
	}

	@Test
	public void testCheckoutLimitWithPublisherConfirmsPhysical() throws Exception {
		testCheckoutLimitWithPublisherConfirms(true);
	}

	private void testCheckoutLimitWithPublisherConfirms(boolean physicalClose) throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection = mock();
		Channel mockChannel = mock();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockConnection.isOpen()).willReturn(true);

		// Called during physical close
		given(mockChannel.isOpen()).willReturn(true);
		CountDownLatch confirmsLatch = new CountDownLatch(1);
		willAnswer(invoc -> {
			confirmsLatch.await(10, TimeUnit.SECONDS);
			return null;
		}).given(mockChannel).waitForConfirmsOrDie(anyLong());
		AtomicReference<ConfirmListener> confirmListener = new AtomicReference<>();
		willAnswer(invoc -> {
			confirmListener.set(invoc.getArgument(0));
			return null;
		}).given(mockChannel).addConfirmListener(any());
		given(mockChannel.getNextPublishSeqNo()).willReturn(1L);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ExecutorService exec = Executors.newCachedThreadPool();
		ccf.setExecutor(exec);
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(1);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);

		final Connection con = ccf.createConnection();

		RabbitTemplate rabbitTemplate = new RabbitTemplate(ccf);
		if (physicalClose) {
			Channel channel1 = con.createChannel(false);
			RabbitUtils.setPhysicalCloseRequired(channel1, true);
			channel1.close();
		}
		else {
			rabbitTemplate.convertAndSend("foo", "bar"); // pending confirm
		}
		assertThatThrownBy(() -> con.createChannel(false)).isInstanceOf(AmqpTimeoutException.class);
		int n = 0;
		if (physicalClose) {
			confirmsLatch.countDown();
			Channel channel2 = null;
			while (channel2 == null && n++ < 100) {
				try {
					channel2 = con.createChannel(false);
				}
				catch (Exception e) {
					Thread.sleep(100);
				}
			}
			assertThat(channel2).isNotNull();
		}
		else {
			confirmListener.get().handleAck(1L, false);
			boolean ok = false;
			while (!ok && n++ < 100) {
				try {
					rabbitTemplate.convertAndSend("foo", "bar");
					ok = true;
				}
				catch (Exception e) {
					Thread.sleep(100);
				}
			}
			assertThat(ok).isTrue();
		}
		exec.shutdownNow();
	}

	@Test
	public void testCheckoutLimitWithPublisherConfirmsLogicalAlreadyCloses() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockConnection.isOpen()).willReturn(true);

		AtomicBoolean open = new AtomicBoolean(true);
		willAnswer(invoc -> {
			return open.get();
		}).given(mockChannel).isOpen();
		given(mockChannel.getNextPublishSeqNo()).willReturn(1L);
		willAnswer(invoc -> {
			open.set(false); // so the logical close detects a closed delegate
			return null;
		}).given(mockChannel).basicPublish(any(), any(), anyBoolean(), any(), any());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(1);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);

		RabbitTemplate rabbitTemplate = new RabbitTemplate(ccf);
		rabbitTemplate.convertAndSend("foo", "bar");
		open.set(true);
		rabbitTemplate.convertAndSend("foo", "bar");
		verify(mockChannel, times(2)).basicPublish(any(), any(), anyBoolean(), any(), any());
	}

	@Test
	public void testReleaseWithForcedPhysicalClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection1);
		given(mockConnection1.createChannel()).willReturn(mockChannel1);
		given(mockConnection1.isOpen()).willReturn(true);

		given(mockChannel1.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(0);
		channel1.close();
		con.close();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);

		channel1 = con.createChannel(false);
		RabbitUtils.setPhysicalCloseRequired(channel1, true);
		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(0);

		channel1.close();
		RabbitUtils.setPhysicalCloseRequired(channel1, false);
		con.close();
		verify(mockChannel1).close();
		verify(mockConnection1, never()).close();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);

		ccf.destroy();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);

	}

	@Test
	public void testDoubleLogicalClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection1);
		given(mockConnection1.createChannel()).willReturn(mockChannel1);
		given(mockConnection1.isOpen()).willReturn(true);

		given(mockChannel1.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(0);
		channel1.close();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);

		channel1.close(); // double close of proxy

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);

		con.close();
		verify(mockChannel1, never()).close();
		verify(mockConnection1, never()).close();

		ccf.destroy();

		assertThat(((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
				.availablePermits()).isEqualTo(1);

	}

	@Test
	public void testCacheSizeExceededAfterClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel1).willReturn(mockChannel2);
		given(mockConnection.isOpen()).willReturn(true);

		// Called during physical close
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		channel1.close(); // should be ignored, and add last into channel cache.
		Channel channel2 = con.createChannel(false);
		channel2.close(); // should be ignored, and add last into channel cache.
		assertThat(channel2).isSameAs(channel1);

		Channel ch1 = con.createChannel(false); // remove first entry in cache
		// (channel1)
		Channel ch2 = con.createChannel(false); // create new channel

		assertThat(ch2).isNotSameAs(ch1);
		assertThat(channel1).isSameAs(ch1);
		assertThat(channel2).isNotSameAs(ch2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(2)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, atLeastOnce()).close();

	}

	@Test
	public void testTransactionalAndNonTransactionalChannelsSegregated() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel1).willReturn(mockChannel2);
		given(mockConnection.isOpen()).willReturn(true);

		// Called during physical close
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(true);
		channel1.txSelect();
		channel1.close(); // should be ignored, and add last into channel cache.
		/*
		 * When a channel is created as non-transactional we should create a new one.
		 */
		Channel channel2 = con.createChannel(false);
		channel2.close(); // should be ignored, and add last into channel cache.
		assertThat(channel2).isNotSameAs(channel1);

		Channel ch1 = con.createChannel(true); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(false); // create new channel

		assertThat(ch2).isNotSameAs(ch1);
		assertThat(channel1).isSameAs(ch1); // The non-transactional one
		assertThat(channel2).isSameAs(ch2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(2)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, never()).close();

		@SuppressWarnings("unchecked")
		List<Channel> notxlist = (List<Channel>) ReflectionTestUtils.getField(ccf, "cachedChannelsNonTransactional");
		assertThat(notxlist).hasSize(1);
		@SuppressWarnings("unchecked")
		List<Channel> txlist = (List<Channel>) ReflectionTestUtils.getField(ccf, "cachedChannelsTransactional");
		assertThat(txlist).hasSize(1);

	}

	@Test
	public void testWithConnectionFactoryDestroy() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);

		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockChannel3 = mock(Channel.class);

		assertThat(mockChannel2).isNotSameAs(mockChannel1);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection1, mockConnection2);
		given(mockConnection1.createChannel()).willReturn(mockChannel1, mockChannel2);
		given(mockConnection1.isOpen()).willReturn(true);
		given(mockConnection2.createChannel()).willReturn(mockChannel3);
		given(mockConnection2.isOpen()).willReturn(true);
		// Called during physical close
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);
		given(mockChannel3.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(2);

		Connection con = ccf.createConnection();

		// This will return a proxy that surpresses calls to close
		Channel channel1 = con.createChannel(false);
		Channel channel2 = con.createChannel(false);

		// Should be ignored, and add last into channel cache.
		channel1.close();
		channel2.close();

		// remove first entry in cache (channel1)
		Channel ch1 = con.createChannel(false);
		// remove first entry in cache (channel2)
		Channel ch2 = con.createChannel(false);

		assertThat(channel1).isSameAs(ch1);
		assertThat(channel2).isSameAs(ch2);

		Channel target1 = ((ChannelProxy) ch1).getTargetChannel();
		Channel target2 = ((ChannelProxy) ch2).getTargetChannel();

		// make sure mokito returned different mocks for the channel
		assertThat(target2).isNotSameAs(target1);

		ch1.close();
		ch2.close();
		con.close(); // should be ignored
		com.rabbitmq.client.Connection conDelegate = targetDelegate(con);

		ccf.destroy(); // should call close on connection and channels in cache

		verify(mockConnection1, times(2)).createChannel();

		verify(mockConnection1).close(anyInt());

		// verify(mockChannel1).close();
		verify(mockChannel2).close();

		// After destroy we can get a new connection
		Connection con1 = ccf.createConnection();
		assertThat(targetDelegate(con1)).isNotSameAs(conDelegate);

		// This will return a proxy that surpresses calls to close
		Channel channel3 = con.createChannel(false);
		assertThat(channel1).isNotSameAs(channel3);
		assertThat(channel2).isNotSameAs(channel3);
	}

	@Test
	public void testWithChannelListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.setChannelListeners(Arrays.asList(new ChannelListener() {

			@Override
			public void onCreate(Channel channel, boolean transactional) {
				called.incrementAndGet();
			}
		}));
		((CachingConnectionFactory) connectionFactory).setChannelCacheSize(1);

		Connection con = connectionFactory.createConnection();
		Channel channel = con.createChannel(false);
		assertThat(called.get()).isEqualTo(1);
		channel.close();

		con.close();
		verify(mockConnection, never()).close();

		connectionFactory.createConnection();
		con.createChannel(false);
		assertThat(called.get()).isEqualTo(1);

		connectionFactory.destroy();
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

	}

	@Test
	public void testWithConnectionListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		given(mockConnection1.toString()).willReturn("conn1");
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);
		given(mockConnection2.toString()).willReturn("conn2");
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection1, mockConnection2);
		given(mockConnection1.isOpen()).willReturn(true);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection1.createChannel()).willReturn(mockChannel);
		given(mockConnection2.createChannel()).willReturn(mockChannel);

		final AtomicReference<Connection> created = new AtomicReference<Connection>();
		final AtomicReference<Connection> closed = new AtomicReference<Connection>();
		final AtomicInteger timesClosed = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.addConnectionListener(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				created.set(connection);
			}

			@Override
			public void onClose(Connection connection) {
				closed.set(connection);
				timesClosed.getAndAdd(1);
			}
		});
		((CachingConnectionFactory) connectionFactory).setChannelCacheSize(1);

		Connection con = connectionFactory.createConnection();
		Channel channel = con.createChannel(false);
		assertThat(created.get()).isSameAs(con);
		channel.close();

		con.close();
		verify(mockConnection1, never()).close();

		Connection same = connectionFactory.createConnection();
		channel = con.createChannel(false);
		assertThat(same).isSameAs(con);
		channel.close();
		com.rabbitmq.client.Connection conDelegate = targetDelegate(con);

		given(mockConnection1.isOpen()).willReturn(false);
		given(mockChannel.isOpen()).willReturn(false); // force a connection refresh
		channel.basicCancel("foo");
		channel.close();
		assertThat(timesClosed.get()).isEqualTo(1);

		Connection notSame = connectionFactory.createConnection();
		assertThat(targetDelegate(notSame)).isNotSameAs(conDelegate);
		assertThat(closed.get()).isSameAs(con);
		assertThat(created.get()).isSameAs(notSame);

		connectionFactory.destroy();
		verify(mockConnection2, atLeastOnce()).close(anyInt());
		assertThat(closed.get()).isSameAs(notSame);
		assertThat(timesClosed.get()).isEqualTo(2);

		verify(mockConnectionFactory, times(2)).newConnection(any(ExecutorService.class), anyString());
	}

	@Test
	public void testWithConnectionFactoryCachedConnection() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);

		final List<com.rabbitmq.client.Connection> mockConnections = new ArrayList<com.rabbitmq.client.Connection>();
		final List<Channel> mockChannels = new ArrayList<Channel>();

		AtomicInteger connectionNumber = new AtomicInteger();
		willAnswer(invocation -> {
			com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
			AtomicInteger channelNumber = new AtomicInteger();
			willAnswer(invocation1 -> {
				Channel channel = mock(Channel.class);
				given(channel.isOpen()).willReturn(true);
				int channelNum = channelNumber.incrementAndGet();
				given(channel.toString()).willReturn("mockChannel" + connectionNumber + ":" + channelNum);
				mockChannels.add(channel);
				return channel;
			}).given(connection).createChannel();
			int connectionNum = connectionNumber.incrementAndGet();
			given(connection.toString()).willReturn("mockConnection" + connectionNum);
			given(connection.isOpen()).willReturn(true);
			mockConnections.add(connection);
			return connection;
		}).given(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.afterPropertiesSet();

		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertThat(allocatedConnections).hasSize(0);
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertThat(idleConnections).hasSize(0);

		final AtomicReference<com.rabbitmq.client.Connection> createNotification =
				new AtomicReference<com.rabbitmq.client.Connection>();
		final AtomicReference<com.rabbitmq.client.Connection> closedNotification =
				new AtomicReference<com.rabbitmq.client.Connection>();
		ccf.setConnectionListeners(Collections.singletonList(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				assertThat(createNotification.get()).isNull();
				createNotification.set(targetDelegate(connection));
			}

			@Override
			public void onClose(Connection connection) {
				assertThat(closedNotification.get()).isNull();
				closedNotification.set(targetDelegate(connection));
			}
		}));

		Connection con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		assertThat(allocatedConnections).hasSize(1);
		assertThat(idleConnections).hasSize(0);
		assertThat(createNotification.get()).isNotNull();
		assertThat(createNotification.getAndSet(null)).isSameAs(mockConnections.get(0));

		Channel channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel1.close();
		//AMQP-358
		verify(mockChannels.get(0), never()).close();

		con1.close(); // should be ignored, and placed into connection cache.
		verify(mockConnections.get(0), never()).close();
		assertThat(allocatedConnections).hasSize(1);
		assertThat(idleConnections).hasSize(1);
		assertThat(closedNotification.get()).isNull();

		/*
		 * will retrieve same connection that was just put into cache, and reuse single channel from cache as well
		 */
		Connection con2 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con2);
		Channel channel2 = con2.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel2);
		channel2.close();
		verify(mockChannels.get(0), never()).close();
		con2.close();
		verify(mockConnections.get(0), never()).close();
		assertThat(allocatedConnections).hasSize(1);
		assertThat(idleConnections).hasSize(1);
		assertThat(createNotification.get()).isNull();

		/*
		 * Now check for multiple connections/channels
		 */
		con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		con2 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(1), con2);
		channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel2 = con2.createChannel(false);
		verifyChannelIs(mockChannels.get(1), channel2);
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(0);
		assertThat(createNotification.get()).isNotNull();
		assertThat(createNotification.getAndSet(null)).isSameAs(mockConnections.get(1));

		// put mock1 in cache
		channel1.close();
		verify(mockChannels.get(1), never()).close();
		con1.close();
		verify(mockConnections.get(0), never()).close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		assertThat(closedNotification.get()).isNull();

		Connection con3 = ccf.createConnection();
		assertThat(createNotification.get()).isNull();
		verifyConnectionIs(mockConnections.get(0), con3);
		Channel channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel3);

		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(0);

		channel2.close();
		con2.close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		channel3.close();
		con3.close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(2);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("1");
		/*
		 *  Cache size is 1; con3 (mock1) should have been a real close.
		 *  con2 (mock2) should still be in the cache.
		 */
		verify(mockConnections.get(0)).close(30000);
		assertThat(closedNotification.get()).isNotNull();
		assertThat(closedNotification.getAndSet(null)).isSameAs(mockConnections.get(0));
		verify(mockChannels.get(1), never()).close();
		verify(mockConnections.get(1), never()).close(30000);
		verify(mockChannels.get(1), never()).close();
		verifyConnectionIs(mockConnections.get(1), idleConnections.iterator().next());
		/*
		 * Now a closed cached connection
		 */
		given(mockConnections.get(1).isOpen()).willReturn(false);
		given(mockChannels.get(1).isOpen()).willReturn(false);
		con3 = ccf.createConnection();
		assertThat(closedNotification.get()).isNotNull();
		assertThat(closedNotification.getAndSet(null)).isSameAs(mockConnections.get(1));
		verifyConnectionIs(mockConnections.get(2), con3);
		assertThat(createNotification.get()).isNotNull();
		assertThat(createNotification.getAndSet(null)).isSameAs(mockConnections.get(2));
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("1");
		channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(2), channel3);
		channel3.close();
		con3.close();
		assertThat(closedNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(2);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("1");

		/*
		 * Now a closed cached connection when creating a channel
		 */
		con3 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(2), con3);
		assertThat(createNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		given(mockConnections.get(2).isOpen()).willReturn(false);
		channel3 = con3.createChannel(false);
		assertThat(closedNotification.getAndSet(null)).isNotNull();
		assertThat(createNotification.getAndSet(null)).isNotNull();

		verifyChannelIs(mockChannels.get(3), channel3);
		channel3.close();
		con3.close();
		assertThat(closedNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(2);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("1");

		// destroy
		ccf.destroy();
		assertThat(closedNotification.get()).isNotNull();
		verify(mockConnections.get(3)).close(30000);
	}

	@Test
	public void testWithConnectionFactoryCachedConnectionAndChannels() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);

		final List<com.rabbitmq.client.Connection> mockConnections = new ArrayList<com.rabbitmq.client.Connection>();
		final List<Channel> mockChannels = new ArrayList<Channel>();

		AtomicInteger connectionNumber = new AtomicInteger();
		willAnswer(invocation -> {
			com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
			AtomicInteger channelNumber = new AtomicInteger();
			willAnswer(invocation1 -> {
				Channel channel = mock(Channel.class);
				given(channel.isOpen()).willReturn(true);
				int channelNum = channelNumber.incrementAndGet();
				given(channel.toString()).willReturn("mockChannel" + connectionNumber + ":" + channelNum);
				mockChannels.add(channel);
				return channel;
			}).given(connection).createChannel();
			int connectionNum = connectionNumber.incrementAndGet();
			given(connection.toString()).willReturn("mockConnection" + connectionNum);
			given(connection.isOpen()).willReturn(true);
			mockConnections.add(connection);
			return connection;
		}).given(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setConnectionCacheSize(2);
		ccf.setChannelCacheSize(2);
		ccf.afterPropertiesSet();

		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertThat(allocatedConnections).hasSize(0);
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertThat(idleConnections).hasSize(0);
		@SuppressWarnings("unchecked")
		Map<?, List<?>> cachedChannels = TestUtils.getPropertyValue(ccf, "allocatedConnectionNonTransactionalChannels",
				Map.class);

		final AtomicReference<com.rabbitmq.client.Connection> createNotification =
				new AtomicReference<com.rabbitmq.client.Connection>();
		final AtomicReference<com.rabbitmq.client.Connection> closedNotification =
				new AtomicReference<com.rabbitmq.client.Connection>();
		ccf.setConnectionListeners(Collections.singletonList(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				assertThat(createNotification.get()).isNull();
				createNotification.set(targetDelegate(connection));
			}

			@Override
			public void onClose(Connection connection) {
				assertThat(closedNotification.get()).isNull();
				closedNotification.set(targetDelegate(connection));
			}
		}));

		Connection con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		assertThat(allocatedConnections).hasSize(1);
		assertThat(idleConnections).hasSize(0);
		assertThat(createNotification.get()).isNotNull();
		assertThat(createNotification.getAndSet(null)).isSameAs(mockConnections.get(0));

		Channel channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel1.close();
		//AMQP-358
		verify(mockChannels.get(0), never()).close();

		con1.close(); // should be ignored, and placed into connection cache.
		verify(mockConnections.get(0), never()).close();
		assertThat(allocatedConnections).hasSize(1);
		assertThat(idleConnections).hasSize(1);
		assertThat(cachedChannels.get(con1)).hasSize(1);
		assertThat(closedNotification.get()).isNull();

		/*
		 * will retrieve same connection that was just put into cache, and reuse single channel from cache as well
		 */
		Connection con2 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con2);
		Channel channel2 = con2.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel2);
		channel2.close();
		verify(mockChannels.get(0), never()).close();
		con2.close();
		verify(mockConnections.get(0), never()).close();
		assertThat(allocatedConnections).hasSize(1);
		assertThat(idleConnections).hasSize(1);
		assertThat(createNotification.get()).isNull();

		/*
		 * Now check for multiple connections/channels
		 */
		con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		con2 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(1), con2);
		channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel2 = con2.createChannel(false);
		verifyChannelIs(mockChannels.get(1), channel2);
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(0);
		assertThat(createNotification.get()).isNotNull();
		assertThat(createNotification.getAndSet(null)).isSameAs(mockConnections.get(1));

		// put mock1 in cache
		channel1.close();
		verify(mockChannels.get(1), never()).close();
		con1.close();
		verify(mockConnections.get(0), never()).close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		assertThat(closedNotification.get()).isNull();

		Connection con3 = ccf.createConnection();
		assertThat(createNotification.get()).isNull();
		verifyConnectionIs(mockConnections.get(0), con3);
		Channel channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel3);

		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(0);

		channel2.close();
		con2.close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		channel3.close();
		con3.close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(2);
		assertThat(cachedChannels.get(con1)).hasSize(1);
		assertThat(cachedChannels.get(con2)).hasSize(1);
		/*
		 *  Cache size is 2; neither should have been a real close.
		 *  con2 (mock2) and con1 should still be in the cache.
		 */
		verify(mockConnections.get(0), never()).close(30000);
		assertThat(closedNotification.get()).isNull();
		verify(mockChannels.get(1), never()).close();
		verify(mockConnections.get(1), never()).close(30000);
		verify(mockChannels.get(1), never()).close();
		assertThat(idleConnections).hasSize(2);
		Iterator<?> iterator = idleConnections.iterator();
		verifyConnectionIs(mockConnections.get(1), iterator.next());
		verifyConnectionIs(mockConnections.get(0), iterator.next());
		/*
		 * Now a closed cached connection
		 */
		given(mockConnections.get(1).isOpen()).willReturn(false);
		con3 = ccf.createConnection();
		assertThat(closedNotification.get()).isNotNull();
		assertThat(closedNotification.getAndSet(null)).isSameAs(mockConnections.get(1));
		verifyConnectionIs(mockConnections.get(0), con3);
		assertThat(createNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel3);
		channel3.close();
		con3.close();
		assertThat(closedNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(2);

		/*
		 * Now a closed cached connection when creating a channel
		 */
		con3 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con3);
		assertThat(createNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(1);
		given(mockConnections.get(0).isOpen()).willReturn(false);
		channel3 = con3.createChannel(false);
		assertThat(closedNotification.getAndSet(null)).isNotNull();
		assertThat(createNotification.getAndSet(null)).isNotNull();

		verifyChannelIs(mockChannels.get(2), channel3);
		channel3.close();
		con3.close();
		assertThat(closedNotification.get()).isNull();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(idleConnections).hasSize(2);

		Connection con4 = ccf.createConnection();
		assertThat(con4).isSameAs(con3);
		assertThat(idleConnections).hasSize(1);
		Channel channelA = con4.createChannel(false);
		Channel channelB = con4.createChannel(false);
		Channel channelC = con4.createChannel(false);
		channelA.close();
		assertThat(cachedChannels.get(con4)).hasSize(1);
		channelB.close();
		assertThat(cachedChannels.get(con4)).hasSize(2);
		channelC.close();
		assertThat(cachedChannels.get(con4)).hasSize(2);

		// destroy
		ccf.destroy();
		assertThat(closedNotification.get()).isNotNull();
		// physical wasn't invoked, because this mockConnection marked with 'false' for 'isOpen()'
		verify(mockConnections.get(0)).close(30000);
		verify(mockConnections.get(1)).close(30000);
		verify(mockConnections.get(2)).close(30000);
	}

	@Test
	public void testWithConnectionFactoryCachedConnectionIdleAreClosed() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);

		final List<com.rabbitmq.client.Connection> mockConnections = new ArrayList<com.rabbitmq.client.Connection>();
		final List<Channel> mockChannels = new ArrayList<Channel>();

		AtomicInteger connectionNumber = new AtomicInteger();
		willAnswer(invocation -> {
			com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
			AtomicInteger channelNumber = new AtomicInteger();
			willAnswer(invocation1 -> {
				Channel channel = mock(Channel.class);
				given(channel.isOpen()).willReturn(true);
				int channelNum = channelNumber.incrementAndGet();
				given(channel.toString()).willReturn("mockChannel" + channelNum);
				mockChannels.add(channel);
				return channel;
			}).given(connection).createChannel();
			int connectionNum = connectionNumber.incrementAndGet();
			given(connection.toString()).willReturn("mockConnection" + connectionNum);
			given(connection.isOpen()).willReturn(true);
			mockConnections.add(connection);
			return connection;
		}).given(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setConnectionCacheSize(5);
		ccf.afterPropertiesSet();

		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertThat(allocatedConnections).hasSize(0);
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertThat(idleConnections).hasSize(0);

		Connection conn1 = ccf.createConnection();
		Connection conn2 = ccf.createConnection();
		Connection conn3 = ccf.createConnection();
		assertThat(allocatedConnections).hasSize(3);
		assertThat(idleConnections).hasSize(0);
		conn1.close();
		conn2.close();
		conn3.close();
		assertThat(allocatedConnections).hasSize(3);
		assertThat(idleConnections).hasSize(3);

		given(mockConnections.get(0).isOpen()).willReturn(false);
		given(mockConnections.get(1).isOpen()).willReturn(false);
		Connection conn4 = ccf.createConnection();
		assertThat(allocatedConnections).hasSize(3);
		assertThat(idleConnections).hasSize(2);
		assertThat(conn4).isSameAs(conn3);
		conn4.close();
		assertThat(allocatedConnections).hasSize(3);
		assertThat(idleConnections).hasSize(3);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("1");

		ccf.destroy();
		assertThat(allocatedConnections).hasSize(3);
		assertThat(idleConnections).hasSize(3);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("0");
	}

	@Test
	public void testConsumerChannelPhysicallyClosedWhenNotIsOpen() throws Exception {
		testConsumerChannelPhysicallyClosedWhenNotIsOpenGuts(false);
	}

	@Test
	public void testConsumerChannelWithPubConfPhysicallyClosedWhenNotIsOpen() throws Exception {
		testConsumerChannelPhysicallyClosedWhenNotIsOpenGuts(true);
	}

	private void testConsumerChannelPhysicallyClosedWhenNotIsOpenGuts(boolean confirms) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
			com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
			Channel mockChannel = mock(Channel.class);

			given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
			given(mockConnection.createChannel()).willReturn(mockChannel);
			given(mockChannel.isOpen()).willReturn(true);
			given(mockConnection.isOpen()).willReturn(true);

			CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
			ccf.setExecutor(executor);
			if (confirms) {
				ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
			}
			Connection con = ccf.createConnection();

			Channel channel = con.createChannel(false);
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			given(mockChannel.isOpen()).willReturn(true);
			final CountDownLatch physicalCloseLatch = new CountDownLatch(1);
			willAnswer(i -> {
				physicalCloseLatch.countDown();
				return null;
			}).given(mockChannel).close();
			channel.close();
			RabbitUtils.setPhysicalCloseRequired(channel, false);
			con.close(); // should be ignored

			assertThat(physicalCloseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			executor.shutdownNow();
		}
	}

	private void verifyConnectionIs(com.rabbitmq.client.Connection mockConnection, Object con) {
		assertThat(targetDelegate(con)).isSameAs(mockConnection);
	}

	private com.rabbitmq.client.Connection targetDelegate(Object con) {
		return TestUtils.getPropertyValue(con, "target.delegate",
				com.rabbitmq.client.Connection.class);
	}

	private void verifyChannelIs(Channel mockChannel, Channel channel) {
		ChannelProxy proxy = (ChannelProxy) channel;
		assertThat(proxy.getTargetChannel()).isSameAs(mockChannel);
	}

	@Test
	public void setAddressesEmpty() throws Exception {
		ConnectionFactory mock = mock(com.rabbitmq.client.ConnectionFactory.class);
		given(mock.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(mock(com.rabbitmq.client.Connection.class));
		CachingConnectionFactory ccf = new CachingConnectionFactory(mock);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setHost("abc");
		ccf.setAddresses("");
		ccf.createConnection();
		verify(mock).isAutomaticRecoveryEnabled();
		verify(mock).setHost("abc");
		Log logger = TestUtils.getPropertyValue(ccf, "logger", Log.class);
		if (logger.isInfoEnabled()) {
			verify(mock).getHost();
			verify(mock).getPort();
		}
		verify(mock).newConnection(any(ExecutorService.class), anyString());
		verifyNoMoreInteractions(mock);
	}

	@Test
	public void setAddressesOneHost() throws Exception {
		ConnectionFactory mock = mock(com.rabbitmq.client.ConnectionFactory.class);
		given(mock.newConnection(any(), anyList(), anyString()))
				.willReturn(mock(com.rabbitmq.client.Connection.class));
		CachingConnectionFactory ccf = new CachingConnectionFactory(mock);
		ccf.setAddresses("mq1");
		ccf.createConnection();
		verify(mock).isAutomaticRecoveryEnabled();
		verify(mock)
				.newConnection(isNull(), eq(Collections.singletonList(new Address("mq1"))), anyString());
		verifyNoMoreInteractions(mock);
	}

	@Test
	public void setAddressesTwoHosts() throws Exception {
		ConnectionFactory mock = mock();
		willReturn(true).given(mock).isAutomaticRecoveryEnabled();
		willReturn(mock(com.rabbitmq.client.Connection.class)).given(mock).newConnection(any(), anyList(), anyString());
		CachingConnectionFactory ccf = new CachingConnectionFactory(mock);
		ccf.setAddresses("mq1,mq2");
		ccf.createConnection();
		verify(mock).isAutomaticRecoveryEnabled();
		verify(mock).setAutomaticRecoveryEnabled(false);
		verify(mock).newConnection(
				isNull(),
				argThat((ArgumentMatcher<List<Address>>) a -> a.size() == 2
						&& a.contains(new Address("mq1")) && a.contains(new Address("mq2"))),
				anyString());
		verifyNoMoreInteractions(mock);
	}

	@Test
	public void setUri() throws Exception {
		URI uri = new URI("amqp://localhost:1234/%2f");

		ConnectionFactory mock = mock();
		given(mock.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(mock(com.rabbitmq.client.Connection.class));
		CachingConnectionFactory ccf = new CachingConnectionFactory(mock);
		ccf.setExecutor(mock(ExecutorService.class));

		ccf.setUri(uri);
		ccf.createConnection();

		InOrder order = inOrder(mock);
		order.verify(mock).isAutomaticRecoveryEnabled();
		order.verify(mock).setUri(uri);
		Log logger = TestUtils.getPropertyValue(ccf, "logger", Log.class);
		if (logger.isInfoEnabled()) {
			order.verify(mock).getHost();
			order.verify(mock).getPort();
		}
		order.verify(mock).newConnection(any(ExecutorService.class), anyString());
		verifyNoMoreInteractions(mock);
	}

	@Test
	public void testChannelCloseIdempotency() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true).willReturn(false);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		Connection con = ccf.createConnection();

		Channel channel = con.createChannel(false);
//		RabbitUtils.setPhysicalCloseRequired(true);
		channel.close(); // should be ignored, and placed into channel cache.
		channel.close(); // physically closed, so remove from the cache.
		channel.close(); // physically closed and removed from the cache  before, so void "close".
		Channel channel2 = con.createChannel(false);
		assertThat(channel2).isNotSameAs(channel);
	}

	@Test
	@Disabled // Test to verify log message is suppressed after patch to CCF
	public void testReturnsNormalCloseDeferredClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		willThrow(new ShutdownSignalException(true, false, new com.rabbitmq.client.AMQP.Connection.Close.Builder()
				.replyCode(200)
				.replyText("OK")
				.build(), null)).given(mockChannel).close();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setPublisherReturns(true);
		ccf.setExecutor(Executors.newSingleThreadExecutor());
		Connection conn = ccf.createConnection();
		Channel channel = conn.createChannel(false);
		RabbitUtils.setPhysicalCloseRequired(channel, true);
		channel.close();
		RabbitUtils.setPhysicalCloseRequired(channel, false);
		Thread.sleep(6000);
	}

	@Test
	public void testOrderlyShutDown() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection((ExecutorService) isNull(), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ApplicationContext ac = mock(ApplicationContext.class);
		ccf.setApplicationContext(ac);
		PublisherCallbackChannel pcc = mock(PublisherCallbackChannel.class);
		given(pcc.isOpen()).willReturn(true);
		CountDownLatch asyncClosingLatch = new CountDownLatch(1);
		willAnswer(invoc -> {
			asyncClosingLatch.countDown();
			return null;
		}).given(pcc).waitForConfirmsOrDie(anyLong());
		AtomicReference<ExecutorService> executor = new AtomicReference<>();
		AtomicBoolean rejected = new AtomicBoolean(true);
		CountDownLatch closeLatch = new CountDownLatch(1);
		ccf.setPublisherChannelFactory((channel, exec) -> {
			executor.set(exec);
			return pcc;
		});
		willAnswer(invoc -> {
			try {
				executor.get().execute(() -> {
				});
				rejected.set(false);
			}
			catch (@SuppressWarnings("unused") RejectedExecutionException e) {
				rejected.set(true);
			}
			closeLatch.countDown();
			return null;
		}).given(pcc).close();
		Channel channel = ccf.createConnection().createChannel(false);
		ExecutorService closeExec = Executors.newSingleThreadExecutor();
		closeExec.execute(() -> {
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			try {
				channel.close();
			}
			catch (@SuppressWarnings("unused") IOException | TimeoutException e) {
				// ignore
			}
		});
		assertThat(asyncClosingLatch.await(10, TimeUnit.SECONDS)).isTrue();
		ccf.onApplicationEvent(new ContextClosedEvent(ac));
		ccf.destroy();
		assertThat(closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rejected.get()).isFalse();
		closeExec.shutdownNow();

		ccf.setChannelCacheSize(42);
		assertThat(ccf.getChannelCacheSize()).isEqualTo(42);
		assertThat(((CachingConnectionFactory) ccf.getPublisherConnectionFactory()).getChannelCacheSize())
				.isEqualTo(42);
		ccf.setPublisherConnectionFactory(new CachingConnectionFactory());
		ccf.setChannelCacheSize(42);
		assertThat(ccf.getChannelCacheSize()).isEqualTo(42);
		assertThat(((CachingConnectionFactory) ccf.getPublisherConnectionFactory()).getChannelCacheSize())
				.isEqualTo(25);
	}

	@Test
	public void testFirstConnectionDoesntWait() throws IOException, TimeoutException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection((ExecutorService) isNull(), anyString())).willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setChannelCheckoutTimeout(60000);
		long t1 = System.currentTimeMillis();
		ccf.createConnection();
		assertThat(System.currentTimeMillis() - t1).isLessThan(30_000);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testShuffleRandom() throws IOException, TimeoutException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection = mock();
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(), anyList(), anyString()))
				.willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setAddresses("host1:5672,host2:5672,host3:5672");
		ccf.setAddressShuffleMode(AddressShuffleMode.RANDOM);
		IntStream.range(0, 100).forEach(i -> ccf.createConnection());
		ccf.destroy();
		ArgumentCaptor<List<Address>> captor = ArgumentCaptor.forClass(List.class);
		verify(mockConnectionFactory, times(100)).newConnection(isNull(), captor.capture(), anyString());
		List<String> firstAddress = captor.getAllValues()
				.stream()
				.map(addresses -> addresses.get(0).getHost())
				.distinct()
				.sorted()
				.collect(Collectors.toList());
		assertThat(firstAddress).containsExactly("host1", "host2", "host3");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testShuffleInOrder() throws IOException, TimeoutException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(isNull(), anyList(), anyString()))
				.willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setAddresses("host1:5672,host2:5672,host3:5672");
		ccf.setAddressShuffleMode(AddressShuffleMode.INORDER);
		IntStream.range(0, 3).forEach(i -> ccf.createConnection());
		ccf.destroy();
		ArgumentCaptor<List<Address>> captor = ArgumentCaptor.forClass(List.class);
		verify(mockConnectionFactory, times(3)).newConnection(isNull(), captor.capture(), anyString());
		List<String> connectAddresses = captor.getAllValues()
				.stream()
				.map(addresses -> addresses.get(0).getHost())
				.collect(Collectors.toList());
		assertThat(connectAddresses).containsExactly("host1", "host2", "host3");
	}

	@Test
	void testResolver() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection = mock();
		Channel mockChannel = mock();

		AddressResolver resolver = () -> Collections.singletonList(Address.parseAddress("foo:5672"));
		given(mockConnectionFactory.newConnection(any(ExecutorService.class), eq(resolver), anyString()))
				.willReturn(mockConnection);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.isOpen()).willReturn(true);
		given(mockConnection.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setAddressResolver(resolver);
		Connection con = ccf.createConnection();
		assertThat(con).isNotNull();
		assertThat(TestUtils.getPropertyValue(con, "target", SimpleConnection.class).getDelegate())
				.isEqualTo(mockConnection);
		verify(mockConnectionFactory).newConnection(any(ExecutorService.class), eq(resolver), anyString());
	}

	@Test
	void nullShutdownCause() {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		AbstractConnectionFactory cf = createConnectionFactory(mockConnectionFactory);
		AtomicBoolean connShutDown = new AtomicBoolean();
		cf.addConnectionListener(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
			}

			@Override
			public void onShutDown(ShutdownSignalException signal) {
				connShutDown.set(true);
			}

		});
		AtomicBoolean chanShutDown = new AtomicBoolean();
		cf.addChannelListener(new ChannelListener() {

			@Override
			public void onCreate(Channel channel, boolean transactional) {
			}

			@Override
			public void onShutDown(ShutdownSignalException signal) {
				chanShutDown.set(true);
			}

		});
		cf.shutdownCompleted(new ShutdownSignalException(false, false, null, chanShutDown));
		assertThat(connShutDown.get()).isTrue();
		assertThat(chanShutDown.get()).isFalse();
	}

	@Test
	void isPublisherConfirmsHandlesSimple() {
		CachingConnectionFactory ccf = new CachingConnectionFactory("someHost", 1234);
		ccf.setPublisherConfirmType(ConfirmType.SIMPLE);

		assertThat(ccf.isPublisherConfirms()).isFalse();
	}

	@Test
	void isPublisherConfirmsHandlesCorrelated() {
		CachingConnectionFactory ccf = new CachingConnectionFactory("someHost", 1234);
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);

		assertThat(ccf.isPublisherConfirms()).isTrue();
	}

	@Test
	void isPublisherConfirmsHandlesNone() {
		CachingConnectionFactory ccf = new CachingConnectionFactory("someHost", 1234);
		ccf.setPublisherConfirmType(ConfirmType.NONE);

		assertThat(ccf.isPublisherConfirms()).isFalse();
	}

}
