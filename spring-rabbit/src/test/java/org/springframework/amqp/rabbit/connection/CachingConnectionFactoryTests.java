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

package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.test.util.ReflectionTestUtils;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

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
	public void testWithConnectionFactoryDefaults() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockConnection.isOpen()).thenReturn(true);

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

		assertSame(con, con2);
		assertSame(channel, channel2);
		verify(mockConnection, never()).close();
		verify(mockChannel, never()).close();
	}

	@Test
	public void testPublisherConnection() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockConnection.isOpen()).thenReturn(true);

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

		assertSame(con, con2);
		assertSame(channel, channel2);
		verify(mockConnection, never()).close();
		verify(mockChannel, never()).close();

		assertNull(TestUtils.getPropertyValue(ccf, "connection.target"));
		assertNotNull(TestUtils.getPropertyValue(ccf, "publisherConnectionFactory.connection.target"));
		assertSame(con, TestUtils.getPropertyValue(ccf, "publisherConnectionFactory.connection"));
	}

	@Test
	public void testWithConnectionFactoryCacheSize() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockTxChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel1, mockChannel2, mockTxChannel);

		when(mockChannel1.basicGet("foo", false)).thenReturn(new GetResponse(null, null, null, 1));
		when(mockChannel2.basicGet("bar", false)).thenReturn(new GetResponse(null, null, null, 1));
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(2);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		Channel channel2 = con.createChannel(false);

		ChannelProxy txChannel = (ChannelProxy) con.createChannel(true);
		assertTrue(txChannel.isTransactional());
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

		assertNotSame(ch1, ch2);
		assertSame(ch1, channel1);
		assertSame(ch2, channel2);

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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2).thenReturn(mockChannel3);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);
		when(mockChannel3.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		// cache size is 1, but the other connection is not released yet so this
		// creates a new one
		Channel channel2 = con.createChannel(false);
		assertNotSame(channel1, channel2);

		// should be ignored, and added last into channel cache.
		channel1.close();
		// should be physically closed
		channel2.close();

		// remove first entry in cache (channel1)
		Channel ch1 = con.createChannel(false);
		// create a new channel
		Channel ch2 = con.createChannel(false);

		assertNotSame(ch1, ch2);
		assertSame(ch1, channel1);
		assertNotSame(ch2, channel2);

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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);

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

		assertSame(ch1, channel1);

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

		when(mockConnection.createChannel()).thenReturn(mockChannel1);

		doAnswer(i -> !brokerDown.get()).when(mockConnection).isOpen();

		// Called during physical close
		doAnswer(i -> !brokerDown.get()).when(mockChannel1).isOpen();

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

		assertSame(ch1, channel1);

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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);

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
		assertSame(con1, con2);

		final CountDownLatch latch2 = new CountDownLatch(1);
		final CountDownLatch latch1 = new CountDownLatch(1);
		final AtomicReference<Connection> connection = new AtomicReference<Connection>();
		ccf.setChannelCheckoutTimeout(30000);
		Executors.newSingleThreadExecutor().execute(() -> {
			latch1.countDown();
			connection.set(ccf.createConnection());
			latch2.countDown();
		});

		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		Thread.sleep(100);
		con2.close();
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		assertSame(con2, connection.get());

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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.thenReturn(mockConnection1, mockConnection2);
		when(mockConnection1.createChannel()).thenReturn(mockChannel1, mockChannel2);
		when(mockConnection1.isOpen()).thenReturn(true);
		when(mockConnection2.createChannel()).thenReturn(mockChannel3, mockChannel4);
		when(mockConnection2.isOpen()).thenReturn(true);

		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);
		when(mockChannel3.isOpen()).thenReturn(true);
		when(mockChannel4.isOpen()).thenReturn(true);

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
		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());
		channel1.close();
		con.close();

		assertEquals(2,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		when(mockConnection1.isOpen()).thenReturn(false);
		when(mockChannel1.isOpen()).thenReturn(false);
		when(mockChannel2.isOpen()).thenReturn(false);

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

		assertEquals(2,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		ccf.destroy();

		assertEquals(2,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

	}

	@Test
	public void testCheckoutLimitWithRelease() throws IOException, Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);

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

		assertTrue(latch.await(10, TimeUnit.SECONDS));
		Channel channel2 = con.createChannel(false);
		assertSame(channelOne.get(), channel2);

		channel2.close();

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();

		ccf.destroy();
	}

	@Test
	public void testReleaseWithForcedPhysicalClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection1);
		when(mockConnection1.createChannel()).thenReturn(mockChannel1);
		when(mockConnection1.isOpen()).thenReturn(true);

		when(mockChannel1.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		assertEquals(0,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());
		channel1.close();
		con.close();

		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		channel1 = con.createChannel(false);
		RabbitUtils.setPhysicalCloseRequired(channel1, true);
		assertEquals(0,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		channel1.close();
		RabbitUtils.setPhysicalCloseRequired(channel1, false);
		con.close();
		verify(mockChannel1).close();
		verify(mockConnection1, never()).close();

		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		ccf.destroy();

		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

	}

	@Test
	public void testDoubleLogicalClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection1);
		when(mockConnection1.createChannel()).thenReturn(mockChannel1);
		when(mockConnection1.isOpen()).thenReturn(true);

		when(mockChannel1.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);
		ccf.setChannelCheckoutTimeout(10);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		assertEquals(0,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());
		channel1.close();

		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		channel1.close(); // double close of proxy

		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

		con.close();
		verify(mockChannel1, never()).close();
		verify(mockConnection1, never()).close();

		ccf.destroy();

		assertEquals(1,
				((Semaphore) TestUtils.getPropertyValue(ccf, "checkoutPermits", Map.class).values().iterator().next())
						.availablePermits());

	}

	@Test
	public void testCacheSizeExceededAfterClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		channel1.close(); // should be ignored, and add last into channel cache.
		Channel channel2 = con.createChannel(false);
		channel2.close(); // should be ignored, and add last into channel cache.
		assertSame(channel1, channel2);

		Channel ch1 = con.createChannel(false); // remove first entry in cache
		// (channel1)
		Channel ch2 = con.createChannel(false); // create new channel

		assertNotSame(ch1, ch2);
		assertSame(ch1, channel1);
		assertNotSame(ch2, channel2);

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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

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
		assertNotSame(channel1, channel2);

		Channel ch1 = con.createChannel(true); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(false); // create new channel

		assertNotSame(ch1, ch2);
		assertSame(ch1, channel1); // The non-transactional one
		assertSame(ch2, channel2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(2)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, never()).close();

		@SuppressWarnings("unchecked")
		List<Channel> notxlist = (List<Channel>) ReflectionTestUtils.getField(ccf, "cachedChannelsNonTransactional");
		assertEquals(1, notxlist.size());
		@SuppressWarnings("unchecked")
		List<Channel> txlist = (List<Channel>) ReflectionTestUtils.getField(ccf, "cachedChannelsTransactional");
		assertEquals(1, txlist.size());

	}

	@Test
	public void testWithConnectionFactoryDestroy() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);

		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockChannel3 = mock(Channel.class);

		assertNotSame(mockChannel1, mockChannel2);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection1, mockConnection2);
		when(mockConnection1.createChannel()).thenReturn(mockChannel1, mockChannel2);
		when(mockConnection1.isOpen()).thenReturn(true);
		when(mockConnection2.createChannel()).thenReturn(mockChannel3);
		when(mockConnection2.isOpen()).thenReturn(true);
		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);
		when(mockChannel3.isOpen()).thenReturn(true);

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

		assertSame(ch1, channel1);
		assertSame(ch2, channel2);

		Channel target1 = ((ChannelProxy) ch1).getTargetChannel();
		Channel target2 = ((ChannelProxy) ch2).getTargetChannel();

		// make sure mokito returned different mocks for the channel
		assertNotSame(target1, target2);

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
		assertNotSame(conDelegate, targetDelegate(con1));

		// This will return a proxy that surpresses calls to close
		Channel channel3 = con.createChannel(false);
		assertNotSame(channel3, channel1);
		assertNotSame(channel3, channel2);
	}

	@Test
	public void testWithChannelListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

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
		assertEquals(1, called.get());
		channel.close();

		con.close();
		verify(mockConnection, never()).close();

		connectionFactory.createConnection();
		con.createChannel(false);
		assertEquals(1, called.get());

		connectionFactory.destroy();
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

	}

	@Test
	public void testWithConnectionListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		when(mockConnection1.toString()).thenReturn("conn1");
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);
		when(mockConnection2.toString()).thenReturn("conn2");
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection1, mockConnection2);
		when(mockConnection1.isOpen()).thenReturn(true);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockConnection1.createChannel()).thenReturn(mockChannel);
		when(mockConnection2.createChannel()).thenReturn(mockChannel);

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
		assertSame(con, created.get());
		channel.close();

		con.close();
		verify(mockConnection1, never()).close();

		Connection same = connectionFactory.createConnection();
		channel = con.createChannel(false);
		assertSame(con, same);
		channel.close();
		com.rabbitmq.client.Connection conDelegate = targetDelegate(con);

		when(mockConnection1.isOpen()).thenReturn(false);
		when(mockChannel.isOpen()).thenReturn(false); // force a connection refresh
		channel.basicCancel("foo");
		channel.close();
		assertEquals(1, timesClosed.get());

		Connection notSame = connectionFactory.createConnection();
		assertNotSame(conDelegate, targetDelegate(notSame));
		assertSame(con, closed.get());
		assertSame(notSame, created.get());

		connectionFactory.destroy();
		verify(mockConnection2, atLeastOnce()).close(anyInt());
		assertSame(notSame, closed.get());
		assertEquals(2, timesClosed.get());

		verify(mockConnectionFactory, times(2)).newConnection(any(ExecutorService.class), anyString());
	}

	@Test
	public void testWithConnectionFactoryCachedConnection() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);

		final List<com.rabbitmq.client.Connection> mockConnections = new ArrayList<com.rabbitmq.client.Connection>();
		final List<Channel> mockChannels = new ArrayList<Channel>();

		doAnswer(new Answer<com.rabbitmq.client.Connection>() {

			private int connectionNumber;

			@Override
			public com.rabbitmq.client.Connection answer(InvocationOnMock invocation) throws Throwable {
				com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
				doAnswer(new Answer<Channel>() {

					private int channelNumber;

					@Override
					public Channel answer(InvocationOnMock invocation) throws Throwable {
						Channel channel = mock(Channel.class);
						when(channel.isOpen()).thenReturn(true);
						int channelNumnber = ++this.channelNumber;
						when(channel.toString()).thenReturn("mockChannel" + connectionNumber + ":" + channelNumnber);
						mockChannels.add(channel);
						return channel;
					}
				}).when(connection).createChannel();
				int connectionNumber = ++this.connectionNumber;
				when(connection.toString()).thenReturn("mockConnection" + connectionNumber);
				when(connection.isOpen()).thenReturn(true);
				mockConnections.add(connection);
				return connection;
			}
		}).when(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.afterPropertiesSet();

		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertEquals(0, allocatedConnections.size());
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertEquals(0, idleConnections.size());

		final AtomicReference<com.rabbitmq.client.Connection> createNotification =
				new AtomicReference<com.rabbitmq.client.Connection>();
		final AtomicReference<com.rabbitmq.client.Connection> closedNotification =
				new AtomicReference<com.rabbitmq.client.Connection>();
		ccf.setConnectionListeners(Collections.singletonList(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				assertNull(createNotification.get());
				createNotification.set(targetDelegate(connection));
			}

			@Override
			public void onClose(Connection connection) {
				assertNull(closedNotification.get());
				closedNotification.set(targetDelegate(connection));
			}
		}));

		Connection con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		assertEquals(1, allocatedConnections.size());
		assertEquals(0, idleConnections.size());
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(0), createNotification.getAndSet(null));

		Channel channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel1.close();
		//AMQP-358
		verify(mockChannels.get(0), never()).close();

		con1.close(); // should be ignored, and placed into connection cache.
		verify(mockConnections.get(0), never()).close();
		assertEquals(1, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(closedNotification.get());

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
		assertEquals(1, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(createNotification.get());

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
		assertEquals(2, allocatedConnections.size());
		assertEquals(0, idleConnections.size());
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(1), createNotification.getAndSet(null));

		// put mock1 in cache
		channel1.close();
		verify(mockChannels.get(1), never()).close();
		con1.close();
		verify(mockConnections.get(0), never()).close();
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(closedNotification.get());

		Connection con3 = ccf.createConnection();
		assertNull(createNotification.get());
		verifyConnectionIs(mockConnections.get(0), con3);
		Channel channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel3);

		assertEquals(2, allocatedConnections.size());
		assertEquals(0, idleConnections.size());

		channel2.close();
		con2.close();
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		channel3.close();
		con3.close();
		assertEquals(2, allocatedConnections.size());
		assertEquals(2, idleConnections.size());
		assertEquals("1", ccf.getCacheProperties().get("openConnections"));
		/*
		 *  Cache size is 1; con3 (mock1) should have been a real close.
		 *  con2 (mock2) should still be in the cache.
		 */
		verify(mockConnections.get(0)).close(30000);
		assertNotNull(closedNotification.get());
		assertSame(mockConnections.get(0), closedNotification.getAndSet(null));
		verify(mockChannels.get(1), never()).close();
		verify(mockConnections.get(1), never()).close(30000);
		verify(mockChannels.get(1), never()).close();
		verifyConnectionIs(mockConnections.get(1), idleConnections.iterator().next());
		/*
		 * Now a closed cached connection
		 */
		when(mockConnections.get(1).isOpen()).thenReturn(false);
		when(mockChannels.get(1).isOpen()).thenReturn(false);
		con3 = ccf.createConnection();
		assertNotNull(closedNotification.get());
		assertSame(mockConnections.get(1), closedNotification.getAndSet(null));
		verifyConnectionIs(mockConnections.get(2), con3);
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(2), createNotification.getAndSet(null));
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertEquals("1", ccf.getCacheProperties().get("openConnections"));
		channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(2), channel3);
		channel3.close();
		con3.close();
		assertNull(closedNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(2, idleConnections.size());
		assertEquals("1", ccf.getCacheProperties().get("openConnections"));

		/*
		 * Now a closed cached connection when creating a channel
		 */
		con3 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(2), con3);
		assertNull(createNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		when(mockConnections.get(2).isOpen()).thenReturn(false);
		channel3 = con3.createChannel(false);
		assertNotNull(closedNotification.getAndSet(null));
		assertNotNull(createNotification.getAndSet(null));

		verifyChannelIs(mockChannels.get(3), channel3);
		channel3.close();
		con3.close();
		assertNull(closedNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(2, idleConnections.size());
		assertEquals("1", ccf.getCacheProperties().get("openConnections"));

		// destroy
		ccf.destroy();
		assertNotNull(closedNotification.get());
		verify(mockConnections.get(3)).close(30000);
	}


	@Test
	public void testWithConnectionFactoryCachedConnectionAndChannels() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);

		final List<com.rabbitmq.client.Connection> mockConnections = new ArrayList<com.rabbitmq.client.Connection>();
		final List<Channel> mockChannels = new ArrayList<Channel>();

		doAnswer(new Answer<com.rabbitmq.client.Connection>() {

			private int connectionNumber;

			@Override
			public com.rabbitmq.client.Connection answer(InvocationOnMock invocation) throws Throwable {
				com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
				doAnswer(new Answer<Channel>() {

					private int channelNumber;

					@Override
					public Channel answer(InvocationOnMock invocation) throws Throwable {
						Channel channel = mock(Channel.class);
						when(channel.isOpen()).thenReturn(true);
						int channelNumnber = ++this.channelNumber;
						when(channel.toString()).thenReturn("mockChannel" + connectionNumber + ":" + channelNumnber);
						mockChannels.add(channel);
						return channel;
					}
				}).when(connection).createChannel();
				int connectionNumber = ++this.connectionNumber;
				when(connection.toString()).thenReturn("mockConnection" + connectionNumber);
				when(connection.isOpen()).thenReturn(true);
				mockConnections.add(connection);
				return connection;
			}
		}).when(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setConnectionCacheSize(2);
		ccf.setChannelCacheSize(2);
		ccf.afterPropertiesSet();

		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertEquals(0, allocatedConnections.size());
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertEquals(0, idleConnections.size());
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
				assertNull(createNotification.get());
				createNotification.set(targetDelegate(connection));
			}

			@Override
			public void onClose(Connection connection) {
				assertNull(closedNotification.get());
				closedNotification.set(targetDelegate(connection));
			}
		}));

		Connection con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		assertEquals(1, allocatedConnections.size());
		assertEquals(0, idleConnections.size());
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(0), createNotification.getAndSet(null));

		Channel channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel1.close();
		//AMQP-358
		verify(mockChannels.get(0), never()).close();

		con1.close(); // should be ignored, and placed into connection cache.
		verify(mockConnections.get(0), never()).close();
		assertEquals(1, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertEquals(1, cachedChannels.get(con1).size());
		assertNull(closedNotification.get());

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
		assertEquals(1, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(createNotification.get());

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
		assertEquals(2, allocatedConnections.size());
		assertEquals(0, idleConnections.size());
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(1), createNotification.getAndSet(null));

		// put mock1 in cache
		channel1.close();
		verify(mockChannels.get(1), never()).close();
		con1.close();
		verify(mockConnections.get(0), never()).close();
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(closedNotification.get());

		Connection con3 = ccf.createConnection();
		assertNull(createNotification.get());
		verifyConnectionIs(mockConnections.get(0), con3);
		Channel channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel3);

		assertEquals(2, allocatedConnections.size());
		assertEquals(0, idleConnections.size());

		channel2.close();
		con2.close();
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		channel3.close();
		con3.close();
		assertEquals(2, allocatedConnections.size());
		assertEquals(2, idleConnections.size());
		assertEquals(1, cachedChannels.get(con1).size());
		assertEquals(1, cachedChannels.get(con2).size());
		/*
		 *  Cache size is 2; neither should have been a real close.
		 *  con2 (mock2) and con1 should still be in the cache.
		 */
		verify(mockConnections.get(0), never()).close(30000);
		assertNull(closedNotification.get());
		verify(mockChannels.get(1), never()).close();
		verify(mockConnections.get(1), never()).close(30000);
		verify(mockChannels.get(1), never()).close();
		assertEquals(2, idleConnections.size());
		Iterator<?> iterator = idleConnections.iterator();
		verifyConnectionIs(mockConnections.get(1), iterator.next());
		verifyConnectionIs(mockConnections.get(0), iterator.next());
		/*
		 * Now a closed cached connection
		 */
		when(mockConnections.get(1).isOpen()).thenReturn(false);
		con3 = ccf.createConnection();
		assertNotNull(closedNotification.get());
		assertSame(mockConnections.get(1), closedNotification.getAndSet(null));
		verifyConnectionIs(mockConnections.get(0), con3);
		assertNull(createNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel3);
		channel3.close();
		con3.close();
		assertNull(closedNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(2, idleConnections.size());

		/*
		 * Now a closed cached connection when creating a channel
		 */
		con3 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con3);
		assertNull(createNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(1, idleConnections.size());
		when(mockConnections.get(0).isOpen()).thenReturn(false);
		channel3 = con3.createChannel(false);
		assertNotNull(closedNotification.getAndSet(null));
		assertNotNull(createNotification.getAndSet(null));

		verifyChannelIs(mockChannels.get(2), channel3);
		channel3.close();
		con3.close();
		assertNull(closedNotification.get());
		assertEquals(2, allocatedConnections.size());
		assertEquals(2, idleConnections.size());

		Connection con4 = ccf.createConnection();
		assertSame(con3, con4);
		assertEquals(1, idleConnections.size());
		Channel channelA = con4.createChannel(false);
		Channel channelB = con4.createChannel(false);
		Channel channelC = con4.createChannel(false);
		channelA.close();
		assertEquals(1, cachedChannels.get(con4).size());
		channelB.close();
		assertEquals(2, cachedChannels.get(con4).size());
		channelC.close();
		assertEquals(2, cachedChannels.get(con4).size());

		// destroy
		ccf.destroy();
		assertNotNull(closedNotification.get());
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

		doAnswer(new Answer<com.rabbitmq.client.Connection>() {

			private int connectionNumber;

			@Override
			public com.rabbitmq.client.Connection answer(InvocationOnMock invocation) throws Throwable {
				com.rabbitmq.client.Connection connection = mock(com.rabbitmq.client.Connection.class);
				doAnswer(new Answer<Channel>() {

					private int channelNumber;

					@Override
					public Channel answer(InvocationOnMock invocation) throws Throwable {
						Channel channel = mock(Channel.class);
						when(channel.isOpen()).thenReturn(true);
						int channelNumnber = ++this.channelNumber;
						when(channel.toString()).thenReturn("mockChannel" + channelNumnber);
						mockChannels.add(channel);
						return channel;
					}
				}).when(connection).createChannel();
				int connectionNumber = ++this.connectionNumber;
				when(connection.toString()).thenReturn("mockConnection" + connectionNumber);
				when(connection.isOpen()).thenReturn(true);
				mockConnections.add(connection);
				return connection;
			}
		}).when(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setConnectionCacheSize(5);
		ccf.afterPropertiesSet();

		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertEquals(0, allocatedConnections.size());
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertEquals(0, idleConnections.size());

		Connection conn1 = ccf.createConnection();
		Connection conn2 = ccf.createConnection();
		Connection conn3 = ccf.createConnection();
		assertEquals(3, allocatedConnections.size());
		assertEquals(0, idleConnections.size());
		conn1.close();
		conn2.close();
		conn3.close();
		assertEquals(3, allocatedConnections.size());
		assertEquals(3, idleConnections.size());

		when(mockConnections.get(0).isOpen()).thenReturn(false);
		when(mockConnections.get(1).isOpen()).thenReturn(false);
		Connection conn4 = ccf.createConnection();
		assertEquals(3, allocatedConnections.size());
		assertEquals(2, idleConnections.size());
		assertSame(conn3, conn4);
		conn4.close();
		assertEquals(3, allocatedConnections.size());
		assertEquals(3, idleConnections.size());
		assertEquals("1", ccf.getCacheProperties().get("openConnections"));

		ccf.destroy();
		assertEquals(3, allocatedConnections.size());
		assertEquals(3, idleConnections.size());
		assertEquals("0", ccf.getCacheProperties().get("openConnections"));
	}

	@Test
	public void testConsumerChannelPhysicallyClosedWhenNotIsOpen() throws Exception {
		testConsumerChannelPhysicallyClosedWhenNotIsOpenGuts(false);
	}

	@Test
	public void testConsumerChannelWithPubConfPhysicallyClosedWhenNotIsOpen() throws Exception {
		testConsumerChannelPhysicallyClosedWhenNotIsOpenGuts(true);
	}

	public void testConsumerChannelPhysicallyClosedWhenNotIsOpenGuts(boolean confirms) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
			com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
			Channel mockChannel = mock(Channel.class);

			when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
			when(mockConnection.createChannel()).thenReturn(mockChannel);
			when(mockChannel.isOpen()).thenReturn(true);
			when(mockConnection.isOpen()).thenReturn(true);

			CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
			ccf.setExecutor(executor);
			ccf.setPublisherConfirms(confirms);
			Connection con = ccf.createConnection();

			Channel channel = con.createChannel(false);
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			when(mockChannel.isOpen()).thenReturn(false);
			final CountDownLatch physicalCloseLatch = new CountDownLatch(1);
			doAnswer(i -> {
				physicalCloseLatch.countDown();
				return null;
			}).when(mockChannel).close();
			channel.close();
			con.close(); // should be ignored

			assertTrue(physicalCloseLatch.await(10, TimeUnit.SECONDS));
		}
		finally {
			executor.shutdownNow();
		}
	}

	private void verifyConnectionIs(com.rabbitmq.client.Connection mockConnection, Object con) {
		assertSame(mockConnection, targetDelegate(con));
	}

	private com.rabbitmq.client.Connection targetDelegate(Object con) {
		return TestUtils.getPropertyValue(con, "target.delegate",
				com.rabbitmq.client.Connection.class);
	}

	private void verifyChannelIs(Channel mockChannel, Channel channel) {
		ChannelProxy proxy = (ChannelProxy) channel;
		assertSame(mockChannel, proxy.getTargetChannel());
	}

	@Test
	public void setAddressesEmpty() throws Exception {
		ConnectionFactory mock = mock(com.rabbitmq.client.ConnectionFactory.class);
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
		CachingConnectionFactory ccf = new CachingConnectionFactory(mock);
		ccf.setAddresses("mq1");
		ccf.createConnection();
		verify(mock).isAutomaticRecoveryEnabled();
		verify(mock)
				.newConnection(isNull(), aryEq(new Address[] { new Address("mq1") }), anyString());
		verifyNoMoreInteractions(mock);
	}

	@Test
	public void setAddressesTwoHosts() throws Exception {
		ConnectionFactory mock = mock(com.rabbitmq.client.ConnectionFactory.class);
		doReturn(true).when(mock).isAutomaticRecoveryEnabled();
		CachingConnectionFactory ccf = new CachingConnectionFactory(mock);
		ccf.setAddresses("mq1,mq2");
		ccf.createConnection();
		verify(mock).isAutomaticRecoveryEnabled();
		verify(mock).newConnection(isNull(),
				aryEq(new Address[] { new Address("mq1"), new Address("mq2") }), anyString());
		verifyNoMoreInteractions(mock);
	}

	@Test
	public void setUri() throws Exception {
		URI uri = new URI("amqp://localhost:1234/%2f");

		ConnectionFactory mock = mock(com.rabbitmq.client.ConnectionFactory.class);
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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);
		when(mockChannel.isOpen()).thenReturn(true).thenReturn(false);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		Connection con = ccf.createConnection();

		Channel channel = con.createChannel(false);
//		RabbitUtils.setPhysicalCloseRequired(true);
		channel.close(); // should be ignored, and placed into channel cache.
		channel.close(); // physically closed, so remove from the cache.
		channel.close(); // physically closed and removed from the cache  before, so void "close".
		Channel channel2 = con.createChannel(false);
		assertNotSame(channel, channel2);
	}

	@Test
	@Ignore // Test to verify log message is suppressed after patch to CCF
	public void testReturnsNormalCloseDeferredClose() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);
		when(mockChannel.isOpen()).thenReturn(true);
		doThrow(new ShutdownSignalException(true, false, new com.rabbitmq.client.AMQP.Connection.Close.Builder()
				.replyCode(200)
				.replyText("OK")
				.build(), null)).when(mockChannel).close();

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setPublisherReturns(true);
		ccf.setExecutor(Executors.newSingleThreadExecutor());
		Connection conn = ccf.createConnection();
		Channel channel = conn.createChannel(false);
		RabbitUtils.setPhysicalCloseRequired(channel, true);
		channel.close();
		Thread.sleep(6000);
	}

}
