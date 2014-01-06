/*
 * Copyright 2002-2014 the original author or authors.
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
package org.springframework.amqp.rabbit.connection;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.test.util.ReflectionTestUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

/**
 * @author Mark Pollack
 * @author Dave Syer
 * @author Gary Russell
 */
public class CachingConnectionFactoryTests extends AbstractConnectionFactoryTests {

	@Override
	protected AbstractConnectionFactory createConnectionFactory(ConnectionFactory connectionFactory) {
		return new CachingConnectionFactory(connectionFactory);
	}

	@Test
	public void testWithConnectionFactoryDefaults() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockConnection.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
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

		Assert.assertSame(con, con2);
		Assert.assertSame(channel, channel2);
		verify(mockConnection, never()).close();
		verify(mockChannel, never()).close();

	}
	@Test
	public void testWithConnectionFactoryCacheSize() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);

		when(mockChannel1.basicGet("foo", false)).thenReturn(new GetResponse(null, null, null, 1));
		when(mockChannel2.basicGet("bar", false)).thenReturn(new GetResponse(null, null, null, 1));
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(2);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		Channel channel2 = con.createChannel(false);

		channel1.basicGet("foo", true);
		channel2.basicGet("bar", true);

		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // should be ignored, and add last into channel cache.

		Channel ch1 = con.createChannel(false); // remove first entry in cache
												// (channel1)
		Channel ch2 = con.createChannel(false); // remove first entry in cache
												// (channel2)

		Assert.assertNotSame(ch1, ch2);
		Assert.assertSame(ch1, channel1);
		Assert.assertSame(ch2, channel2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(2)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, never()).close();

	}

	@Test
	public void testCacheSizeExceeded() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockChannel3 = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2).thenReturn(mockChannel3);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);
		when(mockChannel3.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		// cache size is 1, but the other connection is not released yet so this
		// creates a new one
		Channel channel2 = con.createChannel(false);
		Assert.assertNotSame(channel1, channel2);

		// should be ignored, and added last into channel cache.
		channel1.close();
		// should be physically closed
		channel2.close();

		// remove first entry in cache (channel1)
		Channel ch1 = con.createChannel(false);
		// create a new channel
		Channel ch2 = con.createChannel(false);

		Assert.assertNotSame(ch1, ch2);
		Assert.assertSame(ch1, channel1);
		Assert.assertNotSame(ch2, channel2);

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
	public void testCacheSizeExceededAfterClose() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(false);
		channel1.close(); // should be ignored, and add last into channel cache.
		Channel channel2 = con.createChannel(false);
		channel2.close(); // should be ignored, and add last into channel cache.
		Assert.assertSame(channel1, channel2);

		Channel ch1 = con.createChannel(false); // remove first entry in cache
												// (channel1)
		Channel ch2 = con.createChannel(false); // create new channel

		Assert.assertNotSame(ch1, ch2);
		Assert.assertSame(ch1, channel1);
		Assert.assertNotSame(ch2, channel2);

		ch1.close();
		ch2.close();

		verify(mockConnection, times(2)).createChannel();

		con.close(); // should be ignored

		verify(mockConnection, never()).close();
		verify(mockChannel1, never()).close();
		verify(mockChannel2, atLeastOnce()).close();

	}

	@Test
	public void testTransactionalAndNonTransactionalChannelsSegregated() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);
		when(mockConnection.isOpen()).thenReturn(true);

		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
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
		Assert.assertNotSame(channel1, channel2);

		Channel ch1 = con.createChannel(true); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(false); // create new channel

		Assert.assertNotSame(ch1, ch2);
		Assert.assertSame(ch1, channel1); // The non-transactional one
		Assert.assertSame(ch2, channel2);

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
	public void testWithConnectionFactoryDestroy() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);

		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		Assert.assertNotSame(mockChannel1, mockChannel2);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		// You can't repeat 'when' statements for stubbing consecutive calls to
		// the same method to returning different
		// values.
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);
		when(mockConnection.isOpen()).thenReturn(true);
		// Called during physical close
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
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

		Assert.assertSame(ch1, channel1);
		Assert.assertSame(ch2, channel2);

		Channel target1 = ((ChannelProxy) ch1).getTargetChannel();
		Channel target2 = ((ChannelProxy) ch2).getTargetChannel();

		// make sure mokito returned different mocks for the channel
		Assert.assertNotSame(target1, target2);

		ch1.close();
		ch2.close();
		con.close(); // should be ignored

		ccf.destroy(); // should call close on connection and channels in cache

		verify(mockConnection, times(2)).createChannel();

		verify(mockConnection).close(anyInt());

		// verify(mockChannel1).close();
		verify(mockChannel2).close();

		// After destroy we can get a new connection
		Connection con1 = ccf.createConnection();
		Assert.assertNotSame(con, con1);

		// This will return a proxy that surpresses calls to close
		Channel channel3 = con.createChannel(false);
		Assert.assertNotSame(channel3, channel1);
		Assert.assertNotSame(channel3, channel2);
	}

	@Test
	public void testWithChannelListener() throws IOException {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
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
		((CachingConnectionFactory)connectionFactory).setChannelCacheSize(1);

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

		verify(mockConnectionFactory).newConnection((ExecutorService) null);

	}

	@Test
	public void testWithConnectionListener() throws IOException {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection1, mockConnection2);
		when(mockConnection1.isOpen()).thenReturn(true);
		when(mockChannel.isOpen()).thenReturn(true);
		when(mockConnection1.createChannel()).thenReturn(mockChannel);
		when(mockConnection2.createChannel()).thenReturn(mockChannel);

		final AtomicReference<Connection> created = new AtomicReference<Connection>();
		final AtomicReference<Connection> closed = new AtomicReference<Connection>();
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.addConnectionListener(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				created.set(connection);
			}

			@Override
			public void onClose(Connection connection) {
				closed.set(connection);
			}
		});
		((CachingConnectionFactory)connectionFactory).setChannelCacheSize(1);

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

		when(mockConnection1.isOpen()).thenReturn(false);
		when(mockChannel.isOpen()).thenReturn(false); // force a connection refresh
		channel.basicCancel("foo");
		channel.close();

		Connection notSame = connectionFactory.createConnection();
		assertNotSame(con, notSame);
		assertSame(con, closed.get());
		assertSame(notSame, created.get());

		connectionFactory.destroy();
		verify(mockConnection2, atLeastOnce()).close(anyInt());
		assertSame(notSame, closed.get());

		verify(mockConnectionFactory, times(2)).newConnection((ExecutorService) null);
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
		}).when(mockConnectionFactory).newConnection((ExecutorService) null);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.afterPropertiesSet();

		Set<?> openConnections = TestUtils.getPropertyValue(ccf, "openConnections", Set.class);
		assertEquals(0, openConnections.size());
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertEquals(0, idleConnections.size());

		final AtomicReference<Connection> createNotification = new AtomicReference<Connection>();
		final AtomicReference<Connection> closedNotification = new AtomicReference<Connection>();
		ccf.setConnectionListeners(Collections.singletonList(new ConnectionListener(){

			@Override
			public void onCreate(Connection connection) {
				assertNull(createNotification.get());
				createNotification.set(connection);
			}

			@Override
			public void onClose(Connection connection) {
				assertNull(closedNotification.get());
				closedNotification.set(connection);
			}
		}));

		Connection con1 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con1);
		assertEquals(1, openConnections.size());
		assertEquals(0, idleConnections.size());
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(0), targetDelegate(createNotification.getAndSet(null)));

		Channel channel1 = con1.createChannel(false);
		verifyChannelIs(mockChannels.get(0), channel1);
		channel1.close();
		verify(mockChannels.get(0)).close();

		con1.close(); // should be ignored, and placed into connection cache.
		verify(mockConnections.get(0), never()).close();
		assertEquals(1, openConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(closedNotification.get());

		/*
		 * will retrieve same connection that was just put into cache, and new channel
		 */
		Connection con2 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(0), con2);
		Channel channel2 = con2.createChannel(false);
		verifyChannelIs(mockChannels.get(1), channel2);
		channel2.close();
		verify(mockChannels.get(1)).close();
		con2.close();
		verify(mockConnections.get(0), never()).close();
		assertEquals(1, openConnections.size());
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
		verifyChannelIs(mockChannels.get(2), channel1);
		channel2 = con2.createChannel(false);
		verifyChannelIs(mockChannels.get(3), channel2);
		assertEquals(2, openConnections.size());
		assertEquals(0, idleConnections.size());
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(1), targetDelegate(createNotification.getAndSet(null)));

		// put mock1 in cache
		channel1.close();
		verify(mockChannels.get(2)).close();
		con1.close();
		verify(mockConnections.get(0), never()).close();
		assertEquals(2, openConnections.size());
		assertEquals(1, idleConnections.size());
		assertNull(closedNotification.get());

		Connection con3 = ccf.createConnection();
		assertNull(createNotification.get());
		verifyConnectionIs(mockConnections.get(0), con3);
		Channel channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(4), channel3);

		assertEquals(2, openConnections.size());
		assertEquals(0, idleConnections.size());

		channel2.close();
		con2.close();
		assertEquals(2, openConnections.size());
		assertEquals(1, idleConnections.size());
		channel3.close();
		con3.close();
		assertEquals(1, openConnections.size());
		assertEquals(1, idleConnections.size());
		/*
		 *  Cache size is 1; con3 (mock1) should have been a real close.
		 *  con2 (mock2) should still be in the cache.
		 */
		verify(mockConnections.get(0)).close(30000);
		assertNotNull(closedNotification.get());
		assertSame(mockConnections.get(0), targetDelegate(closedNotification.getAndSet(null)));
		verify(mockChannels.get(4)).close();
		verify(mockConnections.get(1), never()).close(30000);
		verify(mockChannels.get(3)).close();
		verifyConnectionIs(mockConnections.get(1), idleConnections.iterator().next());
		/*
		 * Now a closed cached connection
		 */
		when(mockConnections.get(1).isOpen()).thenReturn(false);
		con3 = ccf.createConnection();
		assertNotNull(closedNotification.get());
		assertSame(mockConnections.get(1), targetDelegate(closedNotification.getAndSet(null)));
		verifyConnectionIs(mockConnections.get(2), con3);
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(2), targetDelegate(createNotification.getAndSet(null)));
		assertEquals(1, openConnections.size());
		assertEquals(0, idleConnections.size());
		channel3 = con3.createChannel(false);
		verifyChannelIs(mockChannels.get(5), channel3);
		channel3.close();
		con3.close();
		assertNull(closedNotification.get());
		assertEquals(1, openConnections.size());
		assertEquals(1, idleConnections.size());

		/*
		 * Now a closed cached connection when creating a channel
		 */
		con3 = ccf.createConnection();
		verifyConnectionIs(mockConnections.get(2), con3);
		assertNull(createNotification.get());
		assertEquals(1, openConnections.size());
		assertEquals(0, idleConnections.size());
		when(mockConnections.get(2).isOpen()).thenReturn(false);
		channel3 = con3.createChannel(false);
		assertNotNull(closedNotification.get());
		closedNotification.set(null);
		assertNotNull(createNotification.get());
		assertSame(mockConnections.get(3), targetDelegate(createNotification.getAndSet(null)));
		verifyChannelIs(mockChannels.get(6), channel3);
		channel3.close();
		con3.close();
		assertNull(closedNotification.get());
		assertEquals(1, openConnections.size());
		assertEquals(1, idleConnections.size());

		// destroy
		ccf.destroy();
		assertNotNull(closedNotification.get());
		verify(mockConnections.get(3)).close(30000);
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
		}).when(mockConnectionFactory).newConnection((ExecutorService) null);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setCacheMode(CacheMode.CONNECTION);
		ccf.setConnectionCacheSize(5);
		ccf.afterPropertiesSet();

		Set<?> openConnections = TestUtils.getPropertyValue(ccf, "openConnections", Set.class);
		assertEquals(0, openConnections.size());
		BlockingQueue<?> idleConnections = TestUtils.getPropertyValue(ccf, "idleConnections", BlockingQueue.class);
		assertEquals(0, idleConnections.size());

		Connection conn1 = ccf.createConnection();
		Connection conn2 = ccf.createConnection();
		Connection conn3 = ccf.createConnection();
		assertEquals(3, openConnections.size());
		assertEquals(0, idleConnections.size());
		conn1.close();
		conn2.close();
		conn3.close();
		assertEquals(3, openConnections.size());
		assertEquals(3, idleConnections.size());

		when(mockConnections.get(0).isOpen()).thenReturn(false);
		when(mockConnections.get(1).isOpen()).thenReturn(false);
		Connection conn4 = ccf.createConnection();
		assertEquals(1, openConnections.size());
		assertEquals(0, idleConnections.size());
		assertSame(conn3, conn4);
		conn4.close();
		assertEquals(1, openConnections.size());
		assertEquals(1, idleConnections.size());

		ccf.destroy();
		assertEquals(0, openConnections.size());
		assertEquals(0, idleConnections.size());
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

}
