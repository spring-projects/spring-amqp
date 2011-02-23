package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

/**
 * @author Mark Pollack
 */
public class CachingConnectionFactoryTests {

	@Test

	public void testWithConnectionFactoryDefaults() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);
		
		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
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

		Channel channel1 = con.createChannel(false); // This will return a
														// Spring AOP proxy that
														// surpresses calls to
														// close
		Channel channel2 = con.createChannel(false); // "

		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // "

		Channel ch1 = con.createChannel(false); // remove first entry in cache
												// (channel1)
		Channel ch2 = con.createChannel(false); // remove first entry in cache
												// (channel2)

		Assert.assertSame(ch1, channel1);
		Assert.assertSame(ch2, channel2);

		Channel target1 = ((ChannelProxy) ch1).getTargetChannel();
		Channel target2 = ((ChannelProxy) ch2).getTargetChannel();

		Assert.assertNotSame(target1, target2); // make sure mokito returned
												// different mocks for the
												// channel

		ch1.close();
		ch2.close();
		con.close(); // should be ignored

		ccf.destroy(); // should call close on connection and channels in cache

		verify(mockConnection, times(2)).createChannel();

		verify(mockConnection, times(1)).close();

		// verify(mockChannel1).close();
		verify(mockChannel2, times(1)).close();

	}
}
