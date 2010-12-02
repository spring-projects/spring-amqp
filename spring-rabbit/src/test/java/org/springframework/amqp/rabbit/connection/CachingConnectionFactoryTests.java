package org.springframework.amqp.rabbit.connection;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

/**
 * @author Mark Pollack
 */

public class CachingConnectionFactoryTests {

	@Test
	public void testWithConnectionFactoryDefaults() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		Connection con = ccf.createConnection();

		Channel channel = con.createChannel();
		channel.close(); // should be ignored, and placed into channel cache.
		con.close(); // should be ignored

		Connection con2 = ccf.createConnection();
		Channel channel2 = con2.createChannel(); // will retrieve same channel object that was just put into channel
													// cache
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
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);

		when(mockChannel1.basicGet("foo", false)).thenReturn(new GetResponse(null, null, null, 1));
		when(mockChannel2.basicGet("bar", false)).thenReturn(new GetResponse(null, null, null, 1));

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(2);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel();
		Channel channel2 = con.createChannel();

		channel1.basicGet("foo", true);
		channel2.basicGet("bar", true);

		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // should be ignored, and add last into channel cache.

		Channel ch1 = con.createChannel(); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(); // remove first entry in cache (channel2)

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
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		Channel mockChannel3 = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2).thenReturn(mockChannel3);

		// Called during physical close
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel();
		// cache size is 1, but the other connection is not released yet so this creates a new one
		Channel channel2 = con.createChannel();
		Assert.assertNotSame(channel1, channel2);

		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // should be physically closed

		Channel ch1 = con.createChannel(); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(); // create anew channel

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
		verify(mockChannel3, never()).close();

	}

	@Test
	public void testCacheSizeExceededAfterClose() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);

		// Called during physical close
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel();
		channel1.close(); // should be ignored, and add last into channel cache.
		Channel channel2 = con.createChannel();
		channel2.close(); // should be ignored, and add last into channel cache.
		Assert.assertSame(channel1, channel2);

		Channel ch1 = con.createChannel(); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(); // create new channel

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
	@Ignore // TODO: add this feature
	public void testTransactionalAndNonTransactional() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);

		// Called during physical close
		when(mockChannel2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(1);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel();
		channel1.txSelect();
		channel1.close(); // should be ignored, and add last into channel cache.
		/*
		 * When a channel is created we can't assume it should be transactional, so this should create a new one.
		 */
		Channel channel2 = con.createChannel();
		channel2.close(); // should be ignored, and add last into channel cache.
		Assert.assertNotSame(channel1, channel2);

		Channel ch1 = con.createChannel(); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(); // create new channel

		Assert.assertNotSame(ch1, ch2);
		Assert.assertSame(ch1, channel2); // The non-transactional one
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
	public void testWithConnectionFactoryDestroy() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);

		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		Assert.assertNotSame(mockChannel1, mockChannel2);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		// You can't repeat 'when' statements for stubbing consecutive calls to the same method to returning different
		// values.
		when(mockConnection.createChannel()).thenReturn(mockChannel1).thenReturn(mockChannel2);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(2);

		Connection con = ccf.createConnection();

		Channel channel1 = con.createChannel(); // This will return a Spring AOP proxy that surpresses calls to close
		Channel channel2 = con.createChannel(); // "

		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // "

		Channel ch1 = con.createChannel(); // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel(); // remove first entry in cache (channel2)

		Assert.assertSame(ch1, channel1);
		Assert.assertSame(ch2, channel2);

		Channel target1 = ((ChannelProxy) ch1).getTargetChannel();
		Channel target2 = ((ChannelProxy) ch2).getTargetChannel();

		Assert.assertNotSame(target1, target2); // make sure mokito returned different mocks for the channel

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
