package org.springframework.amqp.rabbit.connection;

import static org.mockito.Mockito.*;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

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
		con.close();  // should be ignored
		
		Connection con2 = ccf.createConnection();
		Channel channel2 = con2.createChannel(); // will retrieve same channel object that was just put into channel cache
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
		when(mockConnection.createChannel()).thenReturn(mockChannel1);
		when(mockConnection.createChannel()).thenReturn(mockChannel2);
				
		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(2);
		
		Connection con = ccf.createConnection();
		
		Channel channel1 = con.createChannel();
		Channel channel2 = con.createChannel();
		
		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); // should be ignored, and add last into channel cache.
		
		Channel ch1 = con.createChannel();  // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel();  // remove first entry in cache (channel2)

		Assert.assertNotSame(ch1, ch2);
		Assert.assertSame(ch1, channel1);
		Assert.assertSame(ch2, channel2);
				
		ch1.close();
		ch2.close();
		
		verify(mockConnection, times(2)).createChannel();
				
		con.close();  // should be ignored

		verify(mockConnection, never()).close();		
		verify(mockChannel1, never()).close();
		verify(mockChannel2, never()).close();
				
	}
	
	@Test
	public void testWithConnectionFactoryDestory() throws IOException {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);
		
		Assert.assertNotSame(mockChannel1, mockChannel2);
		
		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
		//You can't repeat 'when' statements for stubbing consecutive calls to the same method to returning different values.
		stub(mockConnection.createChannel()).toReturn(mockChannel1).toReturn(mockChannel2);
				
		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setChannelCacheSize(2);
		
		Connection con = ccf.createConnection();
		
		Channel channel1 = con.createChannel();  // This will return a Spring AOP proxy that surpresses calls to close
		Channel channel2 = con.createChannel();  //  "
			
		channel1.close(); // should be ignored, and add last into channel cache.
		channel2.close(); //  "
		
		Channel ch1 = con.createChannel();  // remove first entry in cache (channel1)
		Channel ch2 = con.createChannel();  // remove first entry in cache (channel2)
				
			
		Assert.assertSame(ch1, channel1);
		Assert.assertSame(ch2, channel2);
		
		Channel target1 = ((ChannelProxy)ch1).getTargetChannel();
		Channel target2 = ((ChannelProxy)ch2).getTargetChannel();
		
		Assert.assertNotSame(target1, target2);  // make sure mokito returned different mocks for the channel
				
		ch1.close();
		ch2.close();					
		con.close();  // should be ignored
		
		ccf.destroy();  // should call close on connection and channels in cache
		
		verify(mockConnection, times(2)).createChannel();
		
		verify(mockConnection, times(1)).close();		
		
		//verify(mockChannel1).close();
		verify(mockChannel2, times(1)).close();
				
	}
}
