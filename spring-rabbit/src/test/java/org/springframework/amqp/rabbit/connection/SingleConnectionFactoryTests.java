package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Dave Syer
 */
public class SingleConnectionFactoryTests extends AbstractConnectionFactoryTests {

	@Override
	protected AbstractConnectionFactory createConnectionFactory(ConnectionFactory connectionFactory) {
		return new SingleConnectionFactory(connectionFactory);
	}

	@Test
	public void testWithChannelListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.setChannelListeners(Arrays.asList(new ChannelListener() {
			public void onCreate(Channel channel, boolean transactional) {
				called.incrementAndGet();
			}
		}));

		Connection con = connectionFactory.createConnection();
		Channel channel = con.createChannel(false);
		assertEquals(1, called.get());
		channel.close();

		con.close();
		verify(mockConnection, never()).close();

		connectionFactory.createConnection();
		con.createChannel(false);
		assertEquals(2, called.get());

		connectionFactory.destroy();
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory).newConnection((ExecutorService) null);

	}

}
