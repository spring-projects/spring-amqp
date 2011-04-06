package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * @author Dave Syer
 */
public class SingleConnectionFactoryTests {

	@Test
	public void testWithListener() throws IOException {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);

		final AtomicBoolean called = new AtomicBoolean(false);
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setConnectionListeners(Arrays.asList(new ConnectionListener() {
			public void onCreate(Connection connection) {
				called.set(true);
			}
			public void onClose(Connection connection) {
				called.set(false);
			}
		}));

		Connection con = connectionFactory.createConnection();
		assertTrue(called.get());

		con.close();
		assertTrue(called.get());
		verify(mockConnection, never()).close();

		connectionFactory.destroy();
		assertFalse(called.get());
		verify(mockConnection, atLeastOnce()).close();

	}
	
	@Test
	public void testCloseInvalidConnection() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);

		when(mockConnectionFactory.newConnection()).thenReturn(mockConnection1).thenReturn(mockConnection2);
		// simulate a dead connection
		when(mockConnection1.isOpen()).thenReturn(false);

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		
		Connection connection = connectionFactory.createConnection();
		// the dead connection should be discarded
		connection.createChannel(false);
		verify(mockConnectionFactory, times(2)).newConnection();
		verify(mockConnection2, times(1)).createChannel();
		
		connectionFactory.destroy();
		verify(mockConnection2, times(1)).close();

	}

}
