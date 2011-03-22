package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
		SingleConnectionFactory ccf = new SingleConnectionFactory(mockConnectionFactory);
		ccf.setConnectionListeners(Arrays.asList(new ConnectionListener(){
			public void onCreate(Connection connection) {
				called.set(true);
			}	
		}));
		Connection con = ccf.createConnection();
		
		assertTrue(called.get());
		
		con.close();
		
		verify(mockConnection, never()).close();
		
	}

}
