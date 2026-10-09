/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Dave Syer
 * @author Gary Russell
 */
public class SingleConnectionFactoryTests extends AbstractConnectionFactoryTests {

	@Override
	protected AbstractConnectionFactory createConnectionFactory(ConnectionFactory connectionFactory) {
		SingleConnectionFactory scf = new SingleConnectionFactory(connectionFactory);
		scf.setExecutor(mock(ExecutorService.class));
		return scf;
	}

	@Test
	public void testWithChannelListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.setChannelListeners(Collections.singletonList(
				(channel, transactional) -> called.incrementAndGet()));

		Connection con = connectionFactory.createConnection();
		Channel channel = con.createChannel(false);
		assertThat(called.get()).isEqualTo(1);
		channel.close();

		con.close();
		verify(mockConnection, never()).close();

		connectionFactory.createConnection();
		con.createChannel(false);
		assertThat(called.get()).isEqualTo(2);

		connectionFactory.destroy();
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

	}

}
