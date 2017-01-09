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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

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

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.setChannelListeners(Collections.singletonList(
				(channel, transactional) -> called.incrementAndGet()));

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

		verify(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

	}

}
