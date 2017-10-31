/*
 * Copyright 2010-2017 the original author or authors.
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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Dmitry Dbrazhnikov
 * @author Artem Bilan
 */
public abstract class AbstractConnectionFactoryTests {

	protected abstract AbstractConnectionFactory createConnectionFactory(ConnectionFactory mockConnectionFactory);

	@Test
	public void testWithListener() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.setConnectionListeners(Collections.singletonList(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				called.incrementAndGet();
			}

			@Override
			public void onClose(Connection connection) {
				called.decrementAndGet();
			}

		}));

		Log logger = spy(TestUtils.getPropertyValue(connectionFactory, "logger", Log.class));
		doReturn(true).when(logger).isInfoEnabled();
		new DirectFieldAccessor(connectionFactory).setPropertyValue("logger", logger);
		Connection con = connectionFactory.createConnection();
		assertEquals(1, called.get());
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger, times(2)).info(captor.capture());
		assertThat(captor.getAllValues().get(0),
				containsString("Attempting to connect to: null:0"));
		assertThat(captor.getValue(),
				allOf(containsString("Created new connection: "), containsString("SimpleConnection")));

		con.close();
		assertEquals(1, called.get());
		verify(mockConnection, never()).close(anyInt());

		connectionFactory.createConnection();
		assertEquals(1, called.get());

		connectionFactory.destroy();
		assertEquals(0, called.get());
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory, times(1)).newConnection(any(ExecutorService.class), anyString());

		connectionFactory.setAddresses("foo:5672,bar:5672");
		con = connectionFactory.createConnection();
		assertEquals(1, called.get());
		captor = ArgumentCaptor.forClass(String.class);
		verify(logger, times(4)).info(captor.capture());
		assertThat(captor.getAllValues().get(2),
				containsString("Attempting to connect to: [foo:5672, bar:5672]"));
		assertThat(captor.getValue(),
				allOf(containsString("Created new connection: "), containsString("SimpleConnection")));

		con.close();
		connectionFactory.destroy();
		assertEquals(0, called.get());
	}

	@Test
	public void testWithListenerRegisteredAfterOpen() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		Connection con = connectionFactory.createConnection();
		assertEquals(0, called.get());

		connectionFactory.setConnectionListeners(Collections.singletonList(new ConnectionListener() {

			@Override
			public void onCreate(Connection connection) {
				called.incrementAndGet();
			}

			@Override
			public void onClose(Connection connection) {
				called.decrementAndGet();
			}

		}));
		assertEquals(1, called.get());

		con.close();
		assertEquals(1, called.get());
		verify(mockConnection, never()).close(anyInt());

		connectionFactory.createConnection();
		assertEquals(1, called.get());

		connectionFactory.destroy();
		assertEquals(0, called.get());
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory, times(1)).newConnection(any(ExecutorService.class), anyString());

	}

	@Test
	public void testCloseInvalidConnection() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.thenReturn(mockConnection1, mockConnection2);
		// simulate a dead connection
		when(mockConnection1.isOpen()).thenReturn(false);

		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);

		Connection connection = connectionFactory.createConnection();
		// the dead connection should be discarded
		connection.createChannel(false);
		verify(mockConnectionFactory, times(2)).newConnection(any(ExecutorService.class), anyString());
		verify(mockConnection2, times(1)).createChannel();

		connectionFactory.destroy();
		verify(mockConnection2, times(1)).close(anyInt());

	}

	@Test
	public void testDestroyBeforeUsed() throws Exception {

		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);

		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.destroy();

		verify(mockConnectionFactory, never()).newConnection((ExecutorService) null);
	}

	@Test
	public void testCreatesConnectionWithGivenFactory() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		doCallRealMethod().when(mockConnectionFactory).params(any(ExecutorService.class));
		doCallRealMethod().when(mockConnectionFactory).setThreadFactory(any(ThreadFactory.class));
		doCallRealMethod().when(mockConnectionFactory).getThreadFactory();

		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		ThreadFactory connectionThreadFactory = new CustomizableThreadFactory("connection-thread-");
		connectionFactory.setConnectionThreadFactory(connectionThreadFactory);

		assertEquals(connectionThreadFactory, mockConnectionFactory.getThreadFactory());
	}

}
