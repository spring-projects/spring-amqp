/*
 * Copyright 2010-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.AmqpResourceNotAvailableException;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory.AddressShuffleMode;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.StopWatch;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willCallRealMethod;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Dmitry Dbrazhnikov
 * @author Artem Bilan
 * @author Salk Lee
 */
public abstract class AbstractConnectionFactoryTests {

	protected abstract AbstractConnectionFactory createConnectionFactory(ConnectionFactory mockConnectionFactory);

	@Test
	public void testWithListener() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection = mock();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnectionFactory.newConnection(any(), anyList(), anyString())).willReturn(mockConnection);

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
		willReturn(true).given(logger).isInfoEnabled();
		new DirectFieldAccessor(connectionFactory).setPropertyValue("logger", logger);
		Connection con = connectionFactory.createConnection();
		assertThat(called.get()).isEqualTo(1);
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger, times(2)).info(captor.capture());
		assertThat(captor.getAllValues().get(0)).contains("Attempting to connect to: null:0");
		assertThat(captor.getValue()).contains("Created new connection: ").contains("SimpleConnection");

		con.close();
		assertThat(called.get()).isEqualTo(1);
		verify(mockConnection, never()).close(anyInt());

		connectionFactory.createConnection();
		assertThat(called.get()).isEqualTo(1);

		connectionFactory.destroy();
		assertThat(called.get()).isEqualTo(0);
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory, times(1)).newConnection(any(ExecutorService.class), anyString());

		connectionFactory.setAddresses(List.of("foo:5672", "bar:5672"));
		connectionFactory.setAddressShuffleMode(AddressShuffleMode.NONE);
		con = connectionFactory.createConnection();
		assertThat(called.get()).isEqualTo(1);
		captor = ArgumentCaptor.forClass(String.class);
		verify(logger, times(4)).info(captor.capture());
		assertThat(captor.getAllValues().get(2)).contains("Attempting to connect to: [foo:5672, bar:5672]");
		assertThat(captor.getValue()).contains("Created new connection: ").contains("SimpleConnection");

		con.close();
		connectionFactory.destroy();
		assertThat(called.get()).isEqualTo(0);
	}

	@Test
	public void testWithListenerRegisteredAfterOpen() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection = mock();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);

		final AtomicInteger called = new AtomicInteger(0);
		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		Connection con = connectionFactory.createConnection();
		assertThat(called.get()).isEqualTo(0);

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
		assertThat(called.get()).isEqualTo(1);

		con.close();
		assertThat(called.get()).isEqualTo(1);
		verify(mockConnection, never()).close(anyInt());

		connectionFactory.createConnection();
		assertThat(called.get()).isEqualTo(1);

		connectionFactory.destroy();
		assertThat(called.get()).isEqualTo(0);
		verify(mockConnection, atLeastOnce()).close(anyInt());

		verify(mockConnectionFactory, times(1)).newConnection(any(ExecutorService.class), anyString());

	}

	@Test
	public void testCloseInvalidConnection() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		com.rabbitmq.client.Connection mockConnection1 = mock();
		com.rabbitmq.client.Connection mockConnection2 = mock();

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(mockConnection1, mockConnection2);
		// simulate a dead connection
		given(mockConnection1.isOpen()).willReturn(false);
		given(mockConnection2.createChannel()).willReturn(mock(Channel.class));

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
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();

		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		connectionFactory.destroy();

		verify(mockConnectionFactory, never()).newConnection((ExecutorService) null);
	}

	@Test
	public void testCreatesConnectionWithGivenFactory() {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock();
		willCallRealMethod().given(mockConnectionFactory).params(any(ExecutorService.class));
		willCallRealMethod().given(mockConnectionFactory).setThreadFactory(any(ThreadFactory.class));
		willCallRealMethod().given(mockConnectionFactory).getThreadFactory();

		AbstractConnectionFactory connectionFactory = createConnectionFactory(mockConnectionFactory);
		ThreadFactory connectionThreadFactory = new CustomizableThreadFactory("connection-thread-");
		connectionFactory.setConnectionThreadFactory(connectionThreadFactory);

		assertThat(mockConnectionFactory.getThreadFactory()).isEqualTo(connectionThreadFactory);
	}

	@Test
	public void testConnectionCreatingBackOff() throws Exception {
		int maxAttempts = 2;
		long interval = 100L;
		com.rabbitmq.client.Connection mockConnection = mock(com.rabbitmq.client.Connection.class);
		given(mockConnection.createChannel()).willReturn(null);
		SimpleConnection simpleConnection = new SimpleConnection(mockConnection, 5,
				new FixedBackOff(interval, maxAttempts).start());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		assertThatExceptionOfType(AmqpResourceNotAvailableException.class).isThrownBy(() -> {
			simpleConnection.createChannel(false);
		});
		stopWatch.stop();
		assertThat(stopWatch.getTotalTimeMillis()).isGreaterThanOrEqualTo(maxAttempts * interval);
		verify(mockConnection, times(maxAttempts + 1)).createChannel();
	}

}
