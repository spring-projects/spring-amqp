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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import com.rabbitmq.client.Channel;

/**
 * @author Artem Bilan
 * @author Josh Chappelle
 * @author Gary Russell
 * @since 1.3
 */
public class RoutingConnectionFactoryTests {

	@Test
	public void testAbstractRoutingConnectionFactory() {
		ConnectionFactory connectionFactory1 = Mockito.mock(ConnectionFactory.class);
		ConnectionFactory connectionFactory2 = Mockito.mock(ConnectionFactory.class);
		Map<Object, ConnectionFactory> factories = new HashMap<Object, ConnectionFactory>(2);
		factories.put(Boolean.TRUE, connectionFactory1);
		factories.put(Boolean.FALSE, connectionFactory2);
		ConnectionFactory defaultConnectionFactory = Mockito.mock(ConnectionFactory.class);

		final AtomicBoolean lookupFlag = new AtomicBoolean(true);
		final AtomicInteger count = new AtomicInteger();

		AbstractRoutingConnectionFactory connectionFactory = new AbstractRoutingConnectionFactory() {

			@Override
			protected Object determineCurrentLookupKey() {
				return count.incrementAndGet() > 3 ? null : lookupFlag.getAndSet(!lookupFlag.get());
			}
		};

		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		connectionFactory.setTargetConnectionFactories(factories);

		for (int i = 0; i < 5; i++) {
			connectionFactory.createConnection();
		}

		verify(connectionFactory1, times(2)).createConnection();
		verify(connectionFactory2).createConnection();
		verify(defaultConnectionFactory, times(2)).createConnection();
	}

	@Test
	public void testSimpleRoutingConnectionFactory() throws InterruptedException {
		ConnectionFactory connectionFactory1 = Mockito.mock(ConnectionFactory.class);
		ConnectionFactory connectionFactory2 = Mockito.mock(ConnectionFactory.class);
		Map<Object, ConnectionFactory> factories = new HashMap<Object, ConnectionFactory>(2);
		factories.put("foo", connectionFactory1);
		factories.put("bar", connectionFactory2);


		final AbstractRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();
		connectionFactory.setTargetConnectionFactories(factories);

		ExecutorService executorService = Executors.newFixedThreadPool(3);


		for (int i = 0; i < 3; i++) {
			final AtomicInteger count = new AtomicInteger(i);
			executorService.execute(() -> {
				SimpleResourceHolder.bind(connectionFactory, count.get() % 2 == 0 ? "foo" : "bar");
				connectionFactory.createConnection();
				SimpleResourceHolder.unbind(connectionFactory);
			});
		}

		executorService.shutdown();
		assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

		verify(connectionFactory1, times(2)).createConnection();
		verify(connectionFactory2).createConnection();
	}

	@Test
	public void testGetAddAndRemoveOperationsForTargetConnectionFactories() {
		ConnectionFactory targetConnectionFactory = Mockito.mock(ConnectionFactory.class);

		AbstractRoutingConnectionFactory routingFactory = new AbstractRoutingConnectionFactory() {
			@Override
			protected Object determineCurrentLookupKey() {
				return null;
			}
		};

		//Make sure map is initialized and doesn't contain lookup key "1"
		assertNull(routingFactory.getTargetConnectionFactory("1"));

		//Add one and make sure it's there
		routingFactory.addTargetConnectionFactory("1", targetConnectionFactory);
		assertEquals(targetConnectionFactory, routingFactory.getTargetConnectionFactory("1"));
		assertNull(routingFactory.getTargetConnectionFactory("2"));

		//Remove it and make sure it's gone
		ConnectionFactory removedConnectionFactory = routingFactory.removeTargetConnectionFactory("1");
		assertEquals(targetConnectionFactory, removedConnectionFactory);
		assertNull(routingFactory.getTargetConnectionFactory("1"));
	}

	@Test
	public void testAddTargetConnectionFactoryAddsExistingConnectionListenersToConnectionFactory() {

		AbstractRoutingConnectionFactory routingFactory = new AbstractRoutingConnectionFactory() {
			@Override
			protected Object determineCurrentLookupKey() {
				return null;
			}
		};
		routingFactory.addConnectionListener(Mockito.mock(ConnectionListener.class));
		routingFactory.addConnectionListener(Mockito.mock(ConnectionListener.class));

		ConnectionFactory targetConnectionFactory = Mockito.mock(ConnectionFactory.class);
		routingFactory.addTargetConnectionFactory("1", targetConnectionFactory);
		verify(targetConnectionFactory,
				times(2)).addConnectionListener(any(ConnectionListener.class));
	}

	@Test
	public void testAbstractRoutingConnectionFactoryWithListenerContainer() {
		ConnectionFactory connectionFactory1 = mock(ConnectionFactory.class);
		ConnectionFactory connectionFactory2 = mock(ConnectionFactory.class);
		Map<Object, ConnectionFactory> factories = new HashMap<Object, ConnectionFactory>(2);
		factories.put("[baz]", connectionFactory1);
		factories.put("[foo,bar]", connectionFactory2);
		ConnectionFactory defaultConnectionFactory = mock(ConnectionFactory.class);

		SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();

		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		connectionFactory.setTargetConnectionFactories(factories);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo, bar");
		container.afterPropertiesSet();
		container.start();

		verify(connectionFactory1, never()).createConnection();
		verify(connectionFactory2).createConnection();
		verify(defaultConnectionFactory, never()).createConnection();

		reset(connectionFactory1, connectionFactory2, defaultConnectionFactory);
		container.setQueueNames("baz");
		verify(connectionFactory1).createConnection();
		verify(connectionFactory2, never()).createConnection();
		verify(defaultConnectionFactory, never()).createConnection();

		reset(connectionFactory1, connectionFactory2, defaultConnectionFactory);
		container.setQueueNames("qux");
		verify(connectionFactory1, never()).createConnection();
		verify(connectionFactory2, never()).createConnection();
		verify(defaultConnectionFactory).createConnection();

		container.stop();
	}

	@Test
	public void testWithSMLCAndConnectionListener() throws Exception {
		ConnectionFactory connectionFactory1 = mock(ConnectionFactory.class);
		Map<Object, ConnectionFactory> factories = new HashMap<Object, ConnectionFactory>(2);
		factories.put("xxx[foo]", connectionFactory1);

		final SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();

		final Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		final AtomicReference<Object> connectionMakerKey1 = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			connectionMakerKey1.set(connectionFactory.determineCurrentLookupKey());
			latch.countDown();
			return connection;
		}).given(connectionFactory1).createConnection();
		connectionFactory.setTargetConnectionFactories(factories);

		final AtomicReference<Object> connectionMakerKey2 = new AtomicReference<>();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory) {

			@Override
			protected synchronized void redeclareElementsIfNecessary() {
				connectionMakerKey2.set(connectionFactory.determineCurrentLookupKey());
			}

		};
		container.setQueueNames("foo");
		container.setLookupKeyQualifier("xxx");
		container.afterPropertiesSet();
		container.start();
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.stop();
		assertThat(connectionMakerKey1.get(), equalTo("xxx[foo]"));
		assertThat(connectionMakerKey2.get(), equalTo("xxx[foo]"));
	}

	@Test
	public void testWithDMLCAndConnectionListener() throws Exception {
		ConnectionFactory connectionFactory1 = mock(ConnectionFactory.class);
		Map<Object, ConnectionFactory> factories = new HashMap<Object, ConnectionFactory>(2);
		factories.put("xxx[foo]", connectionFactory1);

		final SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();

		final Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		final AtomicReference<Object> connectionMakerKey = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			connectionMakerKey.set(connectionFactory.determineCurrentLookupKey());
			latch.countDown();
			return connection;
		}).given(connectionFactory1).createConnection();
		connectionFactory.setTargetConnectionFactories(factories);

		final AtomicReference<Object> connectionMakerKey2 = new AtomicReference<>();
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory) {

			@Override
			protected synchronized void redeclareElementsIfNecessary() {
				connectionMakerKey2.set(connectionFactory.determineCurrentLookupKey());
			}

		};
		container.setQueueNames("foo");
		container.setLookupKeyQualifier("xxx");
		container.setShutdownTimeout(10);
		container.afterPropertiesSet();
		container.start();
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.stop();
		assertThat(connectionMakerKey.get(), equalTo("xxx[foo]"));
		assertThat(connectionMakerKey2.get(), equalTo("xxx[foo]"));
	}

}
