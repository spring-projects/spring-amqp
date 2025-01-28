/*
 * Copyright 2002-2025 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Artem Bilan
 * @author Josh Chappelle
 * @author Gary Russell
 * @author Leonardo Ferreira
 * @author Christian Tzolov
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
		assertThat(executorService.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

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
		assertThat(routingFactory.getTargetConnectionFactory("1")).isNull();

		//Add one and make sure it's there
		routingFactory.addTargetConnectionFactory("1", targetConnectionFactory);
		assertThat(routingFactory.getTargetConnectionFactory("1")).isEqualTo(targetConnectionFactory);
		assertThat(routingFactory.getTargetConnectionFactory("2")).isNull();

		//Remove it and make sure it's gone
		ConnectionFactory removedConnectionFactory = routingFactory.removeTargetConnectionFactory("1");
		assertThat(removedConnectionFactory).isEqualTo(targetConnectionFactory);
		assertThat(routingFactory.getTargetConnectionFactory("1")).isNull();
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
		Map<Object, ConnectionFactory> factories = new HashMap<>(2);
		factories.put("[baz]", connectionFactory1);
		factories.put("[foo,bar]", connectionFactory2);
		ConnectionFactory defaultConnectionFactory = mock(ConnectionFactory.class);

		SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();

		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		connectionFactory.setTargetConnectionFactories(factories);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setReceiveTimeout(10);
		container.setQueueNames("foo", "bar");
		container.afterPropertiesSet();
		container.start();

		verify(connectionFactory1, never()).createConnection();
		verify(connectionFactory2, times(2)).createConnection(); // container does an early open
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

			Lock lock = new ReentrantLock();

			@Override
			protected void redeclareElementsIfNecessary() {
				this.lock.lock();
				try {
					connectionMakerKey2.set(connectionFactory.determineCurrentLookupKey());
				}
				finally {
					this.lock.unlock();
				}
			}
		};
		container.setQueueNames("foo");
		container.setLookupKeyQualifier("xxx");
		container.afterPropertiesSet();
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(connectionMakerKey1.get()).isEqualTo("xxx[foo]");
		assertThat(connectionMakerKey2.get()).isEqualTo("xxx[foo]");
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
		final CountDownLatch latch = new CountDownLatch(2); // early connection in Abstract container
		willAnswer(i -> {
			connectionMakerKey.set(connectionFactory.determineCurrentLookupKey());
			latch.countDown();
			return connection;
		}).given(connectionFactory1).createConnection();
		connectionFactory.setTargetConnectionFactories(factories);

		final AtomicReference<Object> connectionMakerKey2 = new AtomicReference<>();
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(connectionFactory) {

			Lock lock = new ReentrantLock();

			@Override
			protected void redeclareElementsIfNecessary() {
				this.lock.lock();
				try {
					connectionMakerKey2.set(connectionFactory.determineCurrentLookupKey());
				}
				finally {
					this.lock.unlock();
				}
			}

		};
		container.setQueueNames("foo");
		container.setLookupKeyQualifier("xxx");
		container.setShutdownTimeout(10);
		container.afterPropertiesSet();
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(connectionMakerKey.get()).isEqualTo("xxx[foo]");
		assertThat(connectionMakerKey2.get()).isEqualTo("xxx[foo]");
	}

	@Test
	public void testWithDRTDMLCAndConnectionListenerExistingRFK() throws Exception {
		ConnectionFactory connectionFactory1 = mock(ConnectionFactory.class);
		Map<Object, ConnectionFactory> factories = new HashMap<Object, ConnectionFactory>(2);
		factories.put("xxx[foo]", connectionFactory1);
		factories.put("xxx[amq.rabbitmq.reply-to]", connectionFactory1);

		final SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();
		SimpleResourceHolder.bind(connectionFactory, "foo");

		final Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(channel.isOpen()).willReturn(true);
		given(connection.createChannel(anyBoolean())).willReturn(channel);
		final AtomicReference<Object> connectionMakerKey = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(2); // early connection in Abstract container
		willAnswer(i -> {
			connectionMakerKey.set(connectionFactory.determineCurrentLookupKey());
			latch.countDown();
			return connection;
		}).given(connectionFactory1).createConnection();
		connectionFactory.setTargetConnectionFactories(factories);

		final AtomicReference<Object> connectionMakerKey2 = new AtomicReference<>();
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(connectionFactory) {

			Lock lock = new ReentrantLock();

			@Override
			protected void redeclareElementsIfNecessary() {
				this.lock.lock();
				try {
					connectionMakerKey2.set(connectionFactory.determineCurrentLookupKey());
				}
				finally {
					this.lock.unlock();
				}
			}

		};
		container.setLookupKeyQualifier("xxx");
		container.setShutdownTimeout(10);
		container.afterPropertiesSet();
		container.start();
		ChannelHolder channelHolder = container.getChannelHolder();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.releaseConsumerFor(channelHolder, true, "test");
		container.stop();
		assertThat(connectionMakerKey.get()).isEqualTo("xxx[amq.rabbitmq.reply-to]");
		assertThat(connectionMakerKey2.get()).isEqualTo("xxx[amq.rabbitmq.reply-to]");
		assertThat(SimpleResourceHolder.unbind(connectionFactory)).isEqualTo("foo");
	}

	@Test
	void afterPropertiesSetShouldNotThrowAnyExceptionAfterAddTargetConnectionFactory() throws Exception {
		AbstractRoutingConnectionFactory routingFactory = new AbstractRoutingConnectionFactory() {
			@Override
			protected Object determineCurrentLookupKey() {
				return null;
			}
		};

		routingFactory.addTargetConnectionFactory("1", Mockito.mock(ConnectionFactory.class));

		assertThatNoException()
				.isThrownBy(routingFactory::afterPropertiesSet);
	}

}
