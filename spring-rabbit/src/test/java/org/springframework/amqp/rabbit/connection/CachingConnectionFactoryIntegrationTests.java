/*
 * Copyright 2002-present the original author or authors.
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

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpResourceNotAvailableException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
@RabbitAvailable(queues = CachingConnectionFactoryIntegrationTests.CF_INTEGRATION_TEST_QUEUE)
@LogLevels(classes = {CachingConnectionFactoryIntegrationTests.class,
		CachingConnectionFactory.class}, categories = "com.rabbitmq", level = "DEBUG")
public class CachingConnectionFactoryIntegrationTests {

	public static final String CF_INTEGRATION_TEST_QUEUE = "cfIntegrationTest";

	private static final String CF_INTEGRATION_CONNECTION_NAME = "cfIntegrationTestConnectionName";

	private static Log logger = LogFactory.getLog(CachingConnectionFactoryIntegrationTests.class);

	private CachingConnectionFactory connectionFactory;

	@BeforeEach
	public void open() {
		connectionFactory = new CachingConnectionFactory("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		connectionFactory.getRabbitConnectionFactory().getClientProperties().put("foo", "bar");
		connectionFactory.setConnectionNameStrategy(cf -> CF_INTEGRATION_CONNECTION_NAME);
	}

	@AfterEach
	public void close() {
		if (!this.connectionFactory.getVirtualHost().equals("non-existent")) {
			RabbitAvailableCondition.getBrokerRunning().purgeTestQueues();
		}
		assertThat(connectionFactory.getRabbitConnectionFactory().getClientProperties().get("foo")).isEqualTo("bar");
		connectionFactory.destroy();
	}

	@Test
	public void testCachedConnections() {
		connectionFactory.setCacheMode(CacheMode.CONNECTION);
		connectionFactory.setConnectionCacheSize(5);
		connectionFactory.setExecutor(Executors.newCachedThreadPool());
		List<Connection> connections = new ArrayList<>();
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		assertThat(connections.get(1)).isNotSameAs(connections.get(0));
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		Set<?> allocatedConnections = TestUtils.propertyValue(connectionFactory, "allocatedConnections");
		assertThat(allocatedConnections).hasSize(6);
		connections.forEach(Connection::close);
		assertThat(allocatedConnections).hasSize(6);
		assertThat(connectionFactory.getCacheProperties().get("openConnections")).isEqualTo("5");
		BlockingQueue<?> idleConnections = TestUtils.propertyValue(connectionFactory, "idleConnections");
		assertThat(idleConnections).hasSize(6);
		connections.clear();
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		assertThat(allocatedConnections).hasSize(6);
		assertThat(idleConnections).hasSize(4);
		connections.forEach(Connection::close);
	}

	@Test
	public void testCachedConnectionsChannelLimit() throws Exception {
		connectionFactory.setCacheMode(CacheMode.CONNECTION);
		connectionFactory.setConnectionCacheSize(2);
		connectionFactory.setChannelCacheSize(1);
		connectionFactory.setChannelCheckoutTimeout(10);
		connectionFactory.setExecutor(Executors.newCachedThreadPool());
		List<Connection> connections = new ArrayList<Connection>();
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		List<Channel> channels = new ArrayList<Channel>();
		channels.add(connections.get(0).createChannel(false));
		try {
			channels.add(connections.get(0).createChannel(false));
			fail("Exception expected");
		}
		catch (AmqpTimeoutException e) {

		}
		channels.add(connections.get(1).createChannel(false));
		try {
			channels.add(connections.get(1).createChannel(false));
			fail("Exception expected");
		}
		catch (AmqpTimeoutException e) {

		}
		channels.get(0).close();
		channels.get(1).close();
		channels.add(connections.get(0).createChannel(false));
		channels.add(connections.get(1).createChannel(false));
		assertThat(channels.get(2)).isSameAs(channels.get(0));
		assertThat(channels.get(3)).isSameAs(channels.get(1));
		channels.get(2).close();
		channels.get(3).close();
		connections.forEach(Connection::close);
	}

	@Test
	public void testCachedConnectionsAndChannels() throws Exception {
		connectionFactory.setCacheMode(CacheMode.CONNECTION);
		connectionFactory.setConnectionCacheSize(1);
		connectionFactory.setChannelCacheSize(3);
		// the following is needed because we close the underlying connection below.
		connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(false);
		List<Connection> connections = new ArrayList<>();
		connections.add(connectionFactory.createConnection());
		connections.add(connectionFactory.createConnection());
		Set<?> allocatedConnections = TestUtils.propertyValue(connectionFactory, "allocatedConnections");
		assertThat(allocatedConnections).hasSize(2);
		assertThat(connections.get(1)).isNotSameAs(connections.get(0));
		List<Channel> channels = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			channels.add(connections.get(0).createChannel(false));
			channels.add(connections.get(1).createChannel(false));
			channels.add(connections.get(0).createChannel(true));
			channels.add(connections.get(1).createChannel(true));
		}
		Map<?, List<?>> cachedChannels =
				TestUtils.propertyValue(connectionFactory, "allocatedConnectionNonTransactionalChannels");
		assertThat(cachedChannels.get(connections.get(0))).hasSize(0);
		assertThat(cachedChannels.get(connections.get(1))).hasSize(0);
		Map<?, List<?>> cachedTxChannels =
				TestUtils.propertyValue(connectionFactory, "allocatedConnectionTransactionalChannels");
		assertThat(cachedTxChannels.get(connections.get(0))).hasSize(0);
		assertThat(cachedTxChannels.get(connections.get(1))).hasSize(0);
		for (Channel channel : channels) {
			channel.close();
		}
		assertThat(cachedChannels.get(connections.get(0))).hasSize(3);
		assertThat(cachedChannels.get(connections.get(1))).hasSize(3);
		assertThat(cachedTxChannels.get(connections.get(0))).hasSize(3);
		assertThat(cachedTxChannels.get(connections.get(1))).hasSize(3);
		for (int i = 0; i < 3; i++) {
			assertThat(connections.get(0).createChannel(false)).isEqualTo(channels.get(i * 4));
			assertThat(connections.get(1).createChannel(false)).isEqualTo(channels.get(i * 4 + 1));
			assertThat(connections.get(0).createChannel(true)).isEqualTo(channels.get(i * 4 + 2));
			assertThat(connections.get(1).createChannel(true)).isEqualTo(channels.get(i * 4 + 3));
		}
		assertThat(cachedChannels.get(connections.get(0))).hasSize(0);
		assertThat(cachedChannels.get(connections.get(1))).hasSize(0);
		assertThat(cachedTxChannels.get(connections.get(0))).hasSize(0);
		assertThat(cachedTxChannels.get(connections.get(1))).hasSize(0);
		for (Channel channel : channels) {
			channel.close();
		}
		for (Connection connection : connections) {
			connection.close();
		}
		assertThat(cachedChannels.get(connections.get(0))).hasSize(3);
		assertThat(cachedChannels.get(connections.get(1))).hasSize(0);
		assertThat(cachedTxChannels.get(connections.get(0))).hasSize(3);
		assertThat(cachedTxChannels.get(connections.get(1))).hasSize(0);

		assertThat(allocatedConnections).hasSize(2);
		assertThat(connectionFactory.getCacheProperties().get("openConnections")).isEqualTo("1");

		Connection connection = connectionFactory.createConnection();
		Connection rabbitConnection = TestUtils.propertyValue(connection, "target");
		rabbitConnection.close();
		Channel channel = connection.createChannel(false);
		assertThat(allocatedConnections).hasSize(2);
		assertThat(connectionFactory.getCacheProperties().get("openConnections")).isEqualTo("1");
		channel.close();
		connection.close();
		assertThat(allocatedConnections).hasSize(2);
		assertThat(connectionFactory.getCacheProperties().get("openConnections")).isEqualTo("1");
	}

	@Test
	public void testSendAndReceiveFromVolatileQueue() {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = admin.declareQueue();
		template.convertAndSend(queue.getName(), "message");
		String result = (String) template.receiveAndConvert(queue.getName());
		assertThat(result).isEqualTo("message");
		template.stop();

	}

	@Test
	public void testReceiveFromNonExistentVirtualHost() {
		connectionFactory.setVirtualHost("non-existent");
		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		assertThatThrownBy(() -> template.receiveAndConvert("foo"))
				.isInstanceOfAny(
						// Wrong vhost is very unfriendly to client - the exception has no clue (just an EOF)
						AmqpIOException.class,
						AmqpAuthenticationException.class,
						/*
						 * If localhost also resolves to an IPv6 address, the client will try that
						 * after a failure due to an invalid vHost and, if Rabbit is not listening there,
						 * we'll get an...
						 */
						AmqpConnectException.class);
	}

	@Test
	public void testSendAndReceiveFromVolatileQueueAfterImplicitRemoval() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = admin.declareQueue();
		template.convertAndSend(queue.getName(), "message");

		// Force a physical close of the channel
		this.connectionFactory.stop();

		// The queue was removed when the channel was closed
		assertThatThrownBy(() -> template.receiveAndConvert(queue.getName()))
				.isInstanceOf(AmqpIOException.class);
		template.stop();
	}

	@Test
	public void testMixTransactionalAndNonTransactional() throws Exception {

		RabbitTemplate template1 = new RabbitTemplate(connectionFactory);
		RabbitTemplate template2 = new RabbitTemplate(connectionFactory);
		template1.setChannelTransacted(true);

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = admin.declareQueue();

		template1.convertAndSend(queue.getName(), "message");
		String result = (String) template2.receiveAndConvert(queue.getName());
		assertThat(result).isEqualTo("message");

		// The channel is not transactional
		assertThatThrownBy(() ->
				template2.execute(channel -> {
					// Should be an exception because the channel is not transactional
					channel.txRollback();
					return null;
				})).isInstanceOf(AmqpIOException.class);

	}

	@Test
	public void testHardErrorAndReconnectNoAuto() throws Exception {
		this.connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(false);
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Queue queue = new Queue(CF_INTEGRATION_TEST_QUEUE);
		admin.declareQueue(queue);
		final String route = queue.getName();

		final CountDownLatch latch = new CountDownLatch(1);
		try {
			template.execute(channel -> {
				channel.getConnection().addShutdownListener(cause -> {
					logger.info("Error", cause);
					latch.countDown();
					// This will be thrown on the Connection thread just before it dies, so basically ignored
					throw new RuntimeException(cause);
				});
				String tag = channel.basicConsume(route, new DefaultConsumer(channel));
				// Consume twice with the same tag is a hard error (connection will be reset)
				String result = channel.basicConsume(route, false, tag, new DefaultConsumer(channel));
				fail("Expected IOException, got: " + result);
				return null;
			});
			fail("Expected AmqpIOException");
		}
		catch (AmqpIOException e) {
			// expected
		}
		template.convertAndSend(route, "message");
		assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isTrue();
		String result = (String) template.receiveAndConvert(route);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(route);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testConnectionCloseLog() {
		Log log = TestUtils.propertyValue(this.connectionFactory, "logger");
		log = spy(log);
		new DirectFieldAccessor(this.connectionFactory).setPropertyValue("logger", log);
		Connection conn = this.connectionFactory.createConnection();
		conn.createChannel(false);
		this.connectionFactory.destroy();
		verify(log, never()).error(anyString());
	}

	@Test
	public void testConnectionName() {
		Connection connection = this.connectionFactory.createConnection();
		com.rabbitmq.client.Connection rabbitConnection = TestUtils.propertyValue(connection, "target.delegate");
		assertThat(rabbitConnection.getClientProperties().get("connection_name"))
				.isEqualTo(CF_INTEGRATION_CONNECTION_NAME);
		this.connectionFactory.destroy();
	}

	@Test
	public void testDestroy() {
		Connection connection1 = this.connectionFactory.createConnection();
		this.connectionFactory.destroy();
		Connection connection2 = this.connectionFactory.createConnection();
		assertThat(connection2).isSameAs(connection1);
		ApplicationContext context = mock(ApplicationContext.class);
		this.connectionFactory.setApplicationContext(context);
		this.connectionFactory.onApplicationEvent(new ContextClosedEvent(context));
		this.connectionFactory.destroy();
		try {
			connection2 = this.connectionFactory.createConnection();
			fail("Expected exception");
		}
		catch (AmqpApplicationContextClosedException e) {
			assertThat(e.getMessage()).contains("is closed");
		}
	}

	@Test
	@Disabled // Don't run this on the CI build server
	public void hangOnClose() throws Exception {
		final Socket proxy = SocketFactory.getDefault().createSocket("localhost", 5672);
		final ServerSocket server = ServerSocketFactory.getDefault().createServerSocket(2765);
		final AtomicBoolean hangOnClose = new AtomicBoolean();
		// create a simple proxy so we can drop the close response
		Executors.newSingleThreadExecutor().execute(() -> {
			try {
				final Socket socket = server.accept();
				Executors.newSingleThreadExecutor().execute(() -> {
					while (!socket.isClosed()) {
						try {
							int c = socket.getInputStream().read();
							if (c >= 0) {
								proxy.getOutputStream().write(c);
							}
						}
						catch (Exception e) {
							try {
								socket.close();
								proxy.close();
							}
							catch (Exception ee) {

							}
						}
					}
				});
				while (!proxy.isClosed()) {
					try {
						int c = proxy.getInputStream().read();
						if (c >= 0 && !hangOnClose.get()) {
							socket.getOutputStream().write(c);
						}
					}
					catch (Exception e) {
						try {
							socket.close();
							proxy.close();
						}
						catch (Exception ee) {

						}
					}
				}
				socket.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		});
		CachingConnectionFactory factory = new CachingConnectionFactory(2765);
		factory.createConnection();
		hangOnClose.set(true);
		factory.destroy();
	}

	@Test
	public void testChannelMax() {
		this.connectionFactory.getRabbitConnectionFactory().setRequestedChannelMax(1);
		Connection connection = this.connectionFactory.createConnection();
		connection.createChannel(true);
		assertThatExceptionOfType(AmqpResourceNotAvailableException.class)
				.isThrownBy(() -> connection.createChannel(false));
	}

}
