/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.connection;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Leonardo Ferreira
 * @author WooSeung Kim
 * @since 2.3
 *
 */
@RabbitAvailable
@SpringJUnitConfig
@DirtiesContext
public class PooledChannelConnectionFactoryTests {

	@Test
	void testBasic() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(rabbitConnectionFactory);
		AtomicBoolean txConfiged = new AtomicBoolean();
		AtomicBoolean nonTxConfiged = new AtomicBoolean();
		pcf.setPoolConfigurer((pool, tx) -> {
			if (tx) {
				txConfiged.set(true);
			}
			else {
				nonTxConfiged.set(true);
			}
		});
		pcf.setSimplePublisherConfirms(true);
		Connection conn = pcf.createConnection();
		assertThat(txConfiged.get()).isTrue();
		assertThat(nonTxConfiged.get()).isTrue();
		Channel chann1 = conn.createChannel(false);
		assertThat(((ChannelProxy) chann1).isConfirmSelected()).isTrue();
		chann1.close();
		Channel chann2 = conn.createChannel(false);
		assertThat(chann2).isSameAs(chann1);
		chann2.close();
		chann1 = conn.createChannel(true);
		assertThat(chann1).isNotSameAs(chann2);
		chann1.close();
		chann2 = conn.createChannel(true);
		assertThat(chann2).isSameAs(chann1);
		RabbitUtils.setPhysicalCloseRequired(chann2, true);
		chann2.close();
		chann1 = conn.createChannel(true);
		assertThat(chann1).isNotSameAs(chann2);
		chann1.close();
		pcf.destroy();
	}

	@Test
	void queueDeclared(@Autowired RabbitAdmin admin, @Autowired Config config,
			@Autowired PooledChannelConnectionFactory pccf) throws Exception {

		assertThat(admin.getQueueProperties("PooledChannelConnectionFactoryTests.q")).isNotNull();
		assertThat(config.created).isTrue();
		pccf.createConnection().createChannel(false).close();
		assertThat(config.channelCreated).isTrue();

		admin.deleteQueue("PooledChannelConnectionFactoryTests.q");
		pccf.destroy();
		assertThat(config.closed).isTrue();
	}

	@Test
	void copyConfigsToPublisherConnectionFactory() {
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(new ConnectionFactory());
		AtomicInteger txConfiged = new AtomicInteger();
		AtomicInteger nonTxConfiged = new AtomicInteger();
		pcf.setPoolConfigurer((pool, tx) -> {
			if (tx) {
				txConfiged.incrementAndGet();
			}
			else {
				nonTxConfiged.incrementAndGet();
			}
		});

		createAndCloseConnectionChannelTxAndChannelNonTx(pcf);

		final org.springframework.amqp.rabbit.connection.ConnectionFactory publisherConnectionFactory = pcf
				.getPublisherConnectionFactory();
		assertThat(publisherConnectionFactory).isNotNull();

		createAndCloseConnectionChannelTxAndChannelNonTx(publisherConnectionFactory);

		assertThat(txConfiged.get()).isEqualTo(2);
		assertThat(nonTxConfiged.get()).isEqualTo(2);

		final Object listenerPoolConfigurer = ReflectionTestUtils.getField(pcf, "poolConfigurer");
		final Object publisherPoolConfigurer = ReflectionTestUtils.getField(publisherConnectionFactory,
				"poolConfigurer");

		assertThat(listenerPoolConfigurer)
				.isSameAs(publisherPoolConfigurer);

		pcf.destroy();
	}

	@Test
	void copyConfigsToPublisherConnectionFactoryWhenUsingCustomPublisherFactory() {
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(new ConnectionFactory());
		AtomicBoolean listenerTxConfiged = new AtomicBoolean();
		AtomicBoolean listenerNonTxConfiged = new AtomicBoolean();
		pcf.setPoolConfigurer((pool, tx) -> {
			if (tx) {
				listenerTxConfiged.set(true);
			}
			else {
				listenerNonTxConfiged.set(true);
			}
		});

		final PooledChannelConnectionFactory publisherConnectionFactory = new PooledChannelConnectionFactory(
				new ConnectionFactory());

		AtomicBoolean publisherTxConfiged = new AtomicBoolean();
		AtomicBoolean publisherNonTxConfiged = new AtomicBoolean();
		publisherConnectionFactory.setPoolConfigurer((pool, tx) -> {
			if (tx) {
				publisherTxConfiged.set(true);
			}
			else {
				publisherNonTxConfiged.set(true);
			}
		});

		pcf.setPublisherConnectionFactory(publisherConnectionFactory);

		assertThat(pcf.getPublisherConnectionFactory()).isSameAs(publisherConnectionFactory);

		createAndCloseConnectionChannelTxAndChannelNonTx(pcf);

		assertThat(listenerTxConfiged.get()).isEqualTo(true);
		assertThat(listenerNonTxConfiged.get()).isEqualTo(true);

		final Object listenerPoolConfigurer = ReflectionTestUtils.getField(pcf, "poolConfigurer");
		final Object publisherPoolConfigurer = ReflectionTestUtils.getField(publisherConnectionFactory,
				"poolConfigurer");

		assertThat(listenerPoolConfigurer)
				.isNotSameAs(publisherPoolConfigurer);

		createAndCloseConnectionChannelTxAndChannelNonTx(publisherConnectionFactory);

		assertThat(publisherTxConfiged.get()).isEqualTo(true);
		assertThat(publisherNonTxConfiged.get()).isEqualTo(true);

		pcf.destroy();
	}

	private void createAndCloseConnectionChannelTxAndChannelNonTx(
			org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory) {

		Connection connection = connectionFactory.createConnection();
		Channel nonTxChannel = connection.createChannel(false);
		Channel txChannel = connection.createChannel(true);

		RabbitUtils.closeChannel(nonTxChannel);
		RabbitUtils.closeChannel(txChannel);
		connection.close();
	}

	@Test
	public void evictShouldCloseAllUnneededChannelsWithoutErrors() throws Exception {
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(new ConnectionFactory());
		AtomicReference<GenericObjectPool<Channel>> channelsReference = new AtomicReference<>();
		AtomicReference<GenericObjectPool<Channel>> txChannelsReference = new AtomicReference<>();
		AtomicInteger swallowedExceptionsCount = new AtomicInteger();
		pcf.setPoolConfigurer((pool, tx) -> {
			if (tx) {
				channelsReference.set(pool);
			}
			else {
				txChannelsReference.set(pool);
			}

			pool.setEvictionPolicy((ec, u, idleCount) -> idleCount > ec.getMinIdle());
			pool.setSwallowedExceptionListener(ex -> swallowedExceptionsCount.incrementAndGet());
			pool.setNumTestsPerEvictionRun(5);

			pool.setMinIdle(1);
			pool.setMaxIdle(5);
		});

		createAndCloseFiveChannelTxAndChannelNonTx(pcf);

		final GenericObjectPool<Channel> channels = channelsReference.get();
		channels.evict();

		assertThat(channels.getNumIdle())
				.isEqualTo(1);
		assertThat(channels.getDestroyedByEvictorCount())
				.isEqualTo(4);

		final GenericObjectPool<Channel> txChannels = txChannelsReference.get();
		txChannels.evict();
		assertThat(txChannels.getNumIdle())
				.isEqualTo(1);
		assertThat(txChannels.getDestroyedByEvictorCount())
				.isEqualTo(4);

		assertThat(swallowedExceptionsCount.get())
				.isZero();
	}

	private void createAndCloseFiveChannelTxAndChannelNonTx(
			org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory) {
		int channelAmount = 5;
		Connection connection = connectionFactory.createConnection();

		List<Channel> channels = new ArrayList<>(channelAmount);
		List<Channel> txChannels = new ArrayList<>(channelAmount);

		for (int i = 0; i < channelAmount; i++) {
			channels.add(connection.createChannel(false));
			txChannels.add(connection.createChannel(true));
		}

		for (int i = 0; i < channelAmount; i++) {
			RabbitUtils.closeChannel(channels.get(i));
			RabbitUtils.closeChannel(txChannels.get(i));
		}

		connection.close();
	}

	@Test
	void physicalCloseReleasesPoolSlot() throws Exception {
		AtomicReference<GenericObjectPool<Channel>> pool = new AtomicReference<>();
		PooledChannelConnectionFactory pcf = mockFactory(pool, null, 1);
		try {
			Connection connection = pcf.createConnection();
			Channel channel = connection.createChannel(false);
			Channel target = ((ChannelProxy) channel).getTargetChannel();
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			channel.close();
			assertThat(target.isOpen()).isFalse();
			assertThat(pool.get().getNumActive()).as("borrowed slots after physical close").isZero();
			connection.createChannel(false).close();
		}
		finally {
			pcf.destroy();
		}
	}

	@Test
	void physicalCloseReleasesPoolSlotWhenCloseFails() throws Exception {
		AtomicReference<GenericObjectPool<Channel>> pool = new AtomicReference<>();
		PooledChannelConnectionFactory pcf = mockFactory(pool, null, 1);
		try {
			Connection connection = pcf.createConnection();
			Channel channel = connection.createChannel(false);
			Channel target = ((ChannelProxy) channel).getTargetChannel();
			ShutdownSignalException failure = new ShutdownSignalException(false, false, null, target);
			willThrow(failure).given(target).close();
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			assertThatThrownBy(channel::close).isSameAs(failure);
			verify(target).close();
			assertThat(pool.get().getNumActive()).as("borrowed slots after failed physical close").isZero();
			assertThat(RabbitUtils.isPhysicalCloseRequired()).isFalse();
			connection.createChannel(false).close();
		}
		finally {
			RabbitUtils.clearPhysicalCloseRequired();
			pcf.destroy();
		}
	}

	@Test
	void transactionalPhysicalCloseReleasesPoolSlot() throws Exception {
		AtomicReference<GenericObjectPool<Channel>> pool = new AtomicReference<>();
		AtomicReference<GenericObjectPool<Channel>> txPool = new AtomicReference<>();
		PooledChannelConnectionFactory pcf = mockFactory(pool, txPool, 1);
		try {
			Connection connection = pcf.createConnection();
			Channel channel = connection.createChannel(true);
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			channel.close();
			assertThat(txPool.get().getNumActive()).as("borrowed transactional slots after physical close").isZero();
			Channel next = connection.createChannel(true);
			assertThat(next).isNotSameAs(channel);
			next.close();
		}
		finally {
			pcf.destroy();
		}
	}

	@Test
	void repeatedPhysicalCloseDoesNotLeakFlag() throws Exception {
		AtomicReference<GenericObjectPool<Channel>> pool = new AtomicReference<>();
		PooledChannelConnectionFactory pcf = mockFactory(pool, null, 2);
		try {
			Connection connection = pcf.createConnection();
			Channel channel = connection.createChannel(false);
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			channel.close();
			RabbitUtils.setPhysicalCloseRequired(channel, true);
			RabbitUtils.closeChannel(channel);
			assertThat(RabbitUtils.isPhysicalCloseRequired()).isFalse();
			Channel next = connection.createChannel(false);
			next.close();
			assertThat(next.isOpen()).isTrue();
			assertThat(pool.get().getNumActive()).as("borrowed slots after repeated physical close").isZero();
		}
		finally {
			RabbitUtils.clearPhysicalCloseRequired();
			pcf.destroy();
		}
	}

	@Test
	void sequentialInvokeDoesNotExhaustPool(ConnectionFactory rabbitConnectionFactory) {
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(rabbitConnectionFactory);
		pcf.setPoolConfigurer((pool, transactional) -> pool.setMaxWait(Duration.ofMillis(500)));
		try {
			RabbitTemplate template = new RabbitTemplate(pcf);
			for (int i = 0; i < 9; i++) {
				template.invoke(operations -> null);
			}
		}
		finally {
			pcf.destroy();
		}
	}

	@Test
	void receiveTimeoutOnBrokerReleasesPoolSlot(ConnectionFactory rabbitConnectionFactory) throws Exception {
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(rabbitConnectionFactory);
		AtomicReference<GenericObjectPool<Channel>> pool = new AtomicReference<>();
		pcf.setPoolConfigurer((candidate, transactional) -> {
			candidate.setMaxTotal(1);
			candidate.setMaxWait(Duration.ofMillis(500));
			if (!transactional) {
				pool.set(candidate);
			}
		});
		CachingConnectionFactory adminConnectionFactory = new CachingConnectionFactory(rabbitConnectionFactory);
		RabbitAdmin admin = new RabbitAdmin(adminConnectionFactory);
		String queue = "pooled-receive-" + UUID.randomUUID();
		try {
			admin.declareQueue(new Queue(queue));
			RabbitTemplate template = new RabbitTemplate(pcf);
			assertThat(template.receive(queue, 100)).isNull();
			assertThat(pool.get().getNumActive()).as("borrowed slots after receive timeout").isZero();
			assertThat(template.receive(queue, 100)).isNull();
		}
		finally {
			admin.deleteQueue(queue);
			adminConnectionFactory.destroy();
			pcf.destroy();
		}
	}

	private static PooledChannelConnectionFactory mockFactory(AtomicReference<GenericObjectPool<Channel>> pool,
			@Nullable AtomicReference<GenericObjectPool<Channel>> txPool, int maxTotal) throws Exception {

		ConnectionFactory rabbitConnectionFactory = mock();
		com.rabbitmq.client.Connection rabbitConnection = mock();
		given(rabbitConnectionFactory.newConnection(nullable(ExecutorService.class), anyString()))
				.willReturn(rabbitConnection);
		given(rabbitConnection.isOpen()).willReturn(true);
		willAnswer(invocation -> newMockChannel()).given(rabbitConnection).createChannel();
		PooledChannelConnectionFactory pcf = new PooledChannelConnectionFactory(rabbitConnectionFactory);
		pcf.setPoolConfigurer((candidate, transactional) -> {
			candidate.setMaxTotal(maxTotal);
			candidate.setMaxWait(Duration.ofMillis(300));
			if (transactional) {
				if (txPool != null) {
					txPool.set(candidate);
				}
			}
			else {
				pool.set(candidate);
			}
		});
		return pcf;
	}

	private static Channel newMockChannel() throws Exception {
		Channel channel = mock();
		AtomicBoolean open = new AtomicBoolean(true);
		willAnswer(invocation -> open.get()).given(channel).isOpen();
		willAnswer(invocation -> {
			open.set(false);
			return null;
		}).given(channel).close();
		return channel;
	}

	@Configuration
	public static class Config {

		boolean created;

		boolean closed;

		@Nullable
		Connection connection;

		boolean channelCreated;

		@Bean
		PooledChannelConnectionFactory pccf() {
			PooledChannelConnectionFactory pccf = new PooledChannelConnectionFactory(
					RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
			pccf.addConnectionListener(new ConnectionListener() {

				@Override
				public void onCreate(@Nullable Connection connection) {
					Config.this.connection = connection;
					Config.this.created = true;
				}

				@Override
				public void onClose(Connection connection) {
					if (connection.equals(Config.this.connection)) {
						Config.this.closed = true;
					}
				}

			});
			pccf.addChannelListener((channel, transactional) -> Config.this.channelCreated = true);
			return pccf;
		}

		@Bean
		RabbitAdmin admin(PooledChannelConnectionFactory pccf) {
			return new RabbitAdmin(pccf);
		}

		@Bean
		Queue queue() {
			return new Queue("PooledChannelConnectionFactoryTests.q");
		}

	}

}
