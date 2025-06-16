/*
 * Copyright 2020-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Leonardo Ferreira
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

	@Configuration
	public static class Config {

		boolean created;

		boolean closed;

		Connection connection;

		boolean channelCreated;

		@Bean
		PooledChannelConnectionFactory pccf() {
			PooledChannelConnectionFactory pccf = new PooledChannelConnectionFactory(
					RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
			pccf.addConnectionListener(new ConnectionListener() {

				@Override
				public void onCreate(Connection connection) {
					Config.this.connection = connection;
					Config.this.created = true;
				}

				@Override
				public void onClose(Connection connection) {
					if (Config.this.connection.equals(connection)) {
						Config.this.closed = true;
					}
				}

			});
			pccf.addChannelListener(new ChannelListener() {

				@Override
				public void onCreate(Channel channel, boolean transactional) {
					Config.this.channelCreated = true;
				}

			});
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
