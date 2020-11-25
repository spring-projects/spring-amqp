/*
 * Copyright 2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
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
