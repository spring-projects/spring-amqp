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

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
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
public class ThreadChannelConnectionFactoryTests {

	@Test
	void testBasic() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		ThreadChannelConnectionFactory scf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		Connection conn = scf.createConnection();
		Channel chann1 = conn.createChannel(false);
		chann1.close();
		Channel chann2 = conn.createChannel(false);
		assertThat(chann2).isSameAs(chann1);
		chann2.close();
		conn.closeThreadChannel();
		assertThat(TestUtils.getPropertyValue(conn, "channels", ThreadLocal.class).get()).isNull();
		chann2 = conn.createChannel(false);
		assertThat(chann2).isNotSameAs(chann1);
		chann2.close();

		chann1 = conn.createChannel(true);
		assertThat(chann1).isNotSameAs(chann2);
		chann1.close();
		chann2 = conn.createChannel(true);
		assertThat(chann2).isSameAs(chann1);
		chann2.close();
		assertThat(TestUtils.getPropertyValue(conn, "channels", ThreadLocal.class).get()).isNotNull();
		assertThat(TestUtils.getPropertyValue(conn, "txChannels", ThreadLocal.class).get()).isNotNull();
		conn.closeThreadChannel();
		assertThat(TestUtils.getPropertyValue(conn, "txChannels", ThreadLocal.class).get()).isNull();
		chann2 = conn.createChannel(true);
		assertThat(((Channel) TestUtils.getPropertyValue(conn, "txChannels", ThreadLocal.class).get()).isOpen())
				.isTrue();
		chann2.close();
		chann2 = conn.createChannel(false);
		RabbitUtils.setPhysicalCloseRequired(chann2, true);
		chann2.close();
		scf.setSimplePublisherConfirms(true);
		chann1 = conn.createChannel(false);
		assertThat(chann1).isNotSameAs(chann2);
		assertThat(((ChannelProxy) chann1).isConfirmSelected()).isTrue();
		chann1.close();
		scf.destroy();
		assertThat(((Channel) TestUtils.getPropertyValue(conn, "channels", ThreadLocal.class).get()).isOpen())
				.isFalse();
		assertThat(((Channel) TestUtils.getPropertyValue(conn, "txChannels", ThreadLocal.class).get()).isOpen())
				.isFalse();
	}

	@Test
	void queueDeclared(@Autowired RabbitAdmin admin, @Autowired Config config,
			@Autowired ThreadChannelConnectionFactory tccf) throws Exception {

		assertThat(admin.getQueueProperties("ThreadChannelConnectionFactoryTests.q")).isNotNull();
		assertThat(config.created).isTrue();
		tccf.createConnection().createChannel(false).close();
		assertThat(config.channelCreated).isTrue();

		admin.deleteQueue("ThreadChannelConnectionFactoryTests.q");
		tccf.destroy();
		assertThat(config.closed).isTrue();
	}

	@Configuration
	public static class Config {

		boolean created;

		boolean closed;

		Connection connection;

		boolean channelCreated;

		@Bean
		ThreadChannelConnectionFactory tccf() {
			ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(
					RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
			tccf.addConnectionListener(new ConnectionListener() {

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
			tccf.addChannelListener(new ChannelListener() {

				@Override
				public void onCreate(Channel channel, boolean transactional) {
					Config.this.channelCreated = true;
				}

			});
			return tccf;
		}

		@Bean
		RabbitAdmin admin(ThreadChannelConnectionFactory tccf) {
			return new RabbitAdmin(tccf);
		}

		@Bean
		Queue queue() {
			return new Queue("ThreadChannelConnectionFactoryTests.q");
		}

	}

}
