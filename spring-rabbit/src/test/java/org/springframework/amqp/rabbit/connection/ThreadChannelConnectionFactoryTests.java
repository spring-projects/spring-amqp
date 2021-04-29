/*
 * Copyright 2020-2021 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
@RabbitAvailable(queues = "ThreadChannelConnectionFactoryTests.q1")
@SpringJUnitConfig
@DirtiesContext
public class ThreadChannelConnectionFactoryTests {

	@Test
	void testBasic() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		Connection conn = tccf.createConnection();
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
		tccf.setSimplePublisherConfirms(true);
		chann1 = conn.createChannel(false);
		assertThat(chann1).isNotSameAs(chann2);
		assertThat(((ChannelProxy) chann1).isConfirmSelected()).isTrue();
		chann1.close();
		tccf.destroy();
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

	@Test
	void contextSwitch() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		TaskExecutor exec = new SimpleAsyncTaskExecutor();
		AtomicReference<Channel> nonTx = new AtomicReference<>();
		AtomicReference<Channel> tx = new AtomicReference<>();
		BlockingQueue<Object> context = new LinkedBlockingQueue<>();
		exec.execute(() -> {
			Connection conn = tccf.createConnection();
			nonTx.set(conn.createChannel(false));
			tx.set(conn.createChannel(true));
			Object ctx = tccf.prepareSwitchContext();
			assertThat(TestUtils.getPropertyValue(conn, "channels", ThreadLocal.class).get()).isNull();
			assertThat(TestUtils.getPropertyValue(conn, "txChannels", ThreadLocal.class).get()).isNull();
			context.add(ctx);
		});
		Object ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		assertThatIllegalStateException().isThrownBy(() -> tccf.switchContext(ctx));
		Connection conn = tccf.createConnection();
		Channel chann1 = conn.createChannel(false);
		assertThat(chann1).isSameAs(nonTx.get());
		Channel chann2 = conn.createChannel(true);
		assertThat(chann2).isSameAs(tx.get());
		tccf.destroy();
	}

	@Test
	void contextSwitchViaTemplate() throws Exception {
		// Template uses the nested publisher factory
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		RabbitTemplate template = new RabbitTemplate(tccf);
		TaskExecutor exec = new SimpleAsyncTaskExecutor();
		AtomicReference<Channel> nonTx = new AtomicReference<>();
		AtomicReference<Channel> tx = new AtomicReference<>();
		BlockingQueue<Object> context = new LinkedBlockingQueue<>();
		exec.execute(() -> {
			template.execute(this::sendChannelNameAsBody);
			context.add(tccf.prepareSwitchContext());
		});
		Object ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		template.execute(this::sendChannelNameAsBody);
		Message received1 = template.receive("ThreadChannelConnectionFactoryTests.q1", 10_000);
		Message received2 = template.receive("ThreadChannelConnectionFactoryTests.q1", 10_000);
		assertThat(new String(received1.getBody())).isEqualTo(new String(received2.getBody()));
		tccf.destroy();
	}

	@Test
	void orphanClosed() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		RabbitTemplate template = new RabbitTemplate(tccf);
		TaskExecutor exec = new SimpleAsyncTaskExecutor();
		AtomicReference<Channel> nonTx = new AtomicReference<>();
		AtomicReference<Channel> tx = new AtomicReference<>();
		BlockingQueue<Object> context = new LinkedBlockingQueue<>();
		Runnable task = () -> {
			template.execute(channel -> null);
			context.add(tccf.prepareSwitchContext());
		};
		exec.execute(task);
		Object ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		Channel toBeOrphaned = template.execute(channel -> null);
		exec.execute(task);
		ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		template.execute(channel -> null);
		assertThat(toBeOrphaned.isOpen()).isFalse();
		tccf.destroy();
	}

	Channel sendChannelNameAsBody(Channel channel) throws IOException {
		channel.basicPublish("", "ThreadChannelConnectionFactoryTests.q1", new BasicProperties(),
				channel.toString().getBytes());
		return channel;
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
