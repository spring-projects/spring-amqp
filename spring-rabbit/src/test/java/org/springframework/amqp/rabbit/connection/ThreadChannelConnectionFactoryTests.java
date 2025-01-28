/*
 * Copyright 2020-2025 the original author or authors.
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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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
	void testClose() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		Connection conn = tccf.createConnection();
		Channel chann1 = conn.createChannel(false);
		Channel targetChannel1 = ((ChannelProxy) chann1).getTargetChannel();
		chann1.close();
		Channel chann2 = conn.createChannel(false);
		Channel targetChannel2 = ((ChannelProxy) chann2).getTargetChannel();
		assertThat(chann2).isSameAs(chann1);
		assertThat(targetChannel2).isSameAs(targetChannel1);
	}

	@Test
	void testTxClose() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		Connection conn = tccf.createConnection();
		Channel chann1 = conn.createChannel(true);
		Channel targetChannel1 = ((ChannelProxy) chann1).getTargetChannel();
		chann1.close();
		Channel chann2 = conn.createChannel(true);
		Channel targetChannel2 = ((ChannelProxy) chann2).getTargetChannel();
		assertThat(chann2).isSameAs(chann1);
		assertThat(targetChannel2).isSameAs(targetChannel1);
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

	@SuppressWarnings("unchecked")
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
			assertThat(tccf.prepareSwitchContext()).isNull();
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
		assertThat(TestUtils.getPropertyValue(tccf, "switchesInProgress", Map.class)).isEmpty();
		tccf.switchContext(null); // test no-op
		tccf.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test
	void contextSwitchMulti() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		TaskExecutor exec = new SimpleAsyncTaskExecutor();
		AtomicReference<Channel> nonTx = new AtomicReference<>();
		AtomicReference<Channel> tx = new AtomicReference<>();
		BlockingQueue<Object> context = new LinkedBlockingQueue<>();
		CountDownLatch latch = new CountDownLatch(1);
		exec.execute(() -> {
			Connection conn = tccf.createConnection();
			nonTx.set(conn.createChannel(false));
			tx.set(conn.createChannel(true));
			Object ctx = tccf.prepareSwitchContext();
			assertThat(tccf.prepareSwitchContext()).isNull();
			assertThat(TestUtils.getPropertyValue(conn, "channels", ThreadLocal.class).get()).isNull();
			assertThat(TestUtils.getPropertyValue(conn, "txChannels", ThreadLocal.class).get()).isNull();
			conn.createChannel(false);
			context.add(ctx);
			context.add(tccf.prepareSwitchContext());
			latch.countDown();
		});
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		Object ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		Connection conn = tccf.createConnection();
		Channel chann1 = conn.createChannel(false);
		assertThat(chann1).isSameAs(nonTx.get());
		Channel chann2 = conn.createChannel(true);
		assertThat(chann2).isSameAs(tx.get());
		assertThat(TestUtils.getPropertyValue(tccf, "switchesInProgress", Map.class)).hasSize(1);
		ctx = context.poll(10, TimeUnit.SECONDS);
		tccf.switchContext(ctx);
		assertThat(TestUtils.getPropertyValue(tccf, "switchesInProgress", Map.class)).isEmpty();
		tccf.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test
	void contextSwitchBothFactories() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		TaskExecutor exec = new SimpleAsyncTaskExecutor();
		AtomicReference<Channel> nonTx1 = new AtomicReference<>();
		AtomicReference<Channel> tx1 = new AtomicReference<>();
		BlockingQueue<Object> context = new LinkedBlockingQueue<>();
		AtomicReference<Channel> nonTx2 = new AtomicReference<>();
		AtomicReference<Channel> tx2 = new AtomicReference<>();
		exec.execute(() -> {
			Connection conn1 = tccf.createConnection();
			nonTx1.set(conn1.createChannel(false));
			tx1.set(conn1.createChannel(true));
			org.springframework.amqp.rabbit.connection.ConnectionFactory pcf = tccf.getPublisherConnectionFactory();
			Connection conn2 = pcf.createConnection();
			nonTx2.set(conn2.createChannel(false));
			tx2.set(conn2.createChannel(true));
			Object ctx = tccf.prepareSwitchContext();
			assertThat(tccf.prepareSwitchContext()).isNull();
			assertThat(TestUtils.getPropertyValue(conn1, "channels", ThreadLocal.class).get()).isNull();
			assertThat(TestUtils.getPropertyValue(conn1, "txChannels", ThreadLocal.class).get()).isNull();
			assertThat(TestUtils.getPropertyValue(conn2, "channels", ThreadLocal.class).get()).isNull();
			assertThat(TestUtils.getPropertyValue(conn2, "txChannels", ThreadLocal.class).get()).isNull();
			context.add(ctx);
		});
		Object ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		assertThatIllegalStateException().isThrownBy(() -> tccf.switchContext(ctx));
		Connection conn1 = tccf.createConnection();
		Channel chann1 = conn1.createChannel(false);
		assertThat(chann1).isSameAs(nonTx1.get());
		Channel chann2 = conn1.createChannel(true);
		assertThat(chann2).isSameAs(tx1.get());
		org.springframework.amqp.rabbit.connection.ConnectionFactory pcf = tccf.getPublisherConnectionFactory();
		Connection conn2 = pcf.createConnection();
		Channel chann3 = conn2.createChannel(false);
		assertThat(chann3).isSameAs(nonTx2.get());
		Channel chann4 = conn2.createChannel(true);
		assertThat(chann4).isSameAs(tx2.get());
		assertThat(TestUtils.getPropertyValue(tccf, "switchesInProgress", Map.class)).isEmpty();
		assertThat(TestUtils.getPropertyValue(tccf, "publisherConnectionFactory.switchesInProgress", Map.class))
				.isEmpty();
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
		Channel toBeOrphaned = template.execute(channel -> channel);
		exec.execute(task);
		ctx = context.poll(10, TimeUnit.SECONDS);
		assertThat(ctx).isNotNull();
		tccf.switchContext(ctx);
		template.execute(channel -> null);
		assertThat(toBeOrphaned.isOpen()).isFalse();
		tccf.destroy();
	}

	@Test
	void unclaimed() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		Log log = spy(TestUtils.getPropertyValue(tccf, "logger", Log.class));
		new DirectFieldAccessor(tccf).setPropertyValue("logger", log);
		given(log.isWarnEnabled()).willReturn(true);
		Connection conn = tccf.createConnection();
		conn.createChannel(false);
		tccf.prepareSwitchContext();
		tccf.destroy();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(log).warn(captor.capture());
		assertThat(captor.getValue()).startsWith("Unclaimed context switches from threads:");
	}

	@Test
	void neitherBound() throws Exception {
		ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
		rabbitConnectionFactory.setHost("localhost");
		rabbitConnectionFactory.setAutomaticRecoveryEnabled(false);
		ThreadChannelConnectionFactory tccf = new ThreadChannelConnectionFactory(rabbitConnectionFactory);
		Log log1 = spy(TestUtils.getPropertyValue(tccf, "logger", Log.class));
		new DirectFieldAccessor(tccf).setPropertyValue("logger", log1);
		Log log2 = spy(TestUtils.getPropertyValue(tccf, "publisherConnectionFactory.logger", Log.class));
		new DirectFieldAccessor(tccf).setPropertyValue("publisherConnectionFactory.logger", log2);
		given(log1.isDebugEnabled()).willReturn(true);
		given(log2.isDebugEnabled()).willReturn(true);
		assertThat(tccf.prepareSwitchContext()).isNull();
		verify(log1).debug("No channels are bound to this thread");
		verify(log2).debug("No channels are bound to this thread");
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
