/*
 * Copyright 2015-present the original author or authors.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.impl.AMQCommand;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.AMQImpl;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 * @since 1.5.3
 *
 */
@RabbitAvailable
public class ConnectionFactoryLifecycleTests {

	@Test
	public void testConnectionFactoryAvailableDuringStop() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		MyLifecycle myLifecycle = context.getBean(MyLifecycle.class);
		CachingConnectionFactory cf = context.getBean(CachingConnectionFactory.class);
		context.close();
		assertThat(myLifecycle.isRunning()).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(cf, "stopped")).isTrue();
		assertThatExceptionOfType(AmqpApplicationContextClosedException.class)
				.isThrownBy(cf::createConnection)
				.withMessageContaining("The ApplicationContext is closed");
	}

	@Test
	public void testBlockedConnection() throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);

		AtomicReference<ConnectionBlockedEvent> blockedConnectionEvent = new AtomicReference<>();
		AtomicReference<ConnectionUnblockedEvent> unblockedConnectionEvent = new AtomicReference<>();

		context.addApplicationListener((ApplicationListener<ConnectionBlockedEvent>) blockedConnectionEvent::set);

		context.addApplicationListener((ApplicationListener<ConnectionUnblockedEvent>) unblockedConnectionEvent::set);

		CachingConnectionFactory cf = context.getBean(CachingConnectionFactory.class);

		CountDownLatch blockedConnectionLatch = new CountDownLatch(1);
		CountDownLatch unblockedConnectionLatch = new CountDownLatch(1);

		Connection connection = cf.createConnection();
		connection.addBlockedListener(new BlockedListener() {

			@Override
			public void handleBlocked(String reason) {
				blockedConnectionLatch.countDown();
			}

			@Override
			public void handleUnblocked() {
				unblockedConnectionLatch.countDown();
			}

		});

		AMQConnection amqConnection = TestUtils.propertyValue(connection, "target.delegate");
		amqConnection.processControlCommand(new AMQCommand(new AMQImpl.Connection.Blocked("Test connection blocked")));

		assertThat(blockedConnectionLatch.await(10, TimeUnit.SECONDS)).isTrue();

		ConnectionBlockedEvent connectionBlockedEvent = blockedConnectionEvent.get();
		assertThat(connectionBlockedEvent).isNotNull();
		assertThat(connectionBlockedEvent.getReason()).isEqualTo("Test connection blocked");
		assertThat(connectionBlockedEvent.getConnection()).isSameAs(TestUtils.getPropertyValue(connection, "target"));

		amqConnection.processControlCommand(new AMQCommand(new AMQImpl.Connection.Unblocked()));

		assertThat(unblockedConnectionLatch.await(10, TimeUnit.SECONDS)).isTrue();

		ConnectionUnblockedEvent connectionUnblockedEvent = unblockedConnectionEvent.get();
		assertThat(connectionUnblockedEvent).isNotNull();
		assertThat(connectionUnblockedEvent.getConnection()).isSameAs(TestUtils.getPropertyValue(connection, "target"));
		context.close();
	}

	@Configuration
	public static class Config {

		@Bean
		public ConnectionFactory cf() {
			return new CachingConnectionFactory("localhost", BrokerTestUtils.DEFAULT_PORT);
		}

		@Bean
		public MyLifecycle myLifeCycle() {
			return new MyLifecycle(cf());
		}

	}

	public static class MyLifecycle implements SmartLifecycle {

		private final RabbitAdmin admin;

		private final Queue queue = new AnonymousQueue();

		private volatile boolean running;

		public MyLifecycle(ConnectionFactory cf) {
			this.admin = new RabbitAdmin(cf);
		}

		@Override
		public void start() {
			this.running = true;
			this.admin.declareQueue(this.queue);
		}

		@Override
		public void stop() {
			// Prior to the fix for AMQP-546, this threw an exception and
			// running was not reset.
			this.admin.deleteQueue(this.queue.getName());
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

		@Override
		public int getPhase() {
			return 0;
		}

		@Override
		public boolean isAutoStartup() {
			return true;
		}

		@Override
		public void stop(Runnable callback) {
			stop();
			callback.run();
		}

	}

}
