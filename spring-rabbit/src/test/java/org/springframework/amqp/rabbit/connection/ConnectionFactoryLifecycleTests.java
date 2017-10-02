/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.AmqpApplicationContextClosedException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.impl.AMQCommand;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.AMQImpl;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5.3
 *
 */
public class ConnectionFactoryLifecycleTests {

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	@Test
	public void testConnectionFactoryAvailableDuringStop() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		MyLifecycle myLifecycle = context.getBean(MyLifecycle.class);
		CachingConnectionFactory cf = context.getBean(CachingConnectionFactory.class);
		context.close();
		assertFalse(myLifecycle.isRunning());
		assertTrue(TestUtils.getPropertyValue(cf, "stopped", Boolean.class));
		try {
			cf.createConnection();
			fail("Expected exception");
		}
		catch (AmqpApplicationContextClosedException e) {
			assertThat(e.getMessage(), containsString("The ApplicationContext is closed"));
		}
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
			public void handleBlocked(String reason) throws IOException {
				blockedConnectionLatch.countDown();
			}

			@Override
			public void handleUnblocked() throws IOException {
				unblockedConnectionLatch.countDown();
			}

		});

		AMQConnection amqConnection = TestUtils.getPropertyValue(connection, "target.delegate", AMQConnection.class);
		amqConnection.processControlCommand(new AMQCommand(new AMQImpl.Connection.Blocked("Test connection blocked")));

		assertTrue(blockedConnectionLatch.await(10, TimeUnit.SECONDS));

		ConnectionBlockedEvent connectionBlockedEvent = blockedConnectionEvent.get();
		assertNotNull(connectionBlockedEvent);
		assertEquals("Test connection blocked", connectionBlockedEvent.getReason());
		assertSame(TestUtils.getPropertyValue(connection, "target"), connectionBlockedEvent.getConnection());

		amqConnection.processControlCommand(new AMQCommand(new AMQImpl.Connection.Unblocked()));

		assertTrue(unblockedConnectionLatch.await(10, TimeUnit.SECONDS));

		ConnectionUnblockedEvent connectionUnblockedEvent = unblockedConnectionEvent.get();
		assertNotNull(connectionUnblockedEvent);
		assertSame(TestUtils.getPropertyValue(connection, "target"), connectionUnblockedEvent.getConnection());
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
