/*
 * Copyright 2015-2016 the original author or authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Gary Russell
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
		catch (IllegalStateException e) {
			assertThat(e.getMessage(), containsString("The ApplicationContext is closed"));
		}
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
