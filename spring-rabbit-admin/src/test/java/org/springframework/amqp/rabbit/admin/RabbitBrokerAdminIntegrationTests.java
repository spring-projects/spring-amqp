/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;

/**
 * 
 * This test class assumes that you are already running the rabbitmq broker.
 * 
 * @author Mark Pollack
 */
public class RabbitBrokerAdminIntegrationTests {

	private static Log logger = LogFactory
			.getLog(RabbitBrokerAdminIntegrationTests.class);

	private static RabbitBrokerAdmin brokerAdmin;

	private static SingleConnectionFactory connectionFactory;

	@BeforeClass
	public static void setUp() throws Exception {
		connectionFactory = new SingleConnectionFactory();
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		brokerAdmin = new RabbitBrokerAdmin(connectionFactory);
		RabbitStatus status = brokerAdmin.getStatus();
		if (status.getNodes().isEmpty()) {
			brokerAdmin.startNode();
			Thread.sleep(1000L);
		} else {
			brokerAdmin.startBrokerApplication();
		}
	}
	
	@AfterClass
	public static void close() {
		brokerAdmin.stopNode();
	}

	@Test
	// @Ignore
	public void integrationTestsUserCrud() throws Exception {
		List<String> users = brokerAdmin.listUsers();
		if (users.contains("joe")) {
			brokerAdmin.deleteUser("joe");
		}
		Thread.sleep(1000L);
		brokerAdmin.addUser("joe", "trader");
		Thread.sleep(1000L);
		brokerAdmin.changeUserPassword("joe", "sales");
		Thread.sleep(1000L);
		users = brokerAdmin.listUsers();
		if (users.contains("joe")) {
			Thread.sleep(1000L);
			brokerAdmin.deleteUser("joe");
		}
	}

	@Test
	public void repeatLifecycle() throws Exception {
		for (int i = 1; i < 20; i++) {
			testStatusAndBrokerLifecycle();
			if (i % 5 == 0) {
				logger.debug("i = " + i);
			}
		}
	}

	// @Test
	public void testStatusAndBrokerLifecycle() throws Exception {

		RabbitStatus status = brokerAdmin.getStatus();

		brokerAdmin.stopBrokerApplication();
		status = brokerAdmin.getStatus();
		assertEquals(0, status.getRunningNodes().size());

		brokerAdmin.startBrokerApplication();
		status = brokerAdmin.getStatus();
		assertBrokerAppRunning(status);
	}

	@Test
	public void testGetQueues() throws Exception {
		brokerAdmin.declareQueue(new Queue("test.queue"));
		assertEquals("/", connectionFactory.getVirtualHost());
		List<QueueInfo> queues = brokerAdmin.getQueues();
		assertEquals("test.queue", queues.get(0).getName());
	}

	private void assertBrokerAppRunning(RabbitStatus status) {
		assertEquals(1, status.getRunningNodes().size());
		assertTrue(status.getRunningNodes().get(0).getName().contains("rabbit"));
	}

}
