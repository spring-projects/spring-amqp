/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.test.BrokerPanic;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.util.exec.Os;

/**
 * 
 * @author Mark Pollack
 * @author Dave Syer
 */
public class RabbitBrokerAdminIntegrationTests {

	private static Log logger = LogFactory.getLog(RabbitBrokerAdminIntegrationTests.class);

	@Rule
	public Log4jLevelAdjuster logLevel = new Log4jLevelAdjuster(Level.INFO, RabbitBrokerAdmin.class);

	/*
	 * Ensure broker dies if a test fails (otherwise the erl process has to be killed manually)
	 */
	@Rule
	public BrokerPanic panic = new BrokerPanic();

	private static RabbitBrokerAdmin brokerAdmin;

	@Before
	public void init() throws Exception {
		panic.setBrokerAdmin(brokerAdmin);
	}

	@BeforeClass
	public static void start() throws Exception {
		brokerAdmin = new RabbitBrokerAdmin("spring@localhost", 15672);
		brokerAdmin.setRabbitLogBaseDirectory("target/rabbitmq/log");
		brokerAdmin.setRabbitMnesiaBaseDirectory("target/rabbitmq/mnesia");
		brokerAdmin.setStartupTimeout(10000L);
		brokerAdmin.startNode();
	}

	@AfterClass
	public static void stop() throws Exception {
		if (Os.isFamily("windows") || Os.isFamily("dos")) {
			brokerAdmin.stopNode();
		}
	}

	@Test
	public void integrationTestsUserCrud() throws Exception {
		List<String> users = brokerAdmin.listUsers();
		if (users.contains("joe")) {
			brokerAdmin.deleteUser("joe");
		}
		Thread.sleep(200L);
		brokerAdmin.addUser("joe", "trader");
		Thread.sleep(200L);
		brokerAdmin.changeUserPassword("joe", "sales");
		Thread.sleep(200L);
		users = brokerAdmin.listUsers();
		if (users.contains("joe")) {
			Thread.sleep(200L);
			brokerAdmin.deleteUser("joe");
		}
	}

	@Test
	public void testStatusAndBrokerLifecycle() throws Exception {

		brokerAdmin.stopBrokerApplication();
		RabbitStatus status = brokerAdmin.getStatus();
		assertEquals(0, status.getRunningNodes().size());

		brokerAdmin.startBrokerApplication();
		status = brokerAdmin.getStatus();
		assertBrokerAppRunning(status);

	}

	@Test
	public void repeatLifecycle() throws Exception {
		for (int i = 1; i <= 20; i++) {
			testStatusAndBrokerLifecycle();
			Thread.sleep(200);
			if (i % 5 == 0) {
				logger.debug("i = " + i);
			}
		}
	}

	@Test
	public void testGetQueues() throws Exception {
		ConnectionFactory connectionFactory = new SingleConnectionFactory();
		new RabbitAdmin(connectionFactory).declareQueue(new Queue("test.queue"));
		assertEquals("/", connectionFactory.getVirtualHost());
		List<QueueInfo> queues = brokerAdmin.getQueues();
		assertEquals("test.queue", queues.get(0).getName());
	}

	private void assertBrokerAppRunning(RabbitStatus status) {
		assertEquals(1, status.getRunningNodes().size());
		assertTrue(status.getRunningNodes().get(0).getName().contains("spring@localhost"));
	}

}
