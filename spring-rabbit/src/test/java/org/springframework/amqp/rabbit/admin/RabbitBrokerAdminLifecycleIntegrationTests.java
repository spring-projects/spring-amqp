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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.EnvironmentAvailable;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.erlang.OtpException;

/**
 * @author Mark Pollack
 * @author Dave Syer
 */
public class RabbitBrokerAdminLifecycleIntegrationTests {

	private static Log logger = LogFactory.getLog(RabbitBrokerAdminLifecycleIntegrationTests.class);

	private static final String NODE_NAME = "spring@localhost";

	@Rule
	public Log4jLevelAdjuster logLevel = new Log4jLevelAdjuster(Level.INFO, RabbitBrokerAdmin.class);

	@Rule
	public static EnvironmentAvailable environment = new EnvironmentAvailable("BROKER_INTEGRATION_TEST");

	@Before
	public void init() throws Exception {
		FileUtils.deleteDirectory(new File("target/rabbitmq"));
	}

	@Test
	public void testStartNode() throws Exception {

		// Set up broker admin for non-root user
		final RabbitBrokerAdmin brokerAdmin = BrokerTestUtils.getRabbitBrokerAdmin(NODE_NAME);
		RabbitStatus status = brokerAdmin.getStatus();
		try {
			// Stop it if it is already running
			if (status.isReady()) {
				brokerAdmin.stopBrokerApplication();
				Thread.sleep(1000L);
			}
		} catch (OtpException e) {
			// Not useful for test.
		}
		status = brokerAdmin.getStatus();
		if (!status.isRunning()) {
			brokerAdmin.startBrokerApplication();
		}
		status = brokerAdmin.getStatus();

		try {

			assertFalse("Broker node did not start. Check logs for hints.", status.getNodes().isEmpty());
			assertTrue("Broker node not running.  Check logs for hints.", status.isRunning());
			assertTrue("Broker application not running.  Check logs for hints.", status.isReady());

			Thread.sleep(1000L);
			brokerAdmin.stopBrokerApplication();
			Thread.sleep(1000L);

		} finally {
			brokerAdmin.stopNode();
		}

	}

	@Test
	public void testStopAndStartBroker() throws Exception {

		// Set up broker admin for non-root user
		final RabbitBrokerAdmin brokerAdmin = BrokerTestUtils.getRabbitBrokerAdmin(NODE_NAME);
		RabbitStatus status = brokerAdmin.getStatus();

		status = brokerAdmin.getStatus();
		if (!status.isRunning()) {
			brokerAdmin.startBrokerApplication();
		}

		brokerAdmin.stopBrokerApplication();

		status = brokerAdmin.getStatus();
		assertEquals(0, status.getRunningNodes().size());

		brokerAdmin.startBrokerApplication();
		status = brokerAdmin.getStatus();
		assertBrokerAppRunning(status);

	}

	@Test
	public void repeatLifecycle() throws Exception {
		for (int i = 1; i <= 20; i++) {
			testStopAndStartBroker();
			Thread.sleep(200);
			if (i % 5 == 0) {
				logger.debug("i = " + i);
			}
		}
	}

	/**
	 * Asserts that the named-node is running.
	 * @param status
	 */
	private void assertBrokerAppRunning(RabbitStatus status) {
		assertEquals(1, status.getRunningNodes().size());
		assertTrue(status.getRunningNodes().get(0).getName().contains(NODE_NAME));
	}

}
