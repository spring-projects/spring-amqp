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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.erlang.OtpException;

/**
 * @author Mark Pollack
 * @author Dave Syer
 */
public class RabbitBrokerAdminStopIntegrationTests {

	@Rule
	public Log4jLevelAdjuster logLevel = new Log4jLevelAdjuster(Level.INFO, RabbitBrokerAdmin.class);

	@Before
	public void init() throws Exception {
		FileUtils.deleteDirectory(new File("target/rabbitmq"));
	}

	@Test
	public void testStartNode() throws Exception {

		// Set up broker admin for non-root user
		final RabbitBrokerAdmin brokerAdmin = new RabbitBrokerAdmin("spring@localhost", 15672);
		brokerAdmin.setRabbitLogBaseDirectory("target/rabbitmq/log");
		brokerAdmin.setRabbitMnesiaBaseDirectory("target/rabbitmq/mnesia");

		brokerAdmin.setStartupTimeout(10000L);

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

}
