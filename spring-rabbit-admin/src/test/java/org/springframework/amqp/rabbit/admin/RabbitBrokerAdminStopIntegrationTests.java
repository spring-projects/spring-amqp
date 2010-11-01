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

import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.erlang.OtpIOException;

/**
 * @author Mark Pollack
 * @author Dave Syer
 */
public class RabbitBrokerAdminStopIntegrationTests {

	@Test
	// @Ignore("NEEDS RABBITMQ_HOME to be set.")
	public void testStartNode() throws Exception {

		RabbitBrokerAdmin brokerAdmin;

		SingleConnectionFactory connectionFactory;

		connectionFactory = new SingleConnectionFactory();
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		brokerAdmin = new RabbitBrokerAdmin(connectionFactory);

		RabbitStatus status = brokerAdmin.getStatus();
		try {
			// Stop it if it is already running
			if (status.getRunningApplications().size() > 0) {
				brokerAdmin.stopBrokerApplication();
				Thread.sleep(1000L);
			}
		} catch (OtpIOException e) {
			// Not useful for test.
		}
		status = brokerAdmin.getStatus();
		if (status.getNodes().isEmpty()) {
			brokerAdmin.startNode();
		} else {
			brokerAdmin.startBrokerApplication();
		}
		Thread.sleep(1000L);
		status = brokerAdmin.getStatus();

		assertFalse("Broker node did not start.  Check logs for hints.", status
				.getNodes().isEmpty());

		try {
			assertFalse("Broker node not running.  Check logs for hints.",
					status.getRunningNodes().isEmpty());
			assertFalse(
					"Broker application not running.  Check logs for hints.",
					status.getRunningApplications().isEmpty());

			// assertEquals(1, 1);
			brokerAdmin.stopBrokerApplication();
			Thread.sleep(1000L);
		} finally {
			brokerAdmin.stopNode();
			Thread.sleep(2000L);
		}
	}

}
