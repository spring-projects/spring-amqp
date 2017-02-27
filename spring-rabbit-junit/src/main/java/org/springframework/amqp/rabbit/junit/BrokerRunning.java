/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.springframework.util.Base64Utils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.http.client.Client;

/**
 * A rule that prevents integration tests from failing if the Rabbit broker application is
 * not running or not accessible. If the Rabbit broker is not running in the background
 * all the tests here will simply be skipped (by default) because of a violated assumption
 * (showing as successful). Usage:
 *
 * <pre class="code">
 * &#064;Rule
 * public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();
 *
 * &#064;Test
 * public void testSendAndReceive() throws Exception {
 * 	// ... test using RabbitTemplate etc.
 * }
 * </pre>
 *
 * The rule can be declared as static so that it only has to check once for all tests in
 * the enclosing test case, but there isn't a lot of overhead in making it non-static.
 * <p>Use {@link #isRunningWithEmptyQueues(String...)} to declare and/or purge test queue(s)
 * when the rule is run.
 * <p>Call {@link #removeTestQueues(String...)} from an {@code @After} method to remove
 * those queues (and optionally others).
 * <p>If you wish to enforce the broker being available, for example, on a CI server,
 * set the environment variable {@value #BROKER_REQUIRED} to {@code true} and the
 * tests will fail fast.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 * @since 1.7
 * @see Assume
 * @see AssumptionViolatedException
 */
public final class BrokerRunning extends TestWatcher {

	public static final String BROKER_REQUIRED = "RABBITMQ_SERVER_REQUIRED";

	private static final String DEFAULT_QUEUE_NAME = BrokerRunning.class.getName();

	private static Log logger = LogFactory.getLog(BrokerRunning.class);

	// Static so that we only test once on failure: speeds up test suite
	private static Map<Integer, Boolean> brokerOnline = new HashMap<Integer, Boolean>();

	// Static so that we only test once on failure
	private static Map<Integer, Boolean> brokerOffline = new HashMap<Integer, Boolean>();

	private final boolean assumeOnline;

	private final boolean purge;

	private final boolean management;

	private final String[] queues;

	private final int defaultPort = BrokerTestUtils.getPort();

	private int port;

	private String hostName = null;

	private ConnectionFactory connectionFactory;

	/**
	 * Ensure the broker is running and has a empty queue(s) with the specified name(s) in the
	 * default exchange.
	 *
	 * @param names the queues to declare for the test.
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunningWithEmptyQueues(String... names) {
		return new BrokerRunning(true, true, names);
	}

	/**
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunning() {
		return new BrokerRunning(true);
	}

	/**
	 * @return a new rule that assumes there is no existing broker
	 */
	public static BrokerRunning isNotRunning() {
		return new BrokerRunning(false);
	}

	/**
	 * @return a new rule that assumes an existing broker with the management plugin
	 */
	public static BrokerRunning isBrokerAndManagementRunning() {
		return new BrokerRunning(true, false, true);
	}

	/**
	 * @param queues the queues.
	 * @return a new rule that assumes an existing broker with the management plugin with
	 * the provided queues declared (and emptied if needed)..
	 */
	public static BrokerRunning isBrokerAndManagementRunningWithEmptyQueues(String...queues) {
		return new BrokerRunning(true, false, true, queues);
	}

	private BrokerRunning(boolean assumeOnline, boolean purge, String... queues) {
		this(assumeOnline, purge, false, queues);
	}

	private BrokerRunning(boolean assumeOnline, boolean purge, boolean management, String... queues) {
		this.assumeOnline = assumeOnline;
		this.queues = queues;
		this.purge = purge;
		this.management = management;
		setPort(this.defaultPort);
	}

	private BrokerRunning(boolean assumeOnline, String... queues) {
		this(assumeOnline, false, queues);
	}

	private BrokerRunning(boolean assumeOnline) {
		this(assumeOnline, DEFAULT_QUEUE_NAME);
	}

	private BrokerRunning(boolean assumeOnline, boolean purge, boolean management) {
		this(assumeOnline, purge, management, DEFAULT_QUEUE_NAME);
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
		if (!brokerOffline.containsKey(port)) {
			brokerOffline.put(port, true);
		}
		if (!brokerOnline.containsKey(port)) {
			brokerOnline.put(port, true);
		}
	}

	/**
	 * @param hostName the hostName to set
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	@Override
	public Statement apply(Statement base, Description description) {

		// Check at the beginning, so this can be used as a static field
		if (this.assumeOnline) {
			Assume.assumeTrue(brokerOnline.get(this.port));
		}
		else {
			Assume.assumeTrue(brokerOffline.get(this.port));
		}

		ConnectionFactory connectionFactory = getConnectionFactory();

		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			connection.setId(generateId());
			channel = connection.createChannel();

			for (String queueName : this.queues) {

				if (this.purge) {
					logger.debug("Deleting queue: " + queueName);
					// Delete completely - gets rid of consumers and bindings as well
					channel.queueDelete(queueName);
				}

				if (isDefaultQueue(queueName)) {
					// Just for test probe.
					channel.queueDelete(queueName);
				}
				else {
					channel.queueDeclare(queueName, true, false, false, null);
				}
			}
			brokerOffline.put(this.port, false);
			if (!this.assumeOnline) {
				Assume.assumeTrue(brokerOffline.get(this.port));
			}

			if (this.management) {
				Client client = new Client("http://localhost:15672/api/", "guest", "guest");
				if (!client.alivenessTest("/")) {
					throw new RuntimeException("Aliveness test failed for localhost:15672 guest/quest; "
							+ "management not available");
				}
			}
		}
		catch (Exception e) {
			logger.warn("Not executing tests because basic connectivity test failed: " + e.getMessage());
			brokerOnline.put(this.port, false);
			if (this.assumeOnline) {
				if (fatal()) {
					fail("RabbitMQ Broker is required, but not available");
				}
				else {
					Assume.assumeNoException(e);
				}
			}
		}
		finally {
			closeResources(connection, channel);
		}

		return super.apply(base, description);
	}

	private boolean fatal() {
		String serversRequired = System.getenv(BROKER_REQUIRED);
		if (Boolean.parseBoolean(serversRequired)) {
			logger.error("RABBITMQ IS REQUIRED BUT NOT AVAILABLE");
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Generate the connection id for the connection used by the rule's
	 * connection factory.
	 * @return the id.
	 */
	public String generateId() {
		UUID uuid = UUID.randomUUID();
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits())
		  .putLong(uuid.getLeastSignificantBits());
		return "SpringBrokerRunning." + Base64Utils.encodeToUrlSafeString(bb.array()).replaceAll("=", "");
	}

	private boolean isDefaultQueue(String queue) {
		return DEFAULT_QUEUE_NAME.equals(queue);
	}

	/**
	 * Remove any test queues that were created by an
	 * {@link #isRunningWithEmptyQueues(String...)} method.
	 * @param additionalQueues additional queues to remove that might have been created by
	 * tests.
	 */
	public void removeTestQueues(String... additionalQueues) {
		List<String> queuesToRemove = Arrays.asList(this.queues);
		if (additionalQueues != null) {
			queuesToRemove = new ArrayList<>(queuesToRemove);
			queuesToRemove.addAll(Arrays.asList(additionalQueues));
		}
		logger.debug("deleting test queues: " + queuesToRemove);
		ConnectionFactory connectionFactory = getConnectionFactory();
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			connection.setId(generateId() + ".queueDelete");
			channel = connection.createChannel();

			for (String queue : queuesToRemove) {
				channel.queueDelete(queue);
			}
		}
		catch (Exception e) {
			logger.warn("Failed to delete queues", e);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	/**
	 * Delete arbitrary queues from the broker.
	 * @param queues the queues to delete.
	 */
	public void deleteQueues(String... queues) {
		ConnectionFactory connectionFactory = getConnectionFactory();
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			connection.setId(generateId() + ".queueDelete");
			channel = connection.createChannel();

			for (String queue : queues) {
				channel.queueDelete(queue);
			}
		}
		catch (Exception e) {
			logger.warn("Failed to delete queues", e);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	/**
	 * Delete arbitrary exchanges from the broker.
	 * @param exchanges the exchanges to delete.
	 */
	public void deleteExchanges(String... exchanges) {
		ConnectionFactory connectionFactory = getConnectionFactory();
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = connectionFactory.newConnection();
			connection.setId(generateId() + ".exchangeDelete");
			channel = connection.createChannel();

			for (String exchange : exchanges) {
				channel.exchangeDelete(exchange);
			}
		}
		catch (Exception e) {
			logger.warn("Failed to delete queues", e);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	/**
	 * Get the connection factory used by this rule.
	 * @return the connection factory.
	 */
	public ConnectionFactory getConnectionFactory() {
		if (this.connectionFactory == null) {
			this.connectionFactory = new ConnectionFactory();
			if (StringUtils.hasText(this.hostName)) {
				this.connectionFactory.setHost(this.hostName);
			}
			else {
				this.connectionFactory.setHost("localhost");
			}
			this.connectionFactory.setPort(this.port);
		}
		return this.connectionFactory;
	}

	private void closeResources(Connection connection, Channel channel) {
		if (channel != null) {
			try {
				channel.close();
			}
			catch (IOException | TimeoutException e) {
				// Ignore
			}
		}
		if (connection != null) {
			try {
				connection.close();
			}
			catch (IOException e) {
				// Ignore
			}
		}
	}

}
