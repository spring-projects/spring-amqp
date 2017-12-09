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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
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

	public static final String BROKER_ADMIN_URI = "RABBITMQ_TEST_ADMIN_URI";

	public static final String BROKER_HOSTNAME = "RABBITMQ_TEST_HOSTNAME";

	public static final String BROKER_PORT = "RABBITMQ_TEST_PORT";

	public static final String BROKER_USER = "RABBITMQ_TEST_USER";

	public static final String BROKER_PW = "RABBITMQ_TEST_PASSWORD";

	public static final String BROKER_ADMIN_USER = "RABBITMQ_TEST_ADMIN_USER";

	public static final String BROKER_ADMIN_PW = "RABBITMQ_TEST_ADMIN_PASSWORD";

	public static final String BROKER_REQUIRED = "RABBITMQ_SERVER_REQUIRED";

	private static final String DEFAULT_QUEUE_NAME = BrokerRunning.class.getName();

	private static final Log logger = LogFactory.getLog(BrokerRunning.class);

	// Static so that we only test once on failure: speeds up test suite
	private static final Map<Integer, Boolean> brokerOnline = new HashMap<Integer, Boolean>();

	// Static so that we only test once on failure
	private static final Map<Integer, Boolean> brokerOffline = new HashMap<Integer, Boolean>();

	private static final Map<String, String> environmentOverrides = new HashMap<>();

	private final boolean assumeOnline;

	private final boolean purge;

	private final boolean management;

	private final String[] queues;

	private final int defaultPort = fromEnvironment(BROKER_PORT, null) == null ? BrokerTestUtils.getPort()
			: Integer.valueOf(fromEnvironment(BROKER_PORT, null));

	private int port;

	private String hostName = fromEnvironment(BROKER_HOSTNAME, "localhost");

	private String adminUri = fromEnvironment(BROKER_ADMIN_URI, null);

	private ConnectionFactory connectionFactory;

	private String user = fromEnvironment(BROKER_USER, "guest");

	private String password = fromEnvironment(BROKER_PW, "guest");

	private String adminUser = fromEnvironment(BROKER_ADMIN_USER, "guest");

	private String adminPassword = fromEnvironment(BROKER_ADMIN_PW, "guest");

	private String fromEnvironment(String key, String defaultValue) {
		String environmentValue = environmentOverrides.get(key);
		if (!StringUtils.hasText(environmentValue)) {
			environmentValue = System.getenv(key);
		}
		if (StringUtils.hasText(environmentValue)) {
			return environmentValue;
		}
		else {
			return defaultValue;
		}
	}

	/**
	 * Set environment variable overrides for host, port etc. Will override any real
	 * environment variables, if present.
	 * <p><b>The variables will only apply to rule instances that are created after this
	 * method is called.</b>
	 * The overrides will remain until
	 * @param environmentVariables the variables.
	 */
	public static void setEnvironmentVariableOverrides(Map<String, String> environmentVariables) {
		environmentOverrides.putAll(environmentVariables);
	}

	/**
	 * Clear any environment variable overrides set in {@link #setEnvironmentVariableOverrides(Map)}.
	 */
	public static void clearEnvironmentVariableOverrides() {
		environmentOverrides.clear();
	}

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

	/**
	 * Set the user for the amqp connection default "guest".
	 * @param user the user.
	 * @since 1.7.2
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * Set the password for the amqp connection default "guest".
	 * @param password the password.
	 * @since 1.7.2
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Set the uri for the REST API.
	 * @param adminUri the uri.
	 * @since 1.7.2
	 */
	public void setAdminUri(String adminUri) {
		this.adminUri = adminUri;
	}

	/**
	 * Set the user for the management REST API connection default "guest".
	 * @param user the user.
	 * @since 1.7.2
	 */
	public void setAdminUser(String user) {
		this.adminUser = user;
	}

	/**
	 * Set the password for the management REST API connection default "guest".
	 * @param password the password.
	 * @since 1.7.2
	 */
	public void setAdminPassword(String password) {
		this.adminPassword = password;
	}

	/**
	 * Return the port.
	 * @return the port.
	 * @since 1.7.2
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * Return the port.
	 * @return the port.
	 * @since 1.7.2
	 */
	public String getHostName() {
		return this.hostName;
	}

	/**
	 * Return the user.
	 * @return the user.
	 * @since 1.7.2
	 */
	public String getUser() {
		return this.user;
	}

	/**
	 * Return the password.
	 * @return the password.
	 * @since 1.7.2
	 */
	public String getPassword() {
		return this.password;
	}

	/**
	 * Return the admin user.
	 * @return the user.
	 * @since 1.7.2
	 */
	public String getAdminUser() {
		return this.adminUser;
	}

	/**
	 * Return the admin password.
	 * @return the password.
	 * @since 1.7.2
	 */
	public String getAdminPassword() {
		return this.adminPassword;
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
			connection = getConnection(connectionFactory);
			channel = createQueues(connection);
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

	public void isUp() throws Exception {
		Connection connection = getConnectionFactory().newConnection();
		Channel channel = null;
		try {
			channel = createQueues(connection);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	private Connection getConnection(ConnectionFactory connectionFactory) throws IOException, TimeoutException {
		Connection connection = connectionFactory.newConnection();
		connection.setId(generateId());
		return connection;
	}

	private Channel createQueues(Connection connection) throws IOException, MalformedURLException, URISyntaxException {
		Channel channel;
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
			Client client = new Client(getAdminUri(), this.adminUser, this.adminPassword);
			if (!client.alivenessTest("/")) {
				throw new RuntimeException("Aliveness test failed for localhost:15672 guest/quest; "
						+ "management not available");
			}
		}
		return channel;
	}

	public static boolean fatal() {
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
			connection = getConnection(connectionFactory);
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
			connection = getConnection(connectionFactory);
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
			connection = getConnection(connectionFactory);
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
			this.connectionFactory.setUsername(this.user);
			this.connectionFactory.setPassword(this.password);
			this.connectionFactory.setAutomaticRecoveryEnabled(false);
		}
		return this.connectionFactory;
	}

	/**
	 * Return the admin uri.
	 * @return the uri.
	 * @since 1.7.2
	 */
	public String getAdminUri() {
		if (!StringUtils.hasText(this.adminUri)) {
			if (!StringUtils.hasText(this.hostName)) {
				this.adminUri = "http://localhost:15672/api/";
			}
			else {
				this.adminUri = "http://" + this.hostName + ":15672/api/";
			}
		}
		return this.adminUri;
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
