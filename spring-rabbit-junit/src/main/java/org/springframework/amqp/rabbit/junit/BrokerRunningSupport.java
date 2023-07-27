/*
 * Copyright 2002-2023 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A class that can be used to prevent integration tests from failing if the Rabbit broker application is
 * not running or not accessible. If the Rabbit broker is not running in the background
 * all the tests here will simply be skipped (by default) because of a violated assumption
 * (showing as successful).
 * <p>
 * If you wish to enforce the broker being available, for example, on a CI server,
 * set the environment variable {@value #BROKER_REQUIRED} to {@code true} and the
 * tests will fail fast.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 * @since 2.2
 */
public final class BrokerRunningSupport {

	private static final int SIXTEEN = 16;

	public static final String BROKER_ADMIN_URI = "RABBITMQ_TEST_ADMIN_URI";

	public static final String BROKER_HOSTNAME = "RABBITMQ_TEST_HOSTNAME";

	public static final String BROKER_PORT = "RABBITMQ_TEST_PORT";

	public static final String BROKER_USER = "RABBITMQ_TEST_USER";

	public static final String BROKER_PW = "RABBITMQ_TEST_PASSWORD";

	public static final String BROKER_ADMIN_USER = "RABBITMQ_TEST_ADMIN_USER";

	public static final String BROKER_ADMIN_PW = "RABBITMQ_TEST_ADMIN_PASSWORD";

	public static final String BROKER_REQUIRED = "RABBITMQ_SERVER_REQUIRED";

	public static final String DEFAULT_QUEUE_NAME = BrokerRunningSupport.class.getName();

	private static final String GUEST = "guest";

	private static final Log LOGGER = LogFactory.getLog(BrokerRunningSupport.class);

	// Static so that we only test once on failure: speeds up test suite
	private static final Map<Integer, Boolean> BROKER_ONLINE = new HashMap<>();

	private static final Map<String, String> ENVIRONMENT_OVERRIDES = new HashMap<>();

	private final boolean purge;

	private final boolean management;

	private final String[] queues;

	private int port;

	private String hostName = fromEnvironment(BROKER_HOSTNAME, "localhost");

	private String adminUri = fromEnvironment(BROKER_ADMIN_URI, null);

	private ConnectionFactory connectionFactory;

	private String user = fromEnvironment(BROKER_USER, GUEST);

	private String password = fromEnvironment(BROKER_PW, GUEST);

	private String adminUser = fromEnvironment(BROKER_ADMIN_USER, GUEST);

	private String adminPassword = fromEnvironment(BROKER_ADMIN_PW, GUEST);

	private boolean purgeAfterEach;

	private static String fromEnvironment(String key, String defaultValue) {
		String environmentValue = ENVIRONMENT_OVERRIDES.get(key);
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
		ENVIRONMENT_OVERRIDES.putAll(environmentVariables);
	}

	/**
	 * Clear any environment variable overrides set in {@link #setEnvironmentVariableOverrides(Map)}.
	 */
	public static void clearEnvironmentVariableOverrides() {
		ENVIRONMENT_OVERRIDES.clear();
	}

	/**
	 * Ensure the broker is running and has a empty queue(s) with the specified name(s) in the
	 * default exchange.
	 *
	 * @param names the queues to declare for the test.
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunningSupport isRunningWithEmptyQueues(String... names) {
		return new BrokerRunningSupport(true, names);
	}

	/**
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunningSupport isRunning() {
		return new BrokerRunningSupport(true);
	}

	/**
	 * @return a new rule that assumes there is no existing broker
	 */
	public static BrokerRunningSupport isNotRunning() {
		return new BrokerRunningSupport(false);
	}

	/**
	 * @return a new rule that assumes an existing broker with the management plugin
	 */
	public static BrokerRunningSupport isBrokerAndManagementRunning() {
		return new BrokerRunningSupport(false, true);
	}

	/**
	 * @param queues the queues.
	 * @return a new rule that assumes an existing broker with the management plugin with
	 * the provided queues declared (and emptied if needed)..
	 */
	public static BrokerRunningSupport isBrokerAndManagementRunningWithEmptyQueues(String... queues) {
		return new BrokerRunningSupport(true, true, queues);
	}

	private BrokerRunningSupport(boolean purge, String... queues) {
		this(purge, false, queues);
	}

	BrokerRunningSupport(boolean purge, boolean management, String... queues) {
		if (queues != null) {
			this.queues = Arrays.copyOf(queues, queues.length);
		}
		else {
			this.queues = null;
		}
		this.purge = purge;
		this.management = management;
		setPort(fromEnvironment(BROKER_PORT, null) == null
				? BrokerTestUtils.getPort()
				: Integer.valueOf(fromEnvironment(BROKER_PORT, null)));
	}

	private BrokerRunningSupport(boolean assumeOnline) {
		this(assumeOnline, DEFAULT_QUEUE_NAME);
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
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
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * Set the password for the amqp connection default "guest".
	 * @param password the password.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Set the uri for the REST API.
	 * @param adminUri the uri.
	 */
	public void setAdminUri(String adminUri) {
		this.adminUri = adminUri;
	}

	/**
	 * Set the user for the management REST API connection default "guest".
	 * @param user the user.
	 */
	public void setAdminUser(String user) {
		this.adminUser = user;
	}

	/**
	 * Set the password for the management REST API connection default "guest".
	 * @param password the password.
	 */
	public void setAdminPassword(String password) {
		this.adminPassword = password;
	}

	/**
	 * Return the port.
	 * @return the port.
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * Return the port.
	 * @return the port.
	 */
	public String getHostName() {
		return this.hostName;
	}

	/**
	 * Return the user.
	 * @return the user.
	 */
	public String getUser() {
		return this.user;
	}

	/**
	 * Return the password.
	 * @return the password.
	 */
	public String getPassword() {
		return this.password;
	}

	/**
	 * Return the admin user.
	 * @return the user.
	 */
	public String getAdminUser() {
		return this.adminUser;
	}

	/**
	 * Return the admin password.
	 * @return the password.
	 */
	public String getAdminPassword() {
		return this.adminPassword;
	}


	public boolean isPurgeAfterEach() {
		return this.purgeAfterEach;
	}

	/**
	 * Purge the test queues after each test (JUnit 5).
	 * @param purgeAfterEach true to purge.
	 */
	public void setPurgeAfterEach(boolean purgeAfterEach) {
		this.purgeAfterEach = purgeAfterEach;
	}

	/**
	 * Check connectivity to the broker and create any queues.
	 * @throws BrokerNotAliveException if the broker is not available.
	 */
	public void test() throws BrokerNotAliveException {

		// Check at the beginning, so this can be used as a static field
		if (Boolean.FALSE.equals(BROKER_ONLINE.get(this.port))) {
			throw new BrokerNotAliveException("Require broker online and it's not");
		}

		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = getConnection(getConnectionFactory());
			channel = createQueues(connection);
		}
		catch (Exception e) {
			BROKER_ONLINE.put(this.port, false);
			throw new BrokerNotAliveException("RabbitMQ Broker is required, but not available", e);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	private Connection getConnection(ConnectionFactory cf) throws IOException, TimeoutException {
		Connection connection = cf.newConnection();
		connection.setId(generateId());
		return connection;
	}

	private Channel createQueues(Connection connection) throws IOException, URISyntaxException {
		Channel channel;
		channel = connection.createChannel();

		for (String queueName : this.queues) {

			if (this.purge) {
				LOGGER.debug("Deleting queue: " + queueName);
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
		if (this.management) {
			alivenessTest();
		}
		return channel;
	}

	private void alivenessTest() throws URISyntaxException {
		HttpClient client = HttpClient.newBuilder()
				.authenticator(new Authenticator() {

					@Override
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(getAdminUser(), getAdminPassword().toCharArray());
					}

				})
				.build();
		URI uri = new URI(getAdminUri())
				.resolve("/api/aliveness-test/" + UriUtils.encodePathSegment("/", StandardCharsets.UTF_8));
		HttpRequest request = HttpRequest.newBuilder()
				.GET()
				.uri(uri)
				.build();
		HttpResponse<String> response;
		try {
			response = client.send(request, BodyHandlers.ofString());
		}
		catch (IOException ex) {
			throw new BrokerNotAliveException("Failed to check aliveness", ex);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new BrokerNotAliveException("Interrupted while checking aliveness", ex);
		}
		String body = null;
		if (response.statusCode() == HttpStatus.OK.value()) {
			body = response.body();
		}
		if (body == null || !body.contentEquals("{\"status\":\"ok\"}")) {
			throw new BrokerNotAliveException("Aliveness test failed for " + uri.toString()
					+ " user: " + getAdminUser() + " pw: " + getAdminPassword()
					+ " status: " + response.statusCode() + " body: " + body
					+ "; management not available");
		}
	}

	public static boolean fatal() {
		String serversRequired = System.getenv(BROKER_REQUIRED);
		if (Boolean.parseBoolean(serversRequired)) {
			LOGGER.error("RABBITMQ IS REQUIRED BUT NOT AVAILABLE");
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
		ByteBuffer bb = ByteBuffer.wrap(new byte[SIXTEEN]);
		bb.putLong(uuid.getMostSignificantBits())
		  .putLong(uuid.getLeastSignificantBits());
		return "SpringBrokerRunning." + Base64.getUrlEncoder().encodeToString(bb.array()).replaceAll("=", "");
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
		LOGGER.debug("deleting test queues: " + queuesToRemove);
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = getConnection(getConnectionFactory());
			connection.setId(generateId() + ".queueDelete");
			channel = connection.createChannel();

			for (String queue : queuesToRemove) {
				channel.queueDelete(queue);
			}
		}
		catch (Exception e) {
			LOGGER.warn("Failed to delete queues", e);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	/**
	 * Remove exchanges from the broker.
	 * @param exchanges the exchanges.
	 * @since 2.3
	 */
	public void removeExchanges(String... exchanges) {
		LOGGER.debug("deleting test exchanges: " + Arrays.toString(exchanges));
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = getConnection(getConnectionFactory());
			connection.setId(generateId() + ".exchangeDelete");
			channel = connection.createChannel();

			for (String exchange : exchanges) {
				channel.exchangeDelete(exchange);
			}
		}
		catch (Exception e) {
			LOGGER.warn("Failed to delete exchanges", e);
		}
		finally {
			closeResources(connection, channel);
		}
	}

	/**
	 * Delete and re-declare all the configured queues. Can be used between tests when
	 * a test might leave stale data and multiple tests use the same queue.
	 */
	public void purgeTestQueues() {
		removeTestQueues();
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;
		try {
			connection = getConnection(getConnectionFactory());
			channel = createQueues(connection);
		}
		catch (Exception e) {
			LOGGER.warn("Failed to re-declare queues during purge: " + e.getMessage());
		}
		finally {
			closeResources(connection, channel);
		}
	}

	/**
	 * Delete arbitrary queues from the broker.
	 * @param queuesToDelete the queues to delete.
	 */
	public void deleteQueues(String... queuesToDelete) {
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = getConnection(getConnectionFactory());
			connection.setId(generateId() + ".queueDelete");
			channel = connection.createChannel();

			for (String queue : queuesToDelete) {
				channel.queueDelete(queue);
			}
		}
		catch (Exception e) {
			LOGGER.warn("Failed to delete queues", e);
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
		Connection connection = null; // NOSONAR (closeResources())
		Channel channel = null;

		try {
			connection = getConnection(getConnectionFactory());
			connection.setId(generateId() + ".exchangeDelete");
			channel = connection.createChannel();

			for (String exchange : exchanges) {
				channel.exchangeDelete(exchange);
			}
		}
		catch (Exception e) {
			LOGGER.warn("Failed to delete queues", e);
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
			catch (@SuppressWarnings("unused") IOException | TimeoutException e) {
				// Ignore
			}
		}
		if (connection != null) {
			try {
				connection.close();
			}
			catch (@SuppressWarnings("unused") IOException e) {
				// Ignore
			}
		}
	}

	/**
	 * The {@link RuntimeException} thrown when broker is not available
	 * on the provided host port.
	 */
	public static class BrokerNotAliveException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		BrokerNotAliveException(String message) {
			super(message);
		}

		BrokerNotAliveException(String message, Throwable throwable) {
			super(message, throwable);
		}

	}

}
