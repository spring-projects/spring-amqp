/*
 * Copyright 2002-2025 the original author or authors.
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

import java.util.Map;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.springframework.amqp.rabbit.junit.BrokerRunningSupport.BrokerNotAliveException;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

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
 * set the environment variable {@value BrokerRunningSupport#BROKER_REQUIRED} to
 * {@code true} and the tests will fail fast.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.7
 *
 * @deprecated since 4.0 in favor of JUnit 5 {@link RabbitAvailable}.
 */
@Deprecated(since = "4.0", forRemoval = true)
public final class BrokerRunning extends TestWatcher {

	private static final Log LOGGER = LogFactory.getLog(BrokerRunningSupport.class);

	private final BrokerRunningSupport brokerRunning;

	/**
	 * Set environment variable overrides for host, port etc. Will override any real
	 * environment variables, if present.
	 * <p><b>The variables will only apply to rule instances that are created after this
	 * method is called.</b>
	 * The overrides will remain until {@link #clearEnvironmentVariableOverrides()} is
	 * called.
	 * @param environmentVariables the variables.
	 */
	public static void setEnvironmentVariableOverrides(Map<String, String> environmentVariables) {
		BrokerRunningSupport.setEnvironmentVariableOverrides(environmentVariables);
	}

	/**
	 * Clear any environment variable overrides set in {@link #setEnvironmentVariableOverrides(Map)}.
	 */
	public static void clearEnvironmentVariableOverrides() {
		BrokerRunningSupport.clearEnvironmentVariableOverrides();
	}

	/**
	 * Ensure the broker is running and has a empty queue(s) with the specified name(s) in the
	 * default exchange.
	 *
	 * @param names the queues to declare for the test.
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunningWithEmptyQueues(String... names) {
		return new BrokerRunning(true, names);
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
		return new BrokerRunning(false, true);
	}

	/**
	 * @param queues the queues.
	 * @return a new rule that assumes an existing broker with the management plugin with
	 * the provided queues declared (and emptied if needed)..
	 */
	public static BrokerRunning isBrokerAndManagementRunningWithEmptyQueues(String... queues) {
		return new BrokerRunning(true, true, queues);
	}

	private BrokerRunning(boolean purge, String... queues) {
		this(purge, false, queues);
	}

	private BrokerRunning(boolean purge, boolean management, String... queues) {
		this.brokerRunning = new BrokerRunningSupport(purge, management, queues);
	}

	private BrokerRunning(String... queues) {
		this(false, queues);
	}

	private BrokerRunning() {
		this(BrokerRunningSupport.DEFAULT_QUEUE_NAME);
	}

	private BrokerRunning(boolean purge, boolean management) {
		this(purge, management, BrokerRunningSupport.DEFAULT_QUEUE_NAME);
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.brokerRunning.setPort(port);
	}

	/**
	 * @param hostName the hostName to set
	 */
	public void setHostName(String hostName) {
		this.brokerRunning.setHostName(hostName);
	}

	/**
	 * Set the user for the amqp connection default "guest".
	 * @param user the user.
	 * @since 1.7.2
	 */
	public void setUser(String user) {
		this.brokerRunning.setUser(user);
	}

	/**
	 * Set the password for the amqp connection default "guest".
	 * @param password the password.
	 * @since 1.7.2
	 */
	public void setPassword(String password) {
		this.brokerRunning.setPassword(password);
	}

	/**
	 * Set the uri for the REST API.
	 * @param adminUri the uri.
	 * @since 1.7.2
	 */
	public void setAdminUri(String adminUri) {
		this.brokerRunning.setAdminUri(adminUri);
	}

	/**
	 * Set the user for the management REST API connection default "guest".
	 * @param user the user.
	 * @since 1.7.2
	 */
	public void setAdminUser(String user) {
		this.brokerRunning.setAdminUser(user);
	}

	/**
	 * Set the password for the management REST API connection default "guest".
	 * @param password the password.
	 * @since 1.7.2
	 */
	public void setAdminPassword(String password) {
		this.brokerRunning.setAdminPassword(password);
	}

	/**
	 * Return the port.
	 * @return the port.
	 * @since 1.7.2
	 */
	public int getPort() {
		return this.brokerRunning.getPort();
	}

	/**
	 * Return the port.
	 * @return the port.
	 * @since 1.7.2
	 */
	public String getHostName() {
		return this.brokerRunning.getHostName();
	}

	/**
	 * Return the user.
	 * @return the user.
	 * @since 1.7.2
	 */
	public String getUser() {
		return this.brokerRunning.getUser();
	}

	/**
	 * Return the password.
	 * @return the password.
	 * @since 1.7.2
	 */
	public String getPassword() {
		return this.brokerRunning.getPassword();
	}

	/**
	 * Return the admin user.
	 * @return the user.
	 * @since 1.7.2
	 */
	public String getAdminUser() {
		return this.brokerRunning.getAdminUser();
	}

	/**
	 * Return the admin password.
	 * @return the password.
	 * @since 1.7.2
	 */
	public String getAdminPassword() {
		return this.brokerRunning.getAdminPassword();
	}

	@Override
	public Statement apply(Statement base, Description description) {
		try {
			this.brokerRunning.test();
		}
		catch (BrokerNotAliveException e) {
			LOGGER.warn("Not executing tests because basic connectivity test failed: " + e.getMessage());
			if (fatal()) {
				fail("RabbitMQ Broker is required, but not available");
			}
			else {
				assumeNoException(e);
			}
		}
		return super.apply(base, description);
	}

	public void isUp() {
		this.brokerRunning.test();
	}

	public static boolean fatal() {
		return BrokerRunningSupport.fatal();
	}

	/**
	 * Generate the connection id for the connection used by the rule's
	 * connection factory.
	 * @return the id.
	 */
	public String generateId() {
		return this.brokerRunning.generateId();
	}

	/**
	 * Remove any test queues that were created by an
	 * {@link #isRunningWithEmptyQueues(String...)} method.
	 * @param additionalQueues additional queues to remove that might have been created by
	 * tests.
	 */
	public void removeTestQueues(String... additionalQueues) {
		this.brokerRunning.removeTestQueues(additionalQueues);
	}

	/**
	 * Delete and re-declare all the configured queues. Can be used between tests when
	 * a test might leave stale data and multiple tests use the same queue.
	 */
	public void purgeTestQueues() {
		this.brokerRunning.purgeTestQueues();
	}

	/**
	 * Delete arbitrary queues from the broker.
	 * @param queuesToDelete the queues to delete.
	 */
	public void deleteQueues(String... queuesToDelete) {
		this.brokerRunning.deleteQueues(queuesToDelete);
	}

	/**
	 * Delete arbitrary exchanges from the broker.
	 * @param exchanges the exchanges to delete.
	 */
	public void deleteExchanges(String... exchanges) {
		this.brokerRunning.deleteExchanges(exchanges);
	}

	/**
	 * Get the connection factory used by this rule.
	 * @return the connection factory.
	 */
	public ConnectionFactory getConnectionFactory() {
		return this.brokerRunning.getConnectionFactory();
	}

	/**
	 * Return the admin uri.
	 * @return the uri.
	 * @since 1.7.2
	 */
	public String getAdminUri() {
		return this.brokerRunning.getAdminUri();
	}

}
