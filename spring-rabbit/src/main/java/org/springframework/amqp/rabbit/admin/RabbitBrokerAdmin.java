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

import java.io.File;
import java.io.FilenameFilter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.erlang.OtpAuthException;
import org.springframework.erlang.OtpException;
import org.springframework.erlang.connection.ConnectionFactory;
import org.springframework.erlang.connection.SimpleConnectionFactory;
import org.springframework.erlang.core.Application;
import org.springframework.erlang.core.ErlangTemplate;
import org.springframework.erlang.core.Node;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.exec.Execute;
import org.springframework.util.exec.Os;

/**
 * Rabbit broker administration implementation exposed via JMX annotations.
 * 
 * @author Mark Pollack
 */
public class RabbitBrokerAdmin implements RabbitBrokerOperations {

	private static final String DEFAULT_VHOST = "/";

	private static String DEFAULT_HOST;

	private static final String DEFAULT_ENCODING = "UTF-8";

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private ErlangTemplate erlangTemplate;

	private String encoding = DEFAULT_ENCODING;

	private long timeout = 0;

	// TODO: extract into field for DI
	private SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();

	private final String hostName;

	// TODO: RABBITMQ_NODE_PORT=5672

	static {
		try {
			DEFAULT_HOST = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			DEFAULT_HOST = "localhost";
		}
	}

	public RabbitBrokerAdmin() {
		this(DEFAULT_HOST);
	}

	public RabbitBrokerAdmin(String hostName) {
		if (Os.isFamily("windows") && !DEFAULT_HOST.equals(hostName)) {
			hostName = hostName.toUpperCase();
		}
		this.hostName = hostName;
		this.executor.setDaemon(true);
		initializeDefaultErlangTemplate(hostName);
	}

	/**
	 * The encoding to use for converting host names to byte arrays (which is needed on the remote side).
	 * @param encoding the encoding to use (default UTF-8)
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Timeout (milliseconds) to wait for the broker to come up. If the provided timeout is greater than zero then we
	 * wait for that period for the broker to be ready. If it is not ready after that time the process is stopped.
	 * Defaults to 0 (no wait).
	 * 
	 * @param timeout the timeout value to set in milliseconds
	 */
	public void setStartupTimeout(long timeout) {
		this.timeout = timeout;
	}

	@SuppressWarnings("unchecked")
	public List<QueueInfo> getQueues() {
		return (List<QueueInfo>) erlangTemplate.executeAndConvertRpc("rabbit_amqqueue", "info_all",
				getBytes(DEFAULT_VHOST));
	}

	@SuppressWarnings("unchecked")
	public List<QueueInfo> getQueues(String virtualHost) {
		return (List<QueueInfo>) erlangTemplate.executeAndConvertRpc("rabbit_amqqueue", "info_all",
				getBytes(virtualHost));
	}

	// User management

	@ManagedOperation()
	public void addUser(String username, String password) {
		erlangTemplate
				.executeAndConvertRpc("rabbit_access_control", "add_user", getBytes(username), getBytes(password));
	}

	@ManagedOperation
	public void deleteUser(String username) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "delete_user", getBytes(username));
	}

	@ManagedOperation
	public void changeUserPassword(String username, String newPassword) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "change_password", getBytes(username),
				getBytes(newPassword));
	}

	@SuppressWarnings("unchecked")
	@ManagedOperation
	public List<String> listUsers() {
		return (List<String>) erlangTemplate.executeAndConvertRpc("rabbit_access_control", "list_users");
	}

	public int addVhost(String vhostPath) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int deleteVhost(String vhostPath) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setPermissions(String username, Pattern configure, Pattern read, Pattern write) {
		// TODO Auto-generated method stub
	}

	public void setPermissions(String username, Pattern configure, Pattern read, Pattern write, String vhostPath) {
		// TODO Auto-generated method stub
	}

	public void clearPermissions(String username) {
		// TODO Auto-generated method stub
	}

	public void clearPermissions(String username, String vhostPath) {
		// TODO Auto-generated method stub
	}

	public List<String> listPermissions() {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> listPermissions(String vhostPath) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String> listUserPermissions(String username) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation
	public void startBrokerApplication() {
		RabbitStatus status = getStatus();
		if (status.isReady()) {
			logger.info("Rabbit Application already running.");
			return;
		}
		if (!status.isAlive()) {
			logger.info("Rabbit Process not running.");
			startNode();
			return;
		}
		logger.info("Starting Rabbit Application.");

		// This call in particular seems to be prone to hanging, so do it in the background...
		final CountDownLatch latch = new CountDownLatch(1);
		Future<Object> result = executor.submit(new Callable<Object>() {
			public Object call() throws Exception {
				try {
					return erlangTemplate.executeAndConvertRpc("rabbit", "start");
				} finally {
					latch.countDown();
				}
			}
		});
		boolean started = false;
		try {
			started = latch.await(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			result.cancel(true);
			return;
		}
		if (timeout > 0 && started) {
			if (!waitForReadyState() && !result.isDone()) {
				result.cancel(true);
			}
		}
	}

	@ManagedOperation
	public void stopBrokerApplication() {
		logger.info("Stopping Rabbit Application.");
		erlangTemplate.executeAndConvertRpc("rabbit", "stop");
		if (timeout > 0) {
			waitForUnreadyState();
		}
	}

	@ManagedOperation
	public void startNode() {

		RabbitStatus status = getStatus();
		if (status.isAlive()) {
			logger.info("Rabbit Process already running.");
			startBrokerApplication();
			return;
		}

		if (!status.isRunning() && status.isReady()) {
			logger.info("Rabbit Process not running but status is ready.  Restarting.");
			stopNode();
		}

		logger.info("Starting RabbitMQ node by shelling out command line.");
		final Execute execute = new Execute();

		String rabbitStartScript = null;
		String hint = "";
		if (Os.isFamily("windows") || Os.isFamily("dos")) {
			rabbitStartScript = "sbin/rabbitmq-server.bat";
		} else if (Os.isFamily("unix") || Os.isFamily("mac")) {
			rabbitStartScript = "bin/rabbitmq-server";
			hint = "Depending on your platform it might help to set RABBITMQ_LOG_BASE and RABBITMQ_MNESIA_BASE System properties to an empty directory.";
		}
		Assert.notNull(rabbitStartScript, "unsupported OS family");

		String rabbitHome = System.getProperty("RABBITMQ_HOME", System.getenv("RABBITMQ_HOME"));
		if (rabbitHome == null) {
			if (Os.isFamily("windows") || Os.isFamily("dos")) {
				rabbitHome = findDirectoryName("c:/Program Files", "rabbitmq");
			} else if (Os.isFamily("unix") || Os.isFamily("mac")) {
				rabbitHome = "/usr/lib/rabbitmq";
			}
		}
		Assert.notNull(rabbitHome, "RABBITMQ_HOME system property (or environment variable) not set.");

		rabbitHome = StringUtils.cleanPath(rabbitHome);
		String rabbitStartCommand = rabbitHome + "/" + rabbitStartScript;
		String[] commandline = new String[] { rabbitStartCommand };

		List<String> env = new ArrayList<String>();
		addEnvironment(env, "RABBITMQ_LOG_BASE");
		addEnvironment(env, "RABBITMQ_MNESIA_BASE");
		addEnvironment(env, "ERLANG_HOME");

		// Make the hostname explicitly the same so the erl process knows who we are
		env.add("HOSTNAME=" + hostName);

		// Ask for a detached erl process so stdout doesn't get diverted to a black hole when the JVM dies (without this
		// you can start the Rabbit broker form Java but if you forget to stop it, the erl process is hosed).
		env.add("RABBITMQ_SERVER_ERL_ARGS=-detached");

		execute.setCommandline(commandline);
		execute.setEnvironment(env.toArray(new String[0]));

		final CountDownLatch running = new CountDownLatch(1);
		final AtomicBoolean finished = new AtomicBoolean(false);
		final String errorHint = hint;

		executor.execute(new Runnable() {
			public void run() {
				try {
					running.countDown();
					int exit = execute.execute();
					finished.set(true);
					logger.info("Finished broker launcher process with exit code=" + exit);
					if (exit != 0) {
						throw new IllegalStateException("Could not start process." + errorHint);
					}
				} catch (Exception e) {
					logger.error("Failed to start node", e);
				}
			}
		});

		try {
			logger.info("Waiting for Rabbit process to be started");
			Assert.state(running.await(1000L, TimeUnit.MILLISECONDS),
					"Timed out waiting for thread to start Rabbit process.");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		if (finished.get()) {
			// throw new
			// IllegalStateException("Expected broker process to start in background, but it has exited early.");
		}

		if (timeout > 0) {
			waitForReadyState();
		}

	}

	private boolean waitForReadyState() {
		return waitForState(new StatusCallback() {
			public boolean get(RabbitStatus status) {
				return status.isReady();
			}
		}, "ready");
	}

	private boolean waitForUnreadyState() {
		return waitForState(new StatusCallback() {
			public boolean get(RabbitStatus status) {
				return !status.isRunning();
			}
		}, "unready");
	}

	private boolean waitForStoppedState() {
		return waitForState(new StatusCallback() {
			public boolean get(RabbitStatus status) {
				return !status.isReady() && !status.isRunning();
			}
		}, "stopped");
	}

	private boolean waitForState(final StatusCallback callable, String state) {

		if (timeout <= 0) {
			return true;
		}

		RabbitStatus status = getStatus();

		if (!callable.get(status)) {

			logger.info("Waiting for broker to enter state: " + state);

			Future<RabbitStatus> started = executor.submit(new Callable<RabbitStatus>() {
				public RabbitStatus call() throws Exception {
					RabbitStatus status = getStatus();
					while (!callable.get(status)) {
						// Any less than 1000L and we tend to clog up the socket?
						Thread.sleep(500L);
						status = getStatus();
					}
					return status;
				}
			});

			try {
				status = started.get(timeout, TimeUnit.MILLISECONDS);
				// This seems to help... really it just means we didn't get the right status data
				Thread.sleep(500L);
			} catch (TimeoutException e) {
				started.cancel(true);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				logger.error("Exception checking broker status for " + state, e.getCause());
			}

			if (!callable.get(status)) {
				logger.error("Rabbit broker not in " + state + " state after timeout. Stopping process.");
				stopNode();
				return false;
			} else {
				logger.info("Finished waiting for broker to enter state: " + state);
				if (logger.isDebugEnabled()) {
					logger.info("Status: " + status);
				}
				return true;
			}

		} else {
			logger.info("Broker already in state: " + state);
		}

		return true;

	}

	/**
	 * Find a directory whose name starts with a substring in a given parent directory. If there is none return null,
	 * otherwise sort the results and return the best match (an exact match if there is one or the last one in a lexical
	 * sort).
	 * 
	 * @param parent
	 * @param child
	 * @return the full name of a directory
	 */
	private String findDirectoryName(String parent, String child) {
		String result = null;
		String[] names = new File(parent).list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.equals("rabbitmq") && new File(dir, name).isDirectory();
			}
		});
		if (names.length == 1) {
			result = new File(parent, names[0]).getAbsolutePath();
			return result;
		}
		List<String> sorted = Arrays.asList(new File(parent).list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith("rabbitmq") && new File(dir, name).isDirectory();
			}
		}));
		Collections.sort(sorted, Collections.reverseOrder());
		if (!sorted.isEmpty()) {
			result = new File(parent, sorted.get(0)).getAbsolutePath();
		}
		return result;
	}

	private void addEnvironment(List<String> env, String key) {
		String value = System.getProperty(key);
		if (value != null) {
			logger.debug("Adding environment variable: " + key + "=" + value);
			env.add(key + "=" + value);
		}
	}

	@ManagedOperation
	public void stopNode() {
		logger.info("Stopping RabbitMQ node.");
		try {
			erlangTemplate.executeAndConvertRpc("rabbit", "stop_and_halt");
		} catch (Exception e) {
			logger.error("Failed to send stop signal", e);
		}
		if (timeout >= 0) {
			waitForStoppedState();
		}
	}

	@ManagedOperation
	public void resetNode() {
		erlangTemplate.executeAndConvertRpc("rabbit_mnesia", "reset");
	}

	@ManagedOperation
	public void forceResetNode() {
		erlangTemplate.executeAndConvertRpc("rabbit_mnesia", "force_reset");

	}

	@ManagedOperation
	public RabbitStatus getStatus() {
		try {
			return (RabbitStatus) getErlangTemplate().executeAndConvertRpc("rabbit", "status");
		} catch (OtpAuthException e) {
			throw new RabbitAdminAuthException(
					"Could not authorise connection to Erlang process. This can happen if the broker is running, "
							+ "but as root or rabbitmq and the current user is not authorised to connect. Try starting the "
							+ "broker again as a different user.", e);
		} catch (OtpException e) {
			logger.debug("Ignoring OtpException (assuming that the broker is simply not running)");
			if (logger.isTraceEnabled()) {
				logger.trace("Status not available owing to exception", e);
			}
			return new RabbitStatus(Collections.<Application> emptyList(), Collections.<Node> emptyList(),
					Collections.<Node> emptyList());
		}
	}

	public void recoverAsync(boolean requeue) {
		// TODO Auto-generated method stub
	}

	public ErlangTemplate getErlangTemplate() {
		return this.erlangTemplate;
	}

	protected void initializeDefaultErlangTemplate(String host) {
		String peerNodeName = "rabbit@" + host;
		logger.debug("Creating jinterface connection with peerNodeName = [" + peerNodeName + "]");
		SimpleConnectionFactory otpConnectionFactory = new SimpleConnectionFactory("rabbit-spring-monitor",
				peerNodeName);
		otpConnectionFactory.afterPropertiesSet();
		createErlangTemplate(otpConnectionFactory);
	}

	protected void createErlangTemplate(ConnectionFactory otpConnectionFactory) {
		erlangTemplate = new ErlangTemplate(otpConnectionFactory);
		erlangTemplate.setErlangConverter(new RabbitControlErlangConverter());
		erlangTemplate.afterPropertiesSet();
	}

	/**
	 * Safely convert a string to its bytes using the encoding provided.
	 * 
	 * @see #setEncoding(String)
	 * 
	 * @param string the value to convert
	 * 
	 * @return the bytes from the string using the encoding provided
	 * 
	 * @throws IllegalStateException if the encoding is ont supported
	 */
	private byte[] getBytes(String string) {
		try {
			return string.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException("Unsupported encoding: " + encoding);
		}
	}

	private static interface StatusCallback {
		boolean get(RabbitStatus status);
	}
}
