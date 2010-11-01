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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.erlang.OtpAuthException;
import org.springframework.erlang.OtpIOException;
import org.springframework.erlang.connection.SingleConnectionFactory;
import org.springframework.erlang.core.Application;
import org.springframework.erlang.core.ErlangTemplate;
import org.springframework.erlang.core.Node;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.exec.Execute;
import org.springframework.util.exec.Os;

import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.Channel;

/**
 * Rabbit broker administration implementation exposed via JMX annotations.
 * 
 * @author Mark Pollack
 */
public class RabbitBrokerAdmin implements RabbitBrokerOperations {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private RabbitTemplate rabbitTemplate;

	private RabbitAdmin rabbitAdmin;

	private ErlangTemplate erlangTemplate;

	private String virtualHost;

	public RabbitBrokerAdmin(ConnectionFactory connectionFactory) {
		this.virtualHost = connectionFactory.getVirtualHost();
		this.rabbitTemplate = new RabbitTemplate(connectionFactory);
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
		initializeDefaultErlangTemplate(rabbitTemplate);
	}

	// Exchange Operations

	public void declareExchange(Exchange exchange) {
		rabbitAdmin.declareExchange(exchange);
	}

	/**
	 * Declare an exchange specifying its durability and auto-delete behavior. Explicit arguments are given so as to
	 * make this method easily accessible from JMX management consoles. Durable exchanges last until they are deleted,
	 * they will survive a server restart. Auto-deleted exchanges last until they are no longer used
	 * 
	 * @param exchangeName the name of the exchange
	 * @param exchangeType the exchange type
	 * @param durable true if we are declaring a durable exchange (the exchange will survive a server restart)
	 * @param autoDelete true if the server should delete the exchange when it is no longer in use
	 */
	@ManagedOperation
	public void declareExchange(final String exchangeName, final String exchangeType, final boolean durable,
			final boolean autoDelete) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.exchangeDeclare(exchangeName, exchangeType, durable, autoDelete, new HashMap<String, Object>());
				return null;
			}
		});
	}

	@ManagedOperation(description = "Delete a exchange, without regard for whether it is in use or has messages on it")
	@ManagedOperationParameters(@ManagedOperationParameter(name = "exchange", description = "the name of the exchange"))
	public void deleteExchange(String exchangeName) {
		rabbitAdmin.deleteExchange(exchangeName);

	}

	@ManagedOperation
	public DeleteOk deleteExchange(final String exchangeName, final boolean ifUnused) {
		return rabbitTemplate.execute(new ChannelCallback<DeleteOk>() {
			public DeleteOk doInRabbit(Channel channel) throws Exception {
				channel.exchangeDelete(exchangeName, ifUnused);
				return null;
			}
		});
	}

	// Queue Operations

	@ManagedOperation
	public Queue declareQueue() {
		return rabbitAdmin.declareQueue();
	}

	@ManagedOperation
	public void declareQueue(Queue queue) {
		rabbitAdmin.declareQueue(queue);
	}

	@ManagedOperation
	public void deleteQueue(String queueName) {
		rabbitAdmin.deleteQueue(queueName);
	}

	@ManagedOperation
	public void deleteQueue(String queueName, boolean unused, boolean empty) {
		rabbitAdmin.deleteQueue(queueName, unused, empty);

	}

	@ManagedOperation
	public void purgeQueue(String queueName, boolean noWait) {
		rabbitAdmin.purgeQueue(queueName, noWait);

	}

	@SuppressWarnings("unchecked")
	public List<QueueInfo> getQueues() {
		return (List<QueueInfo>) erlangTemplate.executeAndConvertRpc("rabbit_amqqueue", "info_all", virtualHost
				.getBytes());
	}

	// Binding operations
	public void declareBinding(Binding binding) {
		rabbitAdmin.declareBinding(binding);
	}

	public void removeBinding(final Binding binding) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueUnbind(binding.getQueue(), binding.getExchange(), binding.getRoutingKey(), binding
						.getArguments());
				return null;
			}
		});
	}

	// User management

	@ManagedOperation()
	public void addUser(String username, String password) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "add_user", username.getBytes(), password
				.getBytes());
	}

	@ManagedOperation
	public void deleteUser(String username) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "delete_user", username.getBytes());
	}

	@ManagedOperation
	public void changeUserPassword(String username, String newPassword) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "change_password", username.getBytes(),
				newPassword.getBytes());
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
		logger.debug("Starting Rabbit Application.");
		erlangTemplate.executeAndConvertRpc("rabbit", "start");
	}

	@ManagedOperation
	public void stopBrokerApplication() {
		logger.debug("Stopping Rabbit Application.");
		erlangTemplate.executeAndConvertRpc("rabbit", "stop");
	}

	@ManagedOperation
	public void startNode() {

		logger.debug("Starting RabbitMQ node by shelling out command line.");
		final Execute execute = new Execute();

		String rabbitStartScript = null;
		String hint = "";
		if (Os.isFamily("windows") || Os.isFamily("dos")) {
			rabbitStartScript = "rabbitmq-server.bat";
		} else if (Os.isFamily("unix") || Os.isFamily("mac")) {
			rabbitStartScript = "rabbitmq-server";
			hint = "Depending on your platform it might help to set RABBITMQ_LOG_BASE and RABBITMQ_MNESIA_BASE System properties to an empty directory.";
		}
		Assert.notNull(rabbitStartScript, "unsupported OS family");

		String rabbitHome = System.getProperty("RABBITMQ_HOME", System.getenv("RABBITMQ_HOME"));
		Assert.notNull(rabbitHome, "RABBITMQ_HOME system property (or environment variable) not set.");

		rabbitHome = StringUtils.cleanPath(rabbitHome);
		String rabbitStartCommand = rabbitHome + System.getProperty("file.separator") + "sbin"
				+ System.getProperty("file.separator") + rabbitStartScript;

		List<String> env = new ArrayList<String>();
		addEnvironment(env, "RABBITMQ_LOG_BASE");
		addEnvironment(env, "RABBITMQ_MNESIA_BASE");
		addEnvironment(env, "ERLANG_HOME");

		execute.setCommandline(new String[] { rabbitStartCommand });
		execute.setEnvironment(env.toArray(new String[0]));

		// TODO: extract into field for DI
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();

		final CountDownLatch running = new CountDownLatch(1);
		final AtomicBoolean finished = new AtomicBoolean(false);
		final String errorHint = hint;

		executor.execute(new Runnable() {
			public void run() {
				try {
					running.countDown();
					int exit = execute.execute();
					finished.set(true);
					if (exit != 0) {
						throw new IllegalStateException("Could not start process." + errorHint);
					}
				} catch (Exception e) {
					logger.error("failed to start node", e);
				}
			}
		});

		try {
			logger.info("Waiting for Rabbit process to be started");
			Assert.state(running.await(1000L, TimeUnit.MILLISECONDS), "Timed out waiting for Rabbit process to start.");
			Thread.sleep(100L);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		if (finished.get()) {
			throw new IllegalStateException("Expected broker process to start in background, but it has exited early.");
		}

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
		logger.debug("Stopping RabbitMQ node.");
		try {
			erlangTemplate.executeAndConvertRpc("rabbit", "stop_and_halt");
		} catch (Exception e) {
			logger.error("Failed to send stop signal", e);
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
		} catch (OtpIOException e) {
			logger.info("Ignoring OtpIOException (assuming that the broker is simply not running)");
			return new RabbitStatus(Collections.<Application> emptyList(), Collections.<Node> emptyList(), Collections
					.<Node> emptyList());
		} catch (OtpAuthException e) {
			throw new RabbitAdminAuthException(
					"Could not authorise connection to Erlang process. This can happen if the broker is running, "
							+ "but as root or rabbitmq and the current user is not authorised to connect. Try starting the "
							+ "broker again as a different user.", e);
		}
	}

	public void recoverAsync(boolean requeue) {
		// TODO Auto-generated method stub
	}

	public ErlangTemplate getErlangTemplate() {
		return this.erlangTemplate;
	}

	protected void initializeDefaultErlangTemplate(RabbitTemplate rabbitTemplate) {
		String host = rabbitTemplate.getConnectionFactory().getHost();
		if (Os.isFamily("windows")) {
			host = host.toUpperCase();
		}
		String peerNodeName = "rabbit@" + host;
		logger.debug("Creating jinterface connection with peerNodeName = [" + peerNodeName + "]");
		SingleConnectionFactory otpCf = new SingleConnectionFactory("rabbit-spring-monitor", peerNodeName);
		otpCf.afterPropertiesSet();
		createErlangTemplate(otpCf);
	}

	protected void createErlangTemplate(org.springframework.erlang.connection.ConnectionFactory otpCf) {
		erlangTemplate = new ErlangTemplate(otpCf);
		erlangTemplate.setErlangConverter(new RabbitControlErlangConverter());
		erlangTemplate.afterPropertiesSet();
	}

}
