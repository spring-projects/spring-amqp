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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.otp.erlang.connection.SimpleConnectionFactory;
import org.springframework.otp.erlang.core.ErlangTemplate;
import org.springframework.util.Assert;
import org.springframework.util.exec.Execute;
import org.springframework.util.exec.Os;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.AMQP.Queue.UnbindOk;

/**
 * Performs broker administration.  Uses {@link Queue}, {@link org.springframework.amqp.core.AbstractExchange},
 * and {@link Binding} as conveniences for code based configuration in Spring.
 * 
 * Exposes administrative operations from Channel and rabbitmqctl through JMX.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
@ManagedResource
public class RabbitAdminTemplate implements RabbitAdminOperations {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());
	
	private RabbitTemplate rabbitTemplate;
	
	private ErlangTemplate erlangTemplate;
	
	public RabbitAdminTemplate(ConnectionFactory rabbitConnectionFactory) {
		this.rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory);
		initializeDefaultErlangTemplate(rabbitTemplate);
	}
	
	public RabbitAdminTemplate(ConnectionFactory rabbitConnectionFactory, 
							   org.springframework.otp.erlang.connection.ConnectionFactory otpConnectionFactory) 
	{
		this.rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory);
		createErlangTemplate(otpConnectionFactory);		
	}
	
	public RabbitAdminTemplate(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;		
		initializeDefaultErlangTemplate(rabbitTemplate);
	}

/*
	public RabbitAdminTemplate(RabbitTemplate rabbitTemplate, ErlangTemplate erlangTemplate) {
		this.rabbitTemplate = rabbitTemplate;
		this.erlangTemplate = erlangTemplate;
	}
	*/

	public AMQP.Queue.DeclareOk declareQueue() {
		return rabbitTemplate.execute(new ChannelCallback<AMQP.Queue.DeclareOk>() {
			public AMQP.Queue.DeclareOk doInRabbit(Channel channel) throws Exception {
				AMQP.Queue.DeclareOk result = channel.queueDeclare();
				System.out.println("created queue: " + result.getQueue());
				return result;
			}
		});
	}

	public AMQP.Queue.DeclareOk declareQueue(final Queue queue) {
		Assert.notNull(queue.getName(), "Queue must contain a name. " +
				"For a server-generated name, call the declareQueue() method that takes no argument.");
		return rabbitTemplate.execute(new ChannelCallback<AMQP.Queue.DeclareOk>() {
			public AMQP.Queue.DeclareOk doInRabbit(Channel channel) throws Exception {
				logger.debug("Declaring queue [" + queue.getName() + "]");
				return channel.queueDeclare(queue.getName(), queue.isPassive(), queue.isDurable(),
						queue.isExclusive(), queue.isAutoDelete(), queue.getArguments());
			}
		});
	}

	public AMQP.Exchange.DeclareOk declareExchange(final org.springframework.amqp.core.AbstractExchange exchange) {
		return rabbitTemplate.execute(new ChannelCallback<AMQP.Exchange.DeclareOk>() {
			public AMQP.Exchange.DeclareOk doInRabbit(Channel channel) throws Exception {
				logger.debug("Declaring exchange [" + exchange.getName() + "]");
				return channel.exchangeDeclare(exchange.getName(), exchange.getExchangeType().name(), 
											   exchange.isPassive(), 
											   exchange.isDurable(),
											   exchange.isAutoDelete(), 
											   exchange.getArguments());
			}
		});
		
	}

	public AMQP.Queue.BindOk declareBinding(final Binding binding) {
		return rabbitTemplate.execute(new ChannelCallback<AMQP.Queue.BindOk>() {

			public AMQP.Queue.BindOk doInRabbit(Channel channel) throws Exception {
				
				logger.debug("Binding queue to exchange with routing key");
				return channel.queueBind(binding.getQueue(), binding.getExchange(), binding.getRoutingKey(), binding.getArguments());
			}
		});
		
	}
	
	public ErlangTemplate getErlangTemplate() {
		return this.erlangTemplate;
	}
	
	protected void initializeDefaultErlangTemplate(RabbitTemplate rabbitTemplate) {	
		String peerNodeName = "rabbit@" + rabbitTemplate.getConnectionFactory().getHostName();
		logger.debug("Creating jinterface connection with peerNodeName = [" + peerNodeName);
		SimpleConnectionFactory otpCf = new SimpleConnectionFactory("rabbit-spring-monitor", peerNodeName);
		otpCf.afterPropertiesSet();
		createErlangTemplate(otpCf);
	}

	private void createErlangTemplate(org.springframework.otp.erlang.connection.ConnectionFactory otpCf) {
		erlangTemplate = new ErlangTemplate(otpCf);
		erlangTemplate.setErlangConverter(new RabbitControlErlangConverter());
		erlangTemplate.afterPropertiesSet();
	}


	@ManagedOperation(description="Delete a exchange, without regard for whether it is in use or has messages on it")
	@ManagedOperationParameters(@ManagedOperationParameter(name = "exchange", description = "the name of the exchange"))
	public DeleteOk deleteExchange(String exchange) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@ManagedOperation()
	public DeleteOk deleteExchange(String exchange, boolean ifUnused) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation()
	public com.rabbitmq.client.AMQP.Queue.DeleteOk deleteQueue(String queueName) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation()
	public com.rabbitmq.client.AMQP.Queue.DeleteOk deleteQueue(
			String queueName, boolean ifUnused, boolean ifEmpty) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation()
	public PurgeOk purgeQueue(String queueName) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation()
	public PurgeOk purgeQueue(String queueName, boolean nowait) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation()
	public void recoverAsync(boolean requeue) {
		// TODO Auto-generated method stub
		
	}

	@ManagedOperation()
	public UnbindOk unbindQueue(String queue, String exchange, String routingKey) {
		// TODO Auto-generated method stub
		return null;
	}
	
	//TODO will Hyperic allow invocation with a Map<String,Object>.  If not, remove from @ManagedOperation

	@ManagedOperation()
	public UnbindOk unbindQueue(String queue, String exchange,
			String routingKey, Map<String, Object> arguments) {
		// TODO Auto-generated method stub
		return null;
	}

	@ManagedOperation()
	public void addUser(String username, String password) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "add_user", username.getBytes(), password.getBytes());
	}
	
	
	@ManagedOperation()
	public void deleteUser(String username) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "delete_user", username.getBytes());		
	}
	
	@SuppressWarnings("unchecked")
	public List<String> listUsers() {
		return (List<String>) erlangTemplate.executeAndConvertRpc("rabbit_access_control", "list_users");			
	}

	public Map<String, String> getQueueInfo(String name) {
		// TODO: is there a more efficient way (direct RPC) to retrieve info for a single queue by name?
		Map<String, Map<String, String>> queues = this.getQueueInfo();
		return queues.get(name);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Map<String, String>> getQueueInfo() { // TODO: pass virtual host?... currently hardcoded to default ("/")
		return (Map<String, Map<String, String>>) erlangTemplate.executeAndConvertRpc("rabbit_amqqueue", "info_all", "/".getBytes());
	}

	@ManagedOperation()
	public void changeUserPassword(String username, String newPassword) {
		erlangTemplate.executeAndConvertRpc("rabbit_access_control", "change_password", username.getBytes(), newPassword.getBytes());		
	}

	@ManagedOperation()
	public void startBrokerApplication() {
		logger.debug("Starting Rabbit Application.");
		erlangTemplate.executeAndConvertRpc("rabbit", "start");
		
	}

	@ManagedOperation()
	public void stopBrokerApplication() {
		logger.debug("Stopping Rabbit Application.");
		erlangTemplate.executeAndConvertRpc("rabbit", "stop");		
	}
	
	@ManagedOperation()
	public void startNode() {
		logger.debug("Staring RabbitMQ node by shelling out command line.");
		final Execute execute = new Execute();

		String rabbitStartCommand = null;
		if (Os.isFamily("windows")) {
			String rabbitHome = System.getenv("RABBITMQ_HOME");
			// TODO remove any trailing directory separators on rabbit home var.
			if (rabbitHome == null) {
				throw new IllegalArgumentException(
						"RABBITMQ_HOME environment variable not set.");
			}
			rabbitStartCommand = rabbitHome
					+ System.getProperty("file.separator") + "sbin"
					+ System.getProperty("file.separator")
					+ "rabbitmq-server.bat";
		} else {
			// TODO abstract out install location and Win/Unix shell script name differencese
			throw new IllegalArgumentException("Only support for windows OS family at the moment...");
		}

		if (rabbitStartCommand != null) {
			execute.setCommandline(new String[] { rabbitStartCommand });
		} else {
			throw new IllegalArgumentException(
					"Could determine OS to create rabbit start command");
		}

		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.execute(new Runnable() {
			public void run() {
				try {
					execute.execute();
				} catch (Exception e) {
					logger.error("Could start node", e);
				}
			}
		});
	}
	
	
	@ManagedOperation()
	public void stopNode() {
		logger.debug("Stopping RabbitMQ node.");
		erlangTemplate.executeAndConvertRpc("rabbit", "stop_and_halt");
	}
	
	@ManagedOperation()
	public void resetNode() {
		erlangTemplate.executeAndConvertRpc("rabbit_mnesia", "reset");
	}
	
	@ManagedOperation()
	public void forceResetNode() {
		erlangTemplate.executeAndConvertRpc("rabbit_mnesia", "force_reset");
	}
	
	@ManagedOperation()
	public RabbitStatus getStatus() {
		return (RabbitStatus) getErlangTemplate().executeAndConvertRpc("rabbit", "status");
	}
	

	@ManagedOperation()
	public int addVhost(String vhostPath) {
		// TODO Auto-generated method stub
		return 0;
	}

	@ManagedOperation()
	public int deleteVhost(String vhostPath) {
		// TODO Auto-generated method stub
		return 0;
	}

	// Permissions

	public void clearPermissions(String username, String vhostPath) {
		// TODO Auto-generated method stub
		
	}


	public void clearPermissions(String username) {
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


	public void setPermissions(String username, Pattern configure,
			Pattern read, Pattern write, String vhostPath) {
		// TODO Auto-generated method stub
		
	}


	public void setPermissions(String username, Pattern configure,
			Pattern read, Pattern write) {
		// TODO Auto-generated method stub
		
	}

	

	
}
