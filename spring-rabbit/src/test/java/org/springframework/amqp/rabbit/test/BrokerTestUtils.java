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
package org.springframework.amqp.rabbit.test;

import org.springframework.amqp.rabbit.admin.RabbitBrokerAdmin;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

/**
 * Global convenience class for all integration tests, carrying constants and other utilities for broker set up.
 * 
 * @author Dave Syer
 * 
 */
public class BrokerTestUtils {

	public static final int DEFAULT_PORT = 5672;

	public static final int TRACER_PORT = 5673;
	
	public static final String ADMIN_NODE_NAME = "spring@localhost";

	
	/**
	 * The port that the broker is listening on (e.g. as input for a {@link ConnectionFactory}).
	 * 
	 * @return a port number
	 */
	public static int getPort() {
		return DEFAULT_PORT;
	}

	/**
	 * The port that the tracer is listening on (e.g. as input for a {@link ConnectionFactory}).
	 * 
	 * @return a port number
	 */
	public static int getTracerPort() {
		return TRACER_PORT;
	}

	/**
	 * An alternative port number than can safely be used to stop and start a broker, even when one is already running
	 * on the standard port as a privileged user. Useful for tests involving {@link RabbitBrokerAdmin} on UN*X.
	 * 
	 * @return a port number
	 */
	public static int getAdminPort() {
		return 15672;
	}

	/**
	 * Convenience factory for a {@link RabbitBrokerAdmin} instance that will usually start and stop cleanly on all
	 * systems.
	 * 
	 * @return a {@link RabbitBrokerAdmin} instance
	 */
	public static RabbitBrokerAdmin getRabbitBrokerAdmin() {
		return getRabbitBrokerAdmin(ADMIN_NODE_NAME, getAdminPort());
	}

	/**
	 * Convenience factory for a {@link RabbitBrokerAdmin} instance that will usually start and stop cleanly on all
	 * systems.
	 * 
	 * @param nodeName the name of the node
	 * 
	 * @return a {@link RabbitBrokerAdmin} instance
	 */
	public static RabbitBrokerAdmin getRabbitBrokerAdmin(String nodeName) {
		return getRabbitBrokerAdmin(nodeName, getAdminPort());
	}

	/**
	 * Convenience factory for a {@link RabbitBrokerAdmin} instance that will usually start and stop cleanly on all
	 * systems.
	 * 
	 * @param nodeName the name of the node
	 * @param port the port to listen on
	 * 
	 * @return a {@link RabbitBrokerAdmin} instance
	 */
	public static RabbitBrokerAdmin getRabbitBrokerAdmin(String nodeName, int port) {
		RabbitBrokerAdmin brokerAdmin = new RabbitBrokerAdmin(nodeName, port);
		brokerAdmin.setRabbitLogBaseDirectory("target/rabbitmq/log");
		brokerAdmin.setRabbitMnesiaBaseDirectory("target/rabbitmq/mnesia");
		brokerAdmin.setStartupTimeout(10000L);
		return brokerAdmin;
	}

}
