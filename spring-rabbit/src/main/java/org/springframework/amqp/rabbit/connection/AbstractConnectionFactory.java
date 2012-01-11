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
package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import com.rabbitmq.client.Address;

/**
 * @author Dave Syer
 * @author Gary Russell
 * 
 */
public abstract class AbstractConnectionFactory implements ConnectionFactory, DisposableBean {

	protected final Log logger = LogFactory.getLog(getClass());

	private final com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;

	private final CompositeConnectionListener connectionListener = new CompositeConnectionListener();

	private final CompositeChannelListener channelListener = new CompositeChannelListener();

	private volatile Address[] addresses;

	/**
	 * Create a new SingleConnectionFactory for the given target ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public AbstractConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		Assert.notNull(rabbitConnectionFactory, "Target ConnectionFactory must not be null");
		this.rabbitConnectionFactory = rabbitConnectionFactory;
	}

	public void setUsername(String username) {
		this.rabbitConnectionFactory.setUsername(username);
	}

	public void setPassword(String password) {
		this.rabbitConnectionFactory.setPassword(password);
	}

	public void setHost(String host) {
		this.rabbitConnectionFactory.setHost(host);
	}

	public String getHost() {
		return this.rabbitConnectionFactory.getHost();
	}

	public void setVirtualHost(String virtualHost) {
		this.rabbitConnectionFactory.setVirtualHost(virtualHost);
	}

	public String getVirtualHost() {
		return rabbitConnectionFactory.getVirtualHost();
	}

	public void setPort(int port) {
		this.rabbitConnectionFactory.setPort(port);
	}

	public int getPort() {
		return this.rabbitConnectionFactory.getPort();
	}

	/**
	 * Set addresses for clustering.
	 * @param addresses list of addresses with form "host[:port],..."
	 */
	public void setAddresses(String addresses) {
		Address[] addressArray = Address.parseAddresses(addresses);
		if (addressArray.length > 0) {
			this.addresses = addressArray;
		}
	}

	/**
	 * A composite connection listener to be used by subclasses when creating and closing connections.
	 * 
	 * @return the connection listener
	 */
	protected ConnectionListener getConnectionListener() {
		return connectionListener;
	}
	
	/**
	 * A composite channel listener to be used by subclasses when creating and closing channels.
	 * 
	 * @return the channel listener
	 */
	protected ChannelListener getChannelListener() {
		return channelListener;
	}

	public void setConnectionListeners(List<? extends ConnectionListener> listeners) {
		this.connectionListener.setDelegates(listeners);
	}

	public void addConnectionListener(ConnectionListener listener) {
		this.connectionListener.addDelegate(listener);
	}

	public void setChannelListeners(List<? extends ChannelListener> listeners) {
		this.channelListener.setDelegates(listeners);
	}

	public void addChannelListener(ChannelListener listener) {
		this.channelListener.addDelegate(listener);
	}

	final protected Connection createBareConnection() {
		try {
			if (this.addresses != null) {
				return new SimpleConnection(this.rabbitConnectionFactory.newConnection(this.addresses));
			}
			return new SimpleConnection(this.rabbitConnectionFactory.newConnection());
		} catch (IOException e) {
			throw RabbitUtils.convertRabbitAccessException(e);
		}
	}

	final protected  String getDefaultHostName() {
		String temp;
		try {
			InetAddress localMachine = InetAddress.getLocalHost();
			temp = localMachine.getHostName();
			logger.debug("Using hostname [" + temp + "] for hostname.");
		} catch (UnknownHostException e) {
			logger.warn("Could not get host name, using 'localhost' as default value", e);
			temp = "localhost";
		}
		return temp;
	}
	
	public void destroy() {
	}
}