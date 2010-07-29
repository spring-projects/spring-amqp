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

package org.springframework.amqp.rabbit.connection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedList;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * NOTE: this ConnectionFactory implementation is considered <b>experimental</b> at this stage.
 * There are concerns to be addressed in relation to the statefulness of channels. Therefore, we
 * recommend using {@link SingleConnectionFactory} for now.
 * 
 * A {@link ConnectionFactory} implementation that returns the same Connections from all
 * {@link #createConnection()} calls, and ignores calls to {@link com.rabbitmq.client.Connection#close()} and
 * caches {@link com.rabbitmq.client.Channel}.
 * 
 * <p>By default, only one single Session will be cached, with further requested Channels being created and
 * disposed on demand.  Consider raising the {@link #setChannelCacheSize(int) "channelCacheSize" value} in case of a 
 * high-concurrency environment.
 * 
 * <p><b>NOTE: This ConnectionFactory requires explicit closing of all Channels obtained form its
 * shared Connection.</b>  This is the usual recommendation for native Rabbit access code anyway.
 * However, with this ConnectionFactory, its use is mandatory in order to actually allow for Channel reuse.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
//TODO are there heartbeats and/or exception thrown if a connection is broken?
public class CachingConnectionFactory extends SingleConnectionFactory implements DisposableBean {

	private int channelCacheSize = 1;

	private final LinkedList<Channel> cachedChannels = new LinkedList<Channel>();
	
	private volatile boolean active = true;


	/**
	 * Create a new CachingConnectionFactory initializing the hostname to be the 
	 * value returned from InetAddress.getLocalHost(), or "localhost" if getLocalHost() throws
	 * an exception.
	 */
	public CachingConnectionFactory() {
		super();
	}

	/**
	 * Create a new CachingConnectionFactory given a host name.
	 * @param hostName the host name to connect to
	 */
	public CachingConnectionFactory(String hostName) {
		super(hostName);
	}

	/**
	 * Create a new CachingConnectionFactory for the given target
	 * ConnectionFactory.
	 * @param rabbitConnectionFactory the target ConnectionFactory
	 */
	public CachingConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
		super(rabbitConnectionFactory);
	} 


	public void setChannelCacheSize(int sessionCacheSize) {
		Assert.isTrue(sessionCacheSize >= 1, "Channel cache size must be 1 or higher");
		this.channelCacheSize = sessionCacheSize;
	}

	public int getChannelCacheSize() {
		return this.channelCacheSize;
	}

	protected Channel getChannel(Connection connection) throws Exception {
		LinkedList<Channel> channelList = this.cachedChannels;
		Channel channel = null;
		synchronized (channelList) {
			if (!channelList.isEmpty()) {
				channel = channelList.removeFirst();
			}
		}
		if (channel != null) {
			if (logger.isTraceEnabled()) {
				logger.trace("Found cached Rabbit Channel");
			}
		}
		else {
			Channel targetChannel = super.getChannel(connection);
			if (logger.isDebugEnabled()) {
				logger.debug("Creating cached Rabbit Channel");
			}
			channel = getCachedChannelProxy(targetChannel, channelList);
		}
		return channel;
	}

	protected Channel getCachedChannelProxy(Channel target, LinkedList<Channel> channelList) {
		return (Channel) Proxy.newProxyInstance(
				ChannelProxy.class.getClassLoader(),
				new Class[] {ChannelProxy.class},
				new CachedChannelInvocationHandler(target, channelList));
	}

	/**
	 * Reset the Channel cache and underlying shared Connection, to be reinitialized on next access.
	 */
	public void resetConnection() {
		this.active = false;
		synchronized (this.cachedChannels) {
			for (Channel channel : cachedChannels) {
				try {
					channel.close();
				}
				catch (Throwable ex) {
					logger.trace("Could not close cached Rabbit Channel", ex);
				}
			}						
			this.cachedChannels.clear();
		}
		this.active = true;
		super.resetConnection();
	}

	@Override
	public String toString() {
		return "CachingConnectionFactory [channelCacheSize=" + channelCacheSize
				+ ", host=" + this.getHost() + ", port=" + this.getPort()
				+ ", active=" + active + "]";
	}


	public class CachedChannelInvocationHandler implements InvocationHandler {

		private final Channel target;

		private final LinkedList<Channel> channelList;

		
		public CachedChannelInvocationHandler(Channel target, LinkedList<Channel> channelList) {
			this.target = target;
			this.channelList = channelList;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String methodName = method.getName();
			if (methodName.equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			}
			else if (methodName.equals("hashCode")) {
				// Use hashCode of Channel proxy.
				return System.identityHashCode(proxy);
			}
			else if (methodName.equals("toString")) {
				return "Cached Rabbit Channel: " + this.target;
			}
			else if (methodName.equals("close")) {
				// Handle close method: don't pass the call on.
				if (active) {
					synchronized (this.channelList) {
						if (this.channelList.size() < getChannelCacheSize()) {
							logicalClose((Channel) proxy);
							// Remain open in the channel list.
							return null;
						}
					}
				}
				
				// If we get here, we're supposed to shut down.
				physicalClose();
				return null;
			}
			else if (methodName.equals("getTargetChannel")) {
				// Handle getTargetSession method: return underlying Channel.
				return this.target;
			}			
			try {
				return method.invoke(this.target, args);
			}
			catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}

		private void logicalClose(Channel proxy) throws Exception {
			// Allow for multiple close calls...
			if (!this.channelList.contains(proxy)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Returning cached Channel: " + this.target);
				}
				this.channelList.addLast(proxy);
			}
		}

		private void physicalClose() throws Exception {
			if (logger.isDebugEnabled()) {
				logger.debug("Closing cached Channel: " + this.target);
			}
			this.target.close();			
		}

	}

}
