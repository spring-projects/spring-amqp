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

package org.springframework.erlang.connection;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.erlang.OtpIOException;
import org.springframework.util.Assert;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * A {@link ConnectionFactory} implementation that returns the same Connections from all
 * {@link #createConnection()} calls, and ignores calls to {@link Connection#close()}.
 * 
 * Provides a more traditional API to creating a connection to a remote erlang
 * node than the JInterface API.
 * 
 * <p>
 * The following is taken from the JInterface javadocs that describe the valid
 * node names that can be used. These naming constraints apply to the string
 * values you pass into the node names in SimpleConnectionFactory's constructor.
 * <p>
 * About nodenames: Erlang nodenames consist of two components, an alivename and
 * a hostname separated by '@'. Additionally, there are two nodename formats:
 * short and long. Short names are of the form "alive@hostname", while long
 * names are of the form "alive@host.fully.qualified.domainname". Erlang has
 * special requirements regarding the use of the short and long formats, in
 * particular they cannot be mixed freely in a network of communicating nodes,
 * however Jinterface makes no distinction. See the Erlang documentation for
 * more information about nodenames.
 * </p>
 * 
 * <p>
 * The constructors for the AbstractNode classes will create names exactly as
 * you provide them as long as the name contains '@'. If the string you provide
 * contains no '@', it will be treated as an alivename and the name of the local
 * host will be appended, resulting in a shortname. Nodenames longer than 255
 * characters will be truncated without warning.
 * </p>
 * 
 * <p>
 * Upon initialization, this class attempts to read the file .erlang.cookie in
 * the user's home directory, and uses the trimmed first line of the file as the
 * default cookie by those constructors lacking a cookie argument. If for any
 * reason the file cannot be found or read, the default cookie will be set to
 * the empty string (""). The location of a user's home directory is determined
 * using the system property "user.home", which may not be automatically set on
 * all platforms.
 * </p>
 * 
 * @author Mark Pollack
 */
public class SingleConnectionFactory implements ConnectionFactory,
		InitializingBean, DisposableBean {

	protected final Log logger = LogFactory.getLog(getClass());
	
	private boolean uniqueSelfNodeName = true;

	private String selfNodeName;

	private String cookie;

	private String peerNodeName;

	private OtpSelf otpSelf;

	private OtpPeer otpPeer;

	/** Raw JInterface Connection */
	private Connection targetConnection;

	/** Proxy Connection */
	private Connection connection;

	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();

	public SingleConnectionFactory(String selfNodeName, String cookie,
			String peerNodeName) {
		this.selfNodeName = selfNodeName;
		this.cookie = cookie;
		this.peerNodeName = peerNodeName;
	}

	public SingleConnectionFactory(String selfNodeName, String peerNodeName) {
		this.selfNodeName = selfNodeName;
		this.peerNodeName = peerNodeName;
	}
	
	public boolean isUniqueSelfNodeName() {
		return uniqueSelfNodeName;
	}

	public void setUniqueSelfNodeName(boolean uniqueSelfNodeName) {
		this.uniqueSelfNodeName = uniqueSelfNodeName;
	}

	public Connection createConnection() throws UnknownHostException,
			OtpAuthException {
		synchronized (this.connectionMonitor) {
			if (this.connection == null) {
				try {
					initConnection();
				} catch (IOException e) {
					throw new OtpIOException("failed to connect from '"
							+ this.selfNodeName + "' to peer node '"
							+ this.peerNodeName + "'", e);
				}

			}
			return this.connection;
		}
	}

	public void initConnection() throws IOException, OtpAuthException {
		synchronized (this.connectionMonitor) {
			if (this.targetConnection != null) {
				closeConnection(this.targetConnection);
			}
			this.targetConnection = doCreateConnection();
			prepareConnection(this.targetConnection);
			if (logger.isInfoEnabled()) {
				logger.info("Established shared Rabbit Connection: "
						+ this.targetConnection);
			}
			this.connection = getSharedConnectionProxy(this.targetConnection);
		}
	}
	
	/**
	 * Close the underlying shared connection.
	 * The provider of this ConnectionFactory needs to care for proper shutdown.
	 * <p>As this bean implements DisposableBean, a bean factory will
	 * automatically invoke this on destruction of its cached singletons.
	 */
	public void destroy() {
		resetConnection();
	}
	
	/**
	 * Reset the underlying shared Connection, to be reinitialized on next access.
	 */
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if (this.targetConnection != null) {
				closeConnection(this.targetConnection);
			}
			this.targetConnection = null;
			this.connection = null;
		}
	}

	/**
	 * Close the given Connection.
	 * 
	 * @param connection
	 *            the Connection to close
	 */
	protected void closeConnection(Connection connection) {
		if (logger.isDebugEnabled()) {
			logger.debug("Closing shared Rabbit Connection: "
					+ this.targetConnection);
		}
		try {
			// TODO there are other close overloads close(int closeCode,
			// java.lang.String closeMessage, int timeout)
			connection.close();
		} catch (Throwable ex) {
			logger.debug("Could not close shared Rabbit Connection", ex);
		}
	}

	/**
	 * Create a JInterface Connection via this class's ConnectionFactory.
	 * 
	 * @return the new Otp Connection
	 * @throws OtpAuthException
	 */
	protected Connection doCreateConnection() throws IOException,
			OtpAuthException {
		return new DefaultConnection(otpSelf.connect(otpPeer));
	}

	protected void prepareConnection(Connection con) throws IOException {
	}

	/**
	 * Wrap the given OtpConnection with a proxy that delegates every method
	 * call to it but suppresses close calls. This is useful for allowing
	 * application code to handle a special framework Connection just like an
	 * ordinary Connection from a Rabbit ConnectionFactory.
	 * 
	 * @param target
	 *            the original Connection to wrap
	 * @return the wrapped Connection
	 */
	protected Connection getSharedConnectionProxy(Connection target) {
		List<Class<?>> classes = new ArrayList<Class<?>>(1);
		classes.add(Connection.class);
		return (Connection) Proxy.newProxyInstance(
				Connection.class.getClassLoader(),
				classes.toArray(new Class[classes.size()]),
				new SharedConnectionInvocationHandler(target));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
		Assert.isTrue(this.selfNodeName != null || this.peerNodeName != null,
				"'selfNodeName' or 'peerNodeName' is required");
		String selfNodeNameToUse = this.selfNodeName;
		if (isUniqueSelfNodeName()) {
			selfNodeNameToUse = this.selfNodeName + "-" + UUID.randomUUID().toString();
			logger.debug("Creating OtpSelf with node name = [" + selfNodeNameToUse + "]");
		}		
		try {
			if (this.cookie == null) {
				this.otpSelf = new OtpSelf(selfNodeNameToUse.trim());
			} else {
				this.otpSelf = new OtpSelf(selfNodeNameToUse.trim(), this.cookie);
			}
		} catch (IOException e) {
			throw new OtpIOException(e);
		}
		this.otpPeer = new OtpPeer(this.peerNodeName.trim());		
	}

	private class SharedConnectionInvocationHandler implements
			InvocationHandler {

		private final Connection target;

		public SharedConnectionInvocationHandler(Connection target) {
			this.target = target;
		}

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (method.getName().equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			} else if (method.getName().equals("hashCode")) {
				// Use hashCode of Connection proxy.
				return System.identityHashCode(proxy);
			} else if (method.getName().equals("toString")) {
				return "Shared Otp Connection: " + this.target;
			} else if (method.getName().equals("close")) {
				// Handle close method: don't pass the call on.
				return null;
			}
			try {
				return method.invoke(this.target, args);
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

}
