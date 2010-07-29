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

package org.springframework.amqp.rabbit.listener;


import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.support.RabbitAccessor;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;

import com.rabbitmq.client.Connection;

/**
 * @author Mark Pollack
 */
public abstract class AbstractRabbitListeningContainer extends RabbitAccessor implements BeanNameAware, DisposableBean, SmartLifecycle {

	//TODO See if can replace methods with general throws Exception signature to use a more specific exception.
	
	private volatile String beanName;
	
	private volatile Connection sharedConnection;
	
	private volatile boolean autoStartup = true;
	
	private int phase = Integer.MAX_VALUE;
	
	private volatile boolean active = false;
	
	private volatile boolean running = false;
	
	protected final Object lifecycleMonitor = new Object();


	/**
	 * Set whether to automatically start the container after initialization.
	 * <p>Default is "true"; set this to "false" to allow for manual startup
	 * through the {@link #start()} method.
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Specify the phase in which this container should be started and
	 * stopped. The startup order proceeds from lowest to highest, and
	 * the shutdown order is the reverse of that. By default this value
	 * is Integer.MAX_VALUE meaning that this container starts as late
	 * as possible and stops as soon as possible.
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Return the phase in which this container will be started and stopped.
	 */
	public int getPhase() {
		return this.phase;
	}

	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * Return the bean name that this listener container has been assigned
	 * in its containing bean factory, if any.
	 */
	protected final String getBeanName() {
		return this.beanName;
	}
	
	/**
	 * Delegates to {@link #validateConfiguration()} and {@link #initialize()}.
	 */
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		validateConfiguration();
		initialize();
	}
	
	/**
	 * Validate the configuration of this container.
	 * <p>The default implementation is empty. To be overridden in subclasses.
	 */
	protected void validateConfiguration() {
	}
	
	/**
	 * Calls {@link #shutdown()} when the BeanFactory destroys the container instance.
	 * @see #shutdown()
	 */
	public void destroy() {
		shutdown();
	}


	//-------------------------------------------------------------------------
	// Lifecycle methods for starting and stopping the container
	//-------------------------------------------------------------------------

	/**
	 * Initialize this container.
	 * <p>Creates a Rabbit Connection and calls {@link #doInitialize()}.
	 */
	public void initialize()  {
		try {
			synchronized (this.lifecycleMonitor) {
				this.active = true;
				this.lifecycleMonitor.notifyAll();
			}
			doInitialize();
		}
		catch (Exception ex) {
			ConnectionFactoryUtils.releaseConnection(this.sharedConnection, getConnectionFactory());
			this.sharedConnection = null;
			throw convertRabbitAccessException(ex);
		}
	}
	
	/**
	 * Stop the shared Connection, call {@link #doShutdown()},
	 * and close this container.
	 */
	public void shutdown() {
		logger.debug("Shutting down Rabbit listener container");
		boolean wasRunning = false;
		synchronized (this.lifecycleMonitor) {
			wasRunning = this.running;
			this.running = false;
			this.active = false;
			this.lifecycleMonitor.notifyAll();
		}

		// Stop shared Connection early, if necessary.
		if (wasRunning && sharedConnectionEnabled()) {
			try {
				stopSharedConnection();
			}
			catch (Throwable ex) {
				logger.debug("Could not stop Rabbit Connection on shutdown", ex);
			}
		}

		// Shut down the invokers.
		try {
			doShutdown();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
		finally {
			if (sharedConnectionEnabled()) {				
				ConnectionFactoryUtils.releaseConnection(this.sharedConnection, getConnectionFactory());
				this.sharedConnection = null;				
			}
		}
	}
	
	/**
	 * Return whether this container is currently active,
	 * that is, whether it has been set up but not shut down yet.
	 */
	public final boolean isActive() {
		synchronized (this.lifecycleMonitor) {
			return this.active;
		}
	}
	
	/**
	 * Start this container.
	 * @see #doStart
	 */
	public void start()  {
		try {
			doStart();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}
	
	/**
	 * Start the shared Connection, if any, and notify all invoker tasks.
	 * @throws Exception  if thrown by Rabbit API methods
	 * @see #establishSharedConnection
	 */
	protected void doStart() throws Exception {
		// Lazily establish a shared Connection, if necessary.
		if (sharedConnectionEnabled()) {
			establishSharedConnection();
		}

		// Reschedule paused tasks, if any.
		synchronized (this.lifecycleMonitor) {
			this.running = true;
			this.lifecycleMonitor.notifyAll();
		}

	}
	
	/**
	 * Stop this container.
	 * @see #doStop
	 */
	public void stop() {
		try {
			doStop();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}
	
	/**
	 * Notify all invoker tasks and stop the shared Connection, if any.
	 * @see #stopSharedConnection
	 */
	protected void doStop() {
		synchronized (this.lifecycleMonitor) {
			this.running = false;
			this.lifecycleMonitor.notifyAll();
		}
		if (sharedConnectionEnabled()) {
			stopSharedConnection();
		}
	}
	
	/**
	 * Determine whether this container is currently running,
	 * that is, whether it has been started and not stopped yet.
	 * @see #start()
	 * @see #stop()
	 * @see #runningAllowed()
	 */
	public final boolean isRunning() {
		synchronized (this.lifecycleMonitor) {
			return (this.running && runningAllowed());
		}
	}
	
	/**
	 * Check whether this container's listeners are generally allowed to run.
	 * <p>This implementation always returns <code>true</code>; the default 'running'
	 * state is purely determined by {@link #start()} / {@link #stop()}.
	 * <p>Subclasses may override this method to check against temporary
	 * conditions that prevent listeners from actually running. In other words,
	 * they may apply further restrictions to the 'running' state, returning
	 * <code>false</code> if such a restriction prevents listeners from running.
	 */
	protected boolean runningAllowed() {
		return true;
	}


	//-------------------------------------------------------------------------
	// Management of a shared Rabbit Connection
	//-------------------------------------------------------------------------
	
	/**
	 * Establish a shared Connection for this container.
	 * <p>The default implementation delegates to {@link #createSharedConnection()},
	 * which does one immediate attempt and throws an exception if it fails.
	 * Can be overridden to have a recovery process in place, retrying
	 * until a Connection can be successfully established.
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected void establishSharedConnection() throws Exception {
		if (this.sharedConnection == null) {
			this.sharedConnection = createSharedConnection();
			logger.debug("Established shared Rabbit Connection");
		}
	}

	/**
	 * Refresh the shared Connection that this container holds.
	 * <p>Called on startup and also after an infrastructure exception
	 * that occurred during invoker setup and/or execution.
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected final void refreshSharedConnection() throws Exception {
		ConnectionFactoryUtils.releaseConnection(
				this.sharedConnection, getConnectionFactory());
		this.sharedConnection = null;
		this.sharedConnection = createSharedConnection();					
	}

	/**
	 * Create a shared Connection for this container.
	 * <p>The default implementation creates a standard Connection
	 * and prepares it through {@link #prepareSharedConnection}.
	 * @return the prepared Connection
	 * @throws Exception if the creation failed
	 */
	protected Connection createSharedConnection() throws Exception {
		Connection con = createConnection();
		try {
			prepareSharedConnection(con);
			return con;
		}
		catch (Exception ex) {
			RabbitUtils.closeConnection(con);
			throw ex;
		}
	}
	
	/**
	 * Prepare the given Connection, which is about to be registered
	 * as shared Connection for this container.
	 * <p>The default implementation sets the specified client id, if any.
	 * Subclasses can override this to apply further settings.
	 * @param connection the Connection to prepare
	 */
	protected void prepareSharedConnection(Connection connection) {
	}

	/**
	 * Stop the shared Connection, logging any exception thrown by
	 * Rabbit API methods.
	 */
	protected void stopSharedConnection() {		
		if (this.sharedConnection != null) {
			try {
				this.sharedConnection.close();
			}
			catch (Exception ex) {
				logger.debug("Ignoring Connection close exception - assuming already closed: " + ex);
			}
		}		
	}

	/**
	 * Return the shared Rabbit Connection maintained by this container.
	 * Available after initialization.
	 * @return the shared Connection (never <code>null</code>)
	 * @throws IllegalStateException if this container does not maintain a
	 * shared Connection, or if the Connection hasn't been initialized yet
	 * @see #sharedConnectionEnabled()
	 */
	protected final Connection getSharedConnection() {
		if (!sharedConnectionEnabled()) {
			throw new IllegalStateException(
					"This listener container does not maintain a shared Connection");
		}
		if (this.sharedConnection == null) {
			throw new SharedConnectionNotInitializedException(
					"This listener container's shared Connection has not been initialized yet");
		}
		return this.sharedConnection;
	}


	//-------------------------------------------------------------------------
	// Template methods to be implemented by subclasses
	//-------------------------------------------------------------------------

	/**
	 * Return whether a shared Rabbit Connection should be maintained
	 * by this container base class.
	 * @see #getSharedConnection()
	 */
	protected abstract boolean sharedConnectionEnabled();

	/**
	 * Register any invokers within this container.
	 * <p>Subclasses need to implement this method for their specific
	 * invoker management process.
	 * <p>A shared Rabbit Connection
	 * @throws Exception 
	 * @see #getSharedConnection()
	 */
	protected abstract void doInitialize() throws Exception;

	/**
	 * Close the registered invokers.
	 * <p>Subclasses need to implement this method for their specific
	 * invoker management process.
	 * <p>A shared Rabbit Connection, if any, will automatically be closed
	 * <i>afterwards</i>.
	 * @see #shutdown()
	 */
	protected abstract void doShutdown();
	

	/**
	 * Exception that indicates that the initial setup of this container's
	 * shared Rabbit Connection failed. This is indicating to invokers that they need
	 * to establish the shared Connection themselves on first access.
	 */
	@SuppressWarnings("serial")
	public static class SharedConnectionNotInitializedException extends RuntimeException {

		/**
		 * Create a new SharedConnectionNotInitializedException.
		 * @param msg the detail message
		 */
		protected SharedConnectionNotInitializedException(String msg) {
			super(msg);
		}
	}

}
