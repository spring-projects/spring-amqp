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

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 */
public abstract class AbstractMessageListenerContainer extends RabbitAccessor implements BeanNameAware, DisposableBean,
		SmartLifecycle {

	private volatile String beanName;

	private volatile boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE;

	private volatile boolean active = false;

	private volatile boolean running = false;

	private final Object lifecycleMonitor = new Object();

	private volatile String[] queueNames;

	private ErrorHandler errorHandler;

	private boolean exposeListenerChannel = true;

	private volatile Object messageListener;

	private volatile AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	/**
	 * <p>
	 * Flag controlling the behaviour of the container with respect to message acknowledgement. The most common usage is
	 * to let the container handle the acknowledgements (so the listener doesn't need to know about the channel or the
	 * message).
	 * </p>
	 * <p>
	 * Set to {@link AcknowledgeMode#MANUAL} if the listener will send the acknowledgements itself using
	 * {@link Channel#basicAck(long, boolean)}. Manual acks are consistent with either a transactional or
	 * non-transactional channel, but if you are doing no other work on the channel at the same other than receiving a
	 * single message then the transaction is probably unnecessary.
	 * </p>
	 * <p>
	 * Set to {@link AcknowledgeMode#NONE} to tell the broker not to expect any acknowledgements, and it will assume all
	 * messages are acknowledged as soon as they are sent (this is "autoack" in native Rabbit broker terms). If
	 * {@link AcknowledgeMode#NONE} then the channel cannot be transactional (so the container will fail on start up if
	 * that flag is accidentally set).
	 * </p>
	 * 
	 * @param acknowledgeMode the acknowledge mode to set. Defaults to {@link AcknowledgeMode#AUTO}
	 * 
	 * @see AcknowledgeMode
	 */
	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	/**
	 * @return the acknowledgeMode
	 */
	public AcknowledgeMode getAcknowledgeMode() {
		return acknowledgeMode;
	}

	/**
	 * Set the name of the queue to receive messages from.
	 * @param queueName the desired queue (can not be <code>null</code>)
	 */
	public void setQueueNames(String... queueName) {
		this.queueNames = queueName;
	}

	public void setQueues(Queue... queues) {
		// TODO: Merge with string queue name values?
		String[] queueNames = new String[queues.length];
		for (int i = 0; i < queues.length; i++) {
			Assert.notNull(queues[i], "Queue must not be null.");
			queueNames[i] = queues[i].getName();
		}
		this.queueNames = queueNames;
	}

	/**
	 * Return the name of the queue to receive messages from.
	 */
	public String[] getQueueNames() {
		return this.queueNames;
	}

	protected String[] getRequiredQueueNames() {
		Assert.notNull(this.queueNames, "Queue names must not be null.");
		Assert.state(this.queueNames.length > 0, "Queue names must not be empty.");
		return this.queueNames;
	}

	/**
	 * Return whether to expose the listener {@link Channel} to a registered {@link ChannelAwareMessageListener}.
	 */
	public boolean isExposeListenerChannel() {
		return this.exposeListenerChannel;
	}

	/**
	 * Set whether to expose the listener Rabbit Channel to a registered {@link ChannelAwareMessageListener} as well as
	 * to {@link org.springframework.amqp.rabbit.core.RabbitTemplate} calls.
	 * <p>
	 * Default is "true", reusing the listener's {@link Channel}. Turn this off to expose a fresh Rabbit Channel fetched
	 * from the same underlying Rabbit {@link Connection} instead.
	 * <p>
	 * Note that Channels managed by an external transaction manager will always get exposed to
	 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate} calls. So in terms of RabbitTemplate exposure, this
	 * setting only affects locally transacted Channels.
	 * @see ChannelAwareMessageListener
	 */
	public void setExposeListenerChannel(boolean exposeListenerChannel) {
		this.exposeListenerChannel = exposeListenerChannel;
	}

	/**
	 * Set the message listener implementation to register. This can be either a Spring {@link MessageListener} object
	 * or a Spring {@link ChannelAwareMessageListener} object.
	 * @throws IllegalArgumentException if the supplied listener is not a {@link MessageListener} or a
	 * {@link ChannelAwareMessageListener}
	 * @see MessageListener
	 * @see ChannelAwareMessageListener
	 */
	public void setMessageListener(Object messageListener) {
		checkMessageListener(messageListener);
		this.messageListener = messageListener;
	}

	/**
	 * Check the given message listener, throwing an exception if it does not correspond to a supported listener type.
	 * <p>
	 * By default, only a Spring {@link MessageListener} object or a Spring
	 * {@link org.springframework.jms.listener.SessionAwareMessageListener} object will be accepted.
	 * @param messageListener the message listener object to check
	 * @throws IllegalArgumentException if the supplied listener is not a MessageListener or SessionAwareMessageListener
	 * @see MessageListener
	 * @see ChannelAwareMessageListener
	 */
	protected void checkMessageListener(Object messageListener) {
		if (!(messageListener instanceof MessageListener || messageListener instanceof ChannelAwareMessageListener)) {
			throw new IllegalArgumentException("Message listener needs to be of type ["
					+ MessageListener.class.getName() + "] or [" + ChannelAwareMessageListener.class.getName() + "]");
		}
	}

	/**
	 * Return the message listener object to register.
	 */
	public Object getMessageListener() {
		return this.messageListener;
	}

	/**
	 * Set an ErrorHandler to be invoked in case of any uncaught exceptions thrown while processing a Message. By
	 * default there will be <b>no</b> ErrorHandler so that error-level logging is the only result.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set whether to automatically start the container after initialization.
	 * <p>
	 * Default is "true"; set this to "false" to allow for manual startup through the {@link #start()} method.
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Specify the phase in which this container should be started and stopped. The startup order proceeds from lowest
	 * to highest, and the shutdown order is the reverse of that. By default this value is Integer.MAX_VALUE meaning
	 * that this container starts as late as possible and stops as soon as possible.
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
	 * Return the bean name that this listener container has been assigned in its containing bean factory, if any.
	 */
	protected final String getBeanName() {
		return this.beanName;
	}

	/**
	 * Delegates to {@link #validateConfiguration()} and {@link #initialize()}.
	 */
	public final void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.state(
				exposeListenerChannel || !getAcknowledgeMode().isManual(),
				"You cannot acknowledge messages manually if the channel is not exposed to the listener "
						+ "(please check your configuration and set exposeListenerChannel=true or acknowledgeMode!=MANUAL)");
		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && isChannelTransacted()),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having a "
						+ "transactional channel. Either use a different AcknowledgeMode or make sure channelTransacted=false");
		validateConfiguration();
		initialize();
	}

	/**
	 * Validate the configuration of this container.
	 * <p>
	 * The default implementation is empty. To be overridden in subclasses.
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

	// -------------------------------------------------------------------------
	// Lifecycle methods for starting and stopping the container
	// -------------------------------------------------------------------------

	/**
	 * Initialize this container.
	 * <p>
	 * Creates a Rabbit Connection and calls {@link #doInitialize()}.
	 */
	public void initialize() {
		try {
			synchronized (this.lifecycleMonitor) {
				this.lifecycleMonitor.notifyAll();
			}
			doInitialize();
		} catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	/**
	 * Stop the shared Connection, call {@link #doShutdown()}, and close this container.
	 */
	public void shutdown() {
		logger.debug("Shutting down Rabbit listener container");
		synchronized (this.lifecycleMonitor) {
			this.active = false;
			this.lifecycleMonitor.notifyAll();
		}

		// Shut down the invokers.
		try {
			doShutdown();
		} catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		} finally {
			synchronized (this.lifecycleMonitor) {
				this.running = false;
				this.lifecycleMonitor.notifyAll();
			}
		}
	}

	/**
	 * Register any invokers within this container.
	 * <p>
	 * Subclasses need to implement this method for their specific invoker management process.
	 * <p>
	 * A shared Rabbit Connection
	 * @throws Exception
	 * @see #getSharedConnection()
	 */
	protected abstract void doInitialize() throws Exception;

	/**
	 * Close the registered invokers.
	 * <p>
	 * Subclasses need to implement this method for their specific invoker management process.
	 * <p>
	 * A shared Rabbit Connection, if any, will automatically be closed <i>afterwards</i>.
	 * @see #shutdown()
	 */
	protected abstract void doShutdown();

	/**
	 * Return whether this container is currently active, that is, whether it has been set up but not shut down yet.
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
	public void start() {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Starting Rabbit listener container.");
			}
			doStart();
		} catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	/**
	 * Start the shared Connection, if any, and notify all invoker tasks.
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #establishSharedConnection
	 */
	protected void doStart() throws Exception {

		// Reschedule paused tasks, if any.
		synchronized (this.lifecycleMonitor) {
			this.active = true;
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
		} catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		} finally {
			synchronized (this.lifecycleMonitor) {
				this.running = false;
				this.lifecycleMonitor.notifyAll();
			}
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
	}

	/**
	 * Determine whether this container is currently running, that is, whether it has been started and not stopped yet.
	 * @see #start()
	 * @see #stop()
	 * @see #runningAllowed()
	 */
	public final boolean isRunning() {
		synchronized (this.lifecycleMonitor) {
			return (this.running);
		}
	}

	/**
	 * Invoke the registered ErrorHandler, if any. Log at error level otherwise.
	 * @param ex the uncaught error that arose during Rabbit processing.
	 * @see #setErrorHandler
	 */
	protected void invokeErrorHandler(Throwable ex) {
		if (this.errorHandler != null) {
			this.errorHandler.handleError(ex);
		} else if (logger.isDebugEnabled()) {
			logger.debug("Execution of Rabbit message listener failed, and no ErrorHandler has been set.", ex);
		} else if (logger.isInfoEnabled()) {
			logger.info("Execution of Rabbit message listener failed, and no ErrorHandler has been set: "+ex.getClass()+": "+ex.getMessage());
		}
	}

	// -------------------------------------------------------------------------
	// Template methods for listener execution
	// -------------------------------------------------------------------------

	/**
	 * Execute the specified listener, committing or rolling back the transaction afterwards (if necessary).
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @see #invokeListener
	 * @see #commitIfNecessary
	 * @see #rollbackOnExceptionIfNecessary
	 * @see #handleListenerException
	 */
	protected void executeListener(Channel channel, Message message) throws Throwable {
		try {
			doExecuteListener(channel, message);
		} catch (Throwable ex) {
			handleListenerException(ex);
			throw ex;
		}
	}

	/**
	 * Execute the specified listener, committing or rolling back the transaction afterwards (if necessary).
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @throws Throwable
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #invokeListener
	 * @see #commitIfNecessary
	 * @see #rollbackOnExceptionIfNecessary
	 * @see #convertRabbitAccessException
	 */
	protected void doExecuteListener(Channel channel, Message message) throws Throwable {
		if (!isRunning()) {
			if (logger.isWarnEnabled()) {
				logger.warn("Rejecting received message because the listener container has been stopped: " + message);
			}
			rollbackIfNecessary(channel);
			throw new MessageRejectedWhileStoppingException();
		}
		try {
			invokeListener(channel, message);
		} catch (Throwable ex) {
			rollbackOnExceptionIfNecessary(channel, message, ex);
			throw ex;
		}
		commitIfNecessary(channel, message);
	}

	/**
	 * Invoke the specified listener: either as standard MessageListener or (preferably) as SessionAwareMessageListener.
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @throws Exception
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setMessageListener
	 */
	protected void invokeListener(Channel channel, Message message) throws Exception {
		Object listener = getMessageListener();
		if (listener instanceof ChannelAwareMessageListener) {
			doInvokeListener((ChannelAwareMessageListener) listener, channel, message);
		} else if (listener instanceof MessageListener) {
			doInvokeListener((MessageListener) listener, message);
		} else if (listener != null) {
			throw new IllegalArgumentException("Only MessageListener and SessionAwareMessageListener supported: "
					+ listener);
		} else {
			throw new IllegalStateException("No message listener specified - see property 'messageListener'");
		}
	}

	/**
	 * Invoke the specified listener as Spring ChannelAwareMessageListener, exposing a new Rabbit Session (potentially
	 * with its own transaction) to the listener if demanded.
	 * @param listener the Spring ChannelAwareMessageListener to invoke
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @throws Exception if thrown by Rabbit API methods or listener itself.
	 * <p/>
	 * Exception thrown from listener will be wrapped to {@link ListenerExecutionFailedException}.
	 * @see ChannelAwareMessageListener
	 * @see #setExposeListenerChannel(boolean)
	 */
	protected void doInvokeListener(ChannelAwareMessageListener listener, Channel channel, Message message)
			throws Exception {

		RabbitResourceHolder resourceHolder = null;
		try {
			Channel channelToUse = channel;
			if (!isExposeListenerChannel()) {
				// We need to expose a separate Channel.
				resourceHolder = getTransactionalResourceHolder();
				channelToUse = resourceHolder.getChannel();
			}
			// Actually invoke the message listener...
			try {
				listener.onMessage(message, channelToUse);
			} catch (Exception e) {
				throw wrapToListenerExecutionFailedExceptionIfNeeded(e);
			}
		} finally {
			ConnectionFactoryUtils.releaseResources(resourceHolder);
		}
	}

	/**
	 * Invoke the specified listener as Spring Rabbit MessageListener.
	 * <p>
	 * Default implementation performs a plain invocation of the <code>onMessage</code> method.
	 * <p/>
	 * Exception thrown from listener will be wrapped to {@link ListenerExecutionFailedException}.
	 * @param listener the Rabbit MessageListener to invoke
	 * @param message the received Rabbit Message
	 * @see org.springframework.amqp.core.MessageListener#onMessage
	 */
	protected void doInvokeListener(MessageListener listener, Message message) throws Exception {
		try {
			listener.onMessage(message);
		} catch (Exception e) {
			throw wrapToListenerExecutionFailedExceptionIfNeeded(e);
		}
	}

	/**
	 * Perform a commit or message acknowledgement, as appropriate.
	 * @param channel the Rabbit channel to commit
	 * @param message the Message to acknowledge
	 * @throws IOException
	 */
	protected void commitIfNecessary(Channel channel, Message message) throws IOException {

		long deliveryTag = message.getMessageProperties().getDeliveryTag();
		boolean ackRequired = !getAcknowledgeMode().isAutoAck() && !getAcknowledgeMode().isManual();
		if (isChannelLocallyTransacted(channel)) {
			if (ackRequired) {
				channel.basicAck(deliveryTag, false);
			}
			RabbitUtils.commitIfNecessary(channel);
		} else if (isChannelTransacted() && ackRequired) {
			// Not locally transacted but it is transacted so it
			// could be synchronized with an external transaction
			ConnectionFactoryUtils.registerDeliveryTag(getConnectionFactory(), channel, deliveryTag);
		} else if (ackRequired) {
			if (ackRequired) {
				channel.basicAck(deliveryTag, false);
			}
		}

	}

	/**
	 * Perform a rollback, if appropriate.
	 * @param channel the Rabbit Channel to roll back
	 */
	protected void rollbackIfNecessary(Channel channel) {
		boolean ackRequired = !getAcknowledgeMode().isAutoAck() && !getAcknowledgeMode().isManual();
		if (ackRequired) {
			/*
			 * Re-queue messages and don't get them re-delivered to the same consumer, otherwise the broker just spins
			 * trying to get us to accept the same message over and over
			 */
			try {
				channel.basicRecover(true);
			} catch (IOException e) {
				throw new AmqpIOException(e);
			}
		}
		if (this.isChannelLocallyTransacted(channel)) {
			// Transacted channel enabled by this container -> rollback.
			RabbitUtils.rollbackIfNecessary(channel);
		}
	}

	/**
	 * Perform a rollback, handling rollback exceptions properly.
	 * @param channel the Rabbit Channel to roll back
	 * @param ex the thrown application exception or error
	 * @throws Exception in case of a rollback error
	 */
	protected void rollbackOnExceptionIfNecessary(Channel channel, Message message, Throwable ex) throws Exception {

		boolean ackRequired = !getAcknowledgeMode().isAutoAck() && !getAcknowledgeMode().isManual();
		try {
			if (this.isChannelTransacted()) {
				if (logger.isDebugEnabled()) {
					logger.debug("Initiating transaction rollback on application exception: " + ex);
				}
				RabbitUtils.rollbackIfNecessary(channel);
			}
			if (message != null) {
				if (ackRequired) {
					if (logger.isDebugEnabled()) {
						logger.debug("Rejecting message");
					}
					channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
				}
				if (this.isChannelTransacted()) {
					// Need to commit the reject (=nack)
					RabbitUtils.commitIfNecessary(channel);
				}
			}
		} catch (Exception e) {
			logger.error("Application exception overridden by rollback exception", ex);
			throw e;
		}
	}

	/**
	 * Check whether the given Channel is locally transacted, that is, whether its transaction is managed by this
	 * listener container's Channel handling and not by an external transaction coordinator.
	 * <p>
	 * Note:This method is about finding out whether the Channel's transaction is local or externally coordinated.
	 * @param channel the Channel to check
	 * @return whether the given Channel is locally transacted
	 * @see #isChannelTransacted()
	 */
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return this.isChannelTransacted();
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * <p>
	 * The default implementation logs the exception at error level, not propagating it to the Rabbit provider -
	 * assuming that all handling of acknowledgment and/or transactions is done by this listener container. This can be
	 * overridden in subclasses.
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		if (ex instanceof MessageRejectedWhileStoppingException) {
			// Internal exception - has been handled before.
			return;
		}
		if (isActive()) {
			// Regular case: failed while active.
			// Invoke ErrorHandler if available.
			invokeErrorHandler(ex);
		} else {
			// Rare case: listener thread failed after container shutdown.
			// Log at debug level, to avoid spamming the shutdown log.
			logger.debug("Listener exception after container shutdown", ex);
		}
	}

	/**
	 * Internal exception class that indicates a rejected message on shutdown. Used to trigger a rollback for an
	 * external transaction manager in that case.
	 */
	@SuppressWarnings("serial")
	private static class MessageRejectedWhileStoppingException extends RuntimeException {

	}

	/**
	 * Exception that indicates that the initial setup of this container's shared Rabbit Connection failed. This is
	 * indicating to invokers that they need to establish the shared Connection themselves on first access.
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

	/**
	 * @param e
	 * @return If 'e' is of type {@link ListenerExecutionFailedException} - return 'e' as it is, otherwise wrap it to
	 * {@link ListenerExecutionFailedException} and return.
	 */
	protected Exception wrapToListenerExecutionFailedExceptionIfNeeded(Exception e) {
		if (!(e instanceof ListenerExecutionFailedException)) {
			// Wrap exception to ListenerExecutionFailedException.
			return new ListenerExecutionFailedException("Listener threw exception", e);
		}
		return e;
	}
}
