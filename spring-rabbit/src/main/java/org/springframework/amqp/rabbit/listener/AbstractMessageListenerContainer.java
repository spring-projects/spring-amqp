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

import java.io.IOException;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 */
public abstract class AbstractMessageListenerContainer extends AbstractRabbitListeningContainer {

	private volatile String queueName;
	
	private ErrorHandler errorHandler;
	
	private boolean exposeListenerChannel = true;
	
	private volatile Object messageListener;

	protected boolean autoAck = false;


	/**
	 * Set the name of the queue to receive messages from.	 
	 * @param queueName the desired queue (can not be <code>null</code>)
	 */
	public void setQueueName(String queueName) {
		//TODO change to QueueNames(String... queueNames)
		Assert.notNull(queueName, "'queueName' must not be null");
		this.queueName = queueName;
	}
	
	public void setQueues(Queue... queues) {
		//TODO check for null arg value, refactor out of string based conventions. Merge with string queue name values?
		StringBuilder sb = new StringBuilder();
		int size = queues.length;
		for (int i=0; i < size; i++) {
			sb.append(queues[i].getName());
			if (i != size-1) sb.append(",");
		}
		this.queueName = sb.toString();
	}

	/**
	 * Return the name of the queue to receive messages from.
	 */
	public String getQueueName() {
		return this.queueName;
	}

	protected String getRequiredQueueName() {
		Assert.notNull(this.queueName, "Queue name must not be null.");
		return this.queueName;
	}

	public boolean isAutoAck() {
		return this.autoAck;
	}

	public void setAutoAck(boolean autoAck) {
		this.autoAck = autoAck;
	}

	/**
	 * Return whether to expose the listener {@link Channel} to a
	 * registered {@link ChannelAwareMessageListener}.
	 */
	public boolean isExposeListenerChannel() {
		return this.exposeListenerChannel;
	}
	
	/**
	 * Set whether to expose the listener Rabbit Channel to a registered
	 * {@link ChannelAwareMessageListener} as well as to
	 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate} calls.
	 * <p>Default is "true", reusing the listener's {@link Channel}.
	 * Turn this off to expose a fresh Rabbit Channel fetched from the same
	 * underlying Rabbit {@link Connection} instead.
	 * <p>Note that Channels managed by an external transaction manager will
	 * always get exposed to {@link org.springframework.amqp.rabbit.core.RabbitTemplate}
	 * calls. So in terms of RabbitTemplate exposure, this setting only affects
	 * locally transacted Channels.
	 * @see ChannelAwareMessageListener
	 */
	public void setExposeListenerChannel(boolean exposeListenerChannel) {
		this.exposeListenerChannel = exposeListenerChannel;
	}

	/**
	 * Set the message listener implementation to register.
	 * This can be either a Spring {@link MessageListener} object
	 * or a Spring {@link ChannelAwareMessageListener} object.
	 * @throws IllegalArgumentException if the supplied listener is not a
	 * {@link MessageListener} or a {@link ChannelAwareMessageListener}
	 * @see MessageListener
	 * @see ChannelAwareMessageListener
	 */
	public void setMessageListener(Object messageListener) {
		checkMessageListener(messageListener);
		this.messageListener = messageListener;		
	}
	
	/**
	 * Check the given message listener, throwing an exception
	 * if it does not correspond to a supported listener type.
	 * <p>By default, only a Spring {@link MessageListener} object or a
	 * Spring {@link SessionAwareMessageListener} object will be accepted.
	 * @param messageListener the message listener object to check
	 * @throws IllegalArgumentException if the supplied listener is not a
	 * {@link MessageListener} or a {@link SessionAwareMessageListener}
	 * @see MessageListener
	 * @see ChannelAwareMessageListener
	 */
	protected void checkMessageListener(Object messageListener) {
		if (!(messageListener instanceof MessageListener ||
				messageListener instanceof ChannelAwareMessageListener)) {
			throw new IllegalArgumentException(
					"Message listener needs to be of type [" + MessageListener.class.getName() +
					"] or [" + ChannelAwareMessageListener.class.getName() + "]");
		}
	}
	
	/**
	 * Return the message listener object to register.
	 */
	public Object getMessageListener() {
		return this.messageListener;
	}
	
	
	
	/**
	 * Set an ErrorHandler to be invoked in case of any uncaught exceptions thrown
	 * while processing a Message. By default there will be <b>no</b> ErrorHandler
	 * so that error-level logging is the only result.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}
	
	
	/**
	 * Invoke the registered ErrorHandler, if any. Log at error level otherwise.
	 * @param ex the uncaught error that arose during Rabbit processing.
	 * @see #setErrorHandler
	 */
	protected void invokeErrorHandler(Throwable ex) {
		if (this.errorHandler != null) {
			this.errorHandler.handleError(ex);
		}
		else if (logger.isWarnEnabled()) {
			logger.warn("Execution of Rabbit message listener failed, and no ErrorHandler has been set.", ex);
		}
	}
	
	//-------------------------------------------------------------------------
	// Template methods for listener execution
	//-------------------------------------------------------------------------

	/**
	 * Execute the specified listener,
	 * committing or rolling back the transaction afterwards (if necessary).
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @see #invokeListener
	 * @see #commitIfNecessary
	 * @see #rollbackOnExceptionIfNecessary
	 * @see #handleListenerException
	 */
	protected void executeListener(Channel channel, Message message) {
		try {
			doExecuteListener(channel, message);
		}
		catch (Throwable ex) {
			handleListenerException(ex);
		}
	}
	
	
	/**
	 * Execute the specified listener,
	 * committing or rolling back the transaction afterwards (if necessary).
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
		//TODO consider adding support for AcceptMessagesWhileStopping functionality
		if (!isRunning()) {
			if (logger.isWarnEnabled()) {
				logger.warn("Rejecting received message because of the listener container " +
						"having been stopped in the meantime: " + message);
			}
			rollbackIfNecessary(channel);
			throw new MessageRejectedWhileStoppingException();
		}
		try {
			invokeListener(channel, message);
		}
		catch (Throwable ex) {
			rollbackOnExceptionIfNecessary(channel, ex);
			throw ex;
		}				
		commitIfNecessary(channel, message);
	}
	
	/**
	 * Invoke the specified listener: either as standard MessageListener
	 * or (preferably) as SessionAwareMessageListener.
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
		}
		else if (listener instanceof MessageListener) {
			doInvokeListener((MessageListener) listener, message);
		}
		else if (listener != null) {
			throw new IllegalArgumentException(
					"Only MessageListener and SessionAwareMessageListener supported: " + listener);
		}
		else {
			throw new IllegalStateException("No message listener specified - see property 'messageListener'");
		}
	}
	
	/**
	 * Invoke the specified listener as Spring ChannelAwareMessageListener,
	 * exposing a new Rabbit Session (potentially with its own transaction)
	 * to the listener if demanded.
	 * @param listener the Spring ChannelAwareMessageListener to invoke
	 * @param session the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @throws Exception if thrown by Rabbit API methods
	 * @see ChannelAwareMessageListener
	 * @see #setExposeListenerSession
	 */
	protected void doInvokeListener(ChannelAwareMessageListener listener, Channel channel, Message message) throws Exception {

		Connection conToClose = null;
		Channel channelToClose = null;
		try {
			Channel channelToUse = channel;
			if (!isExposeListenerChannel()) {
				// We need to expose a separate Session.
				conToClose = createConnection();
				channelToClose = createChannel(conToClose);
				channelToUse = channelToClose;
			}
			// Actually invoke the message listener...
			listener.onMessage(message, channelToUse);
			
			//TODO need to figure out tx usage more.
			/*
			// Clean up specially exposed Channel, if any.
			if (channelToUse != channel) {
				if (channelToUse.getTransacted() && isChannelLocallyTransacted(channelToUse)) {
					// Transacted session created by this container -> commit.
					RabbitUtils.commitIfNecessary(channelToUse);
				}
			}*/
		}
		finally {
			RabbitUtils.closeChannel(channelToClose);
			RabbitUtils.closeConnection(conToClose);
		}
	}
	
	/**
	 * Invoke the specified listener as Spring Rabbit MessageListener.
	 * <p>Default implementation performs a plain invocation of the
	 * <code>onMessage</code> method.
	 * @param listener the Rabbit MessageListener to invoke
	 * @param message the received Rabbit Message
	 * @throws Exception if thrown by RAbbit API methods
	 * @see org.springframework.core.amq.MessageListener#onMessage
	 */
	protected void doInvokeListener(MessageListener listener, Message message) {
		listener.onMessage(message);
	}



	/**
	 * Perform a commit or message acknowledgement, as appropriate.
	 * @param channel the Rabbit channel to commit
	 * @param message the Message to acknowledge
	 * @throws Exception in case of commit failure
	 */
	protected void commitIfNecessary(Channel channel, Message message)  {
		
		//TODO Should probably go to doExecuteListener() of base class.
        if (!autoAck) {
        	if (channel.isOpen()) {
        		try {
					channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.warn("Could not ack message with delivery tag [" + message.getMessageProperties().getDeliveryTag() + "]", e);
				}
        	}
        }
        if (this.isChannelLocallyTransacted(channel)) {
        	RabbitUtils.commitIfNecessary(channel);
        }		
	}

	/**
	 * Perform a rollback, if appropriate.
	 * @param session the Rabbit Channel to rollback
	 * @throws Exception in case of a rollback error
	 */
	protected void rollbackIfNecessary(Channel channel)  {
		if (this.isChannelLocallyTransacted(channel)) {
			// Transacted channel enabled by this container -> rollback.
			RabbitUtils.rollbackIfNecessary(channel);
		}		
	}

	/**
	 * Perform a rollback, handling rollback exceptions properly.
	 * @param session the Rabbit Channel to rollback
	 * @param ex the thrown application exception or error
	 * @throws Exception in case of a rollback error
	 */
	protected void rollbackOnExceptionIfNecessary(Channel channel, Throwable ex) throws Exception {
		
		//TODO not sure if the exception at this point if only from the application or from Rabbit.
		try {
			if (this.isChannelLocallyTransacted(channel)) {
				if (logger.isDebugEnabled()) {
					logger.debug("Initiating transaction rollback on application exception", ex);
				}
				RabbitUtils.rollbackIfNecessary(channel);
			}
		} catch (Exception ex2)
		{
			logger.error("Application exception overridden by rollback exception", ex);
			throw ex2;
		}		
	}
	
	/**
	 * Check whether the given Channel is locally transacted, that is, whether
	 * its transaction is managed by this listener container's Channel handling
	 * and not by an external transaction coordinator.
	 * <p>Note:This method is about finding out whether the Channel's transaction
	 * is local or externally coordinated.
	 * @param channel the Channel to check
	 * @return whether the given Channel is locally transacted
	 * @see #isChannelTransacted()
	 */
	protected boolean isChannelLocallyTransacted(Channel channel) {
		return this.isChannelTransacted();
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * <p>The default implementation logs the exception at error level,
	 * not propagating it to the Rabbit provider - assuming that all handling of
	 * acknowledgment and/or transactions is done by this listener container.
	 * This can be overridden in subclasses.
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		if (ex instanceof MessageRejectedWhileStoppingException) {
			// Internal exception - has been handled before.
			return;
		}
		/* TODO how to handle exceptions that rabbit might throw
		if (ex instanceof JMSException) {
			invokeExceptionListener((JMSException) ex);
		}
		*/
		if (isActive()) {
			// Regular case: failed while active.
			// Invoke ErrorHandler if available.
			invokeErrorHandler(ex);
		}
		else {
			// Rare case: listener thread failed after container shutdown.
			// Log at debug level, to avoid spamming the shutdown log.
			logger.debug("Listener exception after container shutdown", ex);
		}
	}


	/**
	 * Internal exception class that indicates a rejected message on shutdown.
	 * Used to trigger a rollback for an external transaction manager in that case.
	 */
	@SuppressWarnings("serial")
	private static class MessageRejectedWhileStoppingException extends RuntimeException {

	}
}
