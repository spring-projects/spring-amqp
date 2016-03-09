/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import com.rabbitmq.client.Channel;

/**
 * An abstract {@link MessageListener} adapter providing the necessary infrastructure
 * to extract the payload of a {@link Message}
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 * @see MessageListener
 * @see ChannelAwareMessageListener
 */
public abstract class AbstractAdaptableMessageListener implements MessageListener, ChannelAwareMessageListener {

	private static final String DEFAULT_RESPONSE_ROUTING_KEY = "";

	private static final String DEFAULT_ENCODING = "UTF-8";


	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private String responseRoutingKey = DEFAULT_RESPONSE_ROUTING_KEY;

	private String responseExchange = null;

	private volatile boolean mandatoryPublish;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private String encoding = DEFAULT_ENCODING;

	private MessagePostProcessor replyPostProcessor;

	/**
	 * Set the routing key to use when sending response messages.
	 * This will be applied in case of a request message that
	 * does not carry a "ReplyTo" property
	 * <p>
	 * Response destinations are only relevant for listener methods
	 * that return result objects, which will be wrapped in
	 * a response message and sent to a response destination.
	 * @param responseRoutingKey The routing key.
	 */
	public void setResponseRoutingKey(String responseRoutingKey) {
		this.responseRoutingKey = responseRoutingKey;
	}

	/**
	 * The encoding to use when inter-converting between byte arrays and Strings in message properties.
	 * @param encoding the encoding to set
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Set the exchange to use when sending response messages.
	 * This is only used if the exchange from the received message is null.
	 * <p>
	 * Response destinations are only relevant for listener methods
	 * that return result objects, which will be wrapped in
	 * a response message and sent to a response destination.
	 * @param responseExchange The exchange.
	 */
	public void setResponseExchange(String responseExchange) {
		this.responseExchange = responseExchange;
	}

	public void setMandatoryPublish(boolean mandatoryPublish) {
		this.mandatoryPublish = mandatoryPublish;
	}

	/**
	 * Set the converter that will convert incoming Rabbit messages to listener method arguments, and objects returned
	 * from listener methods back to Rabbit messages.
	 * <p>
	 * The default converter is a {@link SimpleMessageConverter}, which is able to handle "text" content-types.
	 * @param messageConverter The message converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Set a post processor to process the reply immediately before {@code Channel#basicPublish()}.
	 * Often used to compress the data.
	 * @param replyPostProcessor the reply post processor.
	 */
	public void setReplyPostProcessor(MessagePostProcessor replyPostProcessor) {
		this.replyPostProcessor = replyPostProcessor;
	}

	/**
	 * Return the converter that will convert incoming Rabbit messages to listener method arguments, and objects
	 * returned from listener methods back to Rabbit messages.
	 * @return The message converter.
	 */
	protected MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Rabbit {@link MessageListener} entry point.
	 * <p>
	 * Delegates the message to the target listener method, with appropriate conversion of the message argument. In case
	 * of an exception, the {@link #handleListenerException(Throwable)} method will be invoked.
	 * <p>
	 * <b>Note:</b> Does not support sending response messages based on result objects returned from listener methods.
	 * Use the {@link ChannelAwareMessageListener} entry point (typically through a Spring message listener container)
	 * for handling result objects as well.
	 * @param message the incoming Rabbit message
	 * @see #handleListenerException
	 * @see #onMessage(Message, com.rabbitmq.client.Channel)
	 */
	@Override
	public void onMessage(Message message) {
		try {
			onMessage(message, null);
		}
		catch (Exception ex) {
			handleListenerException(ex);
		}
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * The default implementation logs the exception at error level.
	 * <p>
	 * This method only applies when using a Rabbit {@link MessageListener}. With
	 * {@link ChannelAwareMessageListener}, exceptions get handled by the
	 * caller instead.
	 * @param ex the exception to handle
	 * @see #onMessage(Message)
	 */
	protected void handleListenerException(Throwable ex) {
		this.logger.error("Listener execution failed", ex);
	}

	/**
	 * Extract the message body from the given Rabbit message.
	 * @param message the Rabbit <code>Message</code>
	 * @return the content of the message, to be passed into the listener method as argument
	 */
	protected Object extractMessage(Message message) {
		MessageConverter converter = getMessageConverter();
		if (converter != null) {
			return converter.fromMessage(message);
		}
		return message;
	}

	/**
	 * Handle the given result object returned from the listener method, sending a response message back.
	 * @param result the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param channel the Rabbit channel to operate on (may be <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #buildMessage
	 * @see #postProcessResponse
	 * @see #getReplyToAddress(Message)
	 * @see #sendResponse
	 */
	protected void handleResult(Object result, Message request, Channel channel) throws Exception {
		if (channel != null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Listener method returned result [" + result + "] - generating response message for it");
			}
			try {
				Message response = buildMessage(channel, result);
				postProcessResponse(request, response);
				Address replyTo = getReplyToAddress(request);
				sendResponse(channel, replyTo, response);
			}
			catch (Exception ex) {
				throw new ReplyFailureException("Failed to send reply with payload '" + result + "'", ex);
			}
		}
		else if (this.logger.isWarnEnabled()) {
			this.logger.warn("Listener method returned result [" + result
					+ "]: not generating response message for it because of no Rabbit Channel given");
		}
	}

	protected String getReceivedExchange(Message request) {
		return request.getMessageProperties().getReceivedExchange();
	}

	/**
	 * Build a Rabbit message to be sent as response based on the given result object.
	 * @param channel the Rabbit Channel to operate on
	 * @param result the content of the message, as returned from the listener method
	 * @return the Rabbit <code>Message</code> (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setMessageConverter
	 */
	protected Message buildMessage(Channel channel, Object result) throws Exception {
		MessageConverter converter = getMessageConverter();
		if (converter != null && !(result instanceof Message)) {
			return converter.toMessage(result, new MessageProperties());
		}
		else {
			if (!(result instanceof Message)) {
				throw new MessageConversionException("No MessageConverter specified - cannot handle message ["
						+ result + "]");
			}
			return (Message) result;
		}
	}

	/**
	 * Post-process the given response message before it will be sent.
	 * <p>
	 * The default implementation sets the response's correlation id to the request message's correlation id, if any;
	 * otherwise to the request message id.
	 * @param request the original incoming Rabbit message
	 * @param response the outgoing Rabbit message about to be sent
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected void postProcessResponse(Message request, Message response) throws Exception {
		byte[] correlation = request.getMessageProperties().getCorrelationId();

		if (correlation == null) {
			String messageId = request.getMessageProperties().getMessageId();
			if (messageId != null) {
				correlation = messageId.getBytes(SimpleMessageConverter.DEFAULT_CHARSET);
			}
		}
		response.getMessageProperties().setCorrelationId(correlation);
	}

	/**
	 * Determine a reply-to Address for the given message.
	 * <p>
	 * The default implementation first checks the Rabbit Reply-To Address of the supplied request; if that is not
	 * <code>null</code> it is returned; if it is <code>null</code>, then the configured default response Exchange and
	 * routing key are used to construct a reply-to Address. If the responseExchange property is also <code>null</code>,
	 * then an {@link org.springframework.amqp.AmqpException} is thrown.
	 * @param request the original incoming Rabbit message
	 * @return the reply-to Address (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @throws org.springframework.amqp.AmqpException if no {@link Address} can be determined
	 * @see #setResponseExchange(String)
	 * @see #setResponseRoutingKey(String)
	 * @see org.springframework.amqp.core.Message#getMessageProperties()
	 * @see org.springframework.amqp.core.MessageProperties#getReplyTo()
	 */
	protected Address getReplyToAddress(Message request) throws Exception {
		Address replyTo = request.getMessageProperties().getReplyToAddress();
		if (replyTo == null) {
			if (this.responseExchange == null) {
				throw new AmqpException(
						"Cannot determine ReplyTo message property value: " +
								"Request message does not contain reply-to property, " +
								"and no default response Exchange was set.");
			}
			replyTo = new Address(this.responseExchange, this.responseRoutingKey);
		}
		return replyTo;
	}

	/**
	 * Send the given response message to the given destination.
	 * @param channel the Rabbit channel to operate on
	 * @param replyTo the Rabbit ReplyTo string to use when sending. Currently interpreted to be the routing key.
	 * @param messageIn the Rabbit message to send
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #postProcessResponse(Message, Message)
	 */
	protected void sendResponse(Channel channel, Address replyTo, Message messageIn) throws Exception {
		Message message;
		if (this.replyPostProcessor == null) {
			message = messageIn;
		}
		else {
			message = this.replyPostProcessor.postProcessMessage(messageIn);
		}
		postProcessChannel(channel, message);

		try {
			this.logger.debug("Publishing response to exchange = [" + replyTo.getExchangeName() + "], routingKey = ["
					+ replyTo.getRoutingKey() + "]");
			channel.basicPublish(replyTo.getExchangeName(), replyTo.getRoutingKey(), this.mandatoryPublish,
					this.messagePropertiesConverter.fromMessageProperties(message.getMessageProperties(), this.encoding),
					message.getBody());
		}
		catch (Exception ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
	}

	/**
	 * Post-process the given message before sending the response.
	 * <p>
	 * The default implementation is empty.
	 *
	 * @param channel The channel.
	 * @param response the outgoing Rabbit message about to be sent
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected void postProcessChannel(Channel channel, Message response) throws Exception {
	}

}
