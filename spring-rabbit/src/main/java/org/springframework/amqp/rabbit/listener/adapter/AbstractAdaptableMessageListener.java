/*
 * Copyright 2014-2018 the original author or authors.
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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;

/**
 * An abstract {@link MessageListener} adapter providing the necessary infrastructure
 * to extract the payload of a {@link Message}.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4
 *
 * @see MessageListener
 * @see ChannelAwareMessageListener
 */
public abstract class AbstractAdaptableMessageListener implements MessageListener, ChannelAwareMessageListener {

	private static final String DEFAULT_RESPONSE_ROUTING_KEY = "";

	private static final String DEFAULT_ENCODING = "UTF-8";

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	/** Logger available to subclasses. */
	protected final Log logger = LogFactory.getLog(getClass());

	private final StandardEvaluationContext evalContext = new StandardEvaluationContext();

	private String responseRoutingKey = DEFAULT_RESPONSE_ROUTING_KEY;

	private String responseExchange = null;

	private Address responseAddress = null;

	private Expression responseExpression;

	private volatile boolean mandatoryPublish;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private String encoding = DEFAULT_ENCODING;

	private MessagePostProcessor[] beforeSendReplyPostProcessors;

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
	 * The encoding to use when inter-converting between byte arrays and Strings in
	 * message properties.
	 * @param encoding the encoding to set.
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * The encoding to use when inter-converting between byte arrays and Strings in
	 * message properties.
	 * @return encoding the encoding.
	 */
	public String getEncoding() {
		return this.encoding;
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

	/**
	 * Set the default replyTo address to use when sending response messages.
	 * This is only used if the replyTo from the received message is null.
	 * <p>
	 * Response destinations are only relevant for listener methods
	 * that return result objects, which will be wrapped in
	 * a response message and sent to a response destination.
	 * <p>
	 * Can be a string starting with "SpEL:" in which case the expression is
	 * evaluated at runtime; see the reference manual for more information.
	 * @param defaultReplyTo The exchange.
	 * @since 1.6
	 */
	public void setResponseAddress(String defaultReplyTo) {
		if (defaultReplyTo.startsWith(PARSER_CONTEXT.getExpressionPrefix())) {
			this.responseExpression = PARSER.parseExpression(defaultReplyTo, PARSER_CONTEXT);
		}
		else {
			this.responseAddress = new Address(defaultReplyTo);
		}
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
	 * Set a post processor to process the reply immediately before
	 * {@code Channel#basicPublish()}. Often used to compress the data.
	 * @param replyPostProcessor the reply post processor.
	 * @deprecated in favor of
	 * {@link #setBeforeSendReplyPostProcessors(MessagePostProcessor...)}.
	 */
	@Deprecated
	public void setReplyPostProcessor(MessagePostProcessor replyPostProcessor) {
		setBeforeSendReplyPostProcessors(replyPostProcessor);
	}

	/**
	 * Set post processors that will be applied before sending replies.
	 * @param beforeSendReplyPostProcessors the post processors.
	 * @since 2.0.3
	 */
	public void setBeforeSendReplyPostProcessors(MessagePostProcessor... beforeSendReplyPostProcessors) {
		Assert.noNullElements(beforeSendReplyPostProcessors, "'replyPostProcessors' must not have any null elements");
		this.beforeSendReplyPostProcessors = Arrays.copyOf(beforeSendReplyPostProcessors,
				beforeSendReplyPostProcessors.length);
	}

	/**
	 * Set a bean resolver for runtime SpEL expressions. Also configures the evaluation
	 * context with a standard type converter and map accessor.
	 * @param beanResolver the resolver.
	 * @since 1.6
	 */
	public void setBeanResolver(BeanResolver beanResolver) {
		this.evalContext.setBeanResolver(beanResolver);
		this.evalContext.setTypeConverter(new StandardTypeConverter());
		this.evalContext.addPropertyAccessor(new MapAccessor());
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
	 * Delegates the message to the target listener method, with appropriate conversion of the message argument.
	 * <p>
	 * <b>Note:</b> Does not support sending response messages based on result objects returned from listener methods.
	 * Use the {@link ChannelAwareMessageListener} entry point (typically through a Spring message listener container)
	 * for handling result objects as well.
	 * @param message the incoming Rabbit message
	 * @see #onMessage(Message, com.rabbitmq.client.Channel)
	 */
	@Override
	public void onMessage(Message message) {
		try {
			onMessage(message, null);
		}
		catch (Exception e) {
			throw new ListenerExecutionFailedException("Listener threw exception", e, message);
		}
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * The default implementation logs the exception at error level.
	 * <p>
	 * Can be used by inheritors from overridden {@link #onMessage(Message)}
	 * or {@link #onMessage(Message, com.rabbitmq.client.Channel)}
	 * @param ex the exception to handle
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
	 * Handle the given result object returned from the listener method, sending a
	 * response message back.
	 * @param resultArg the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param channel the Rabbit channel to operate on (may be <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #buildMessage
	 * @see #postProcessResponse
	 * @see #getReplyToAddress(Message, Object, Object)
	 * @see #sendResponse
	 */
	protected void handleResult(Object resultArg, Message request, Channel channel) throws Exception {
		handleResult(resultArg, request, channel, null);
	}

	/**
	 * Handle the given result object returned from the listener method, sending a
	 * response message back.
	 * @param resultArg the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param channel the Rabbit channel to operate on (may be <code>null</code>)
	 * @param source the source data for the method invocation - e.g.
	 * {@code o.s.messaging.Message<?>}; may be null
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #buildMessage
	 * @see #postProcessResponse
	 * @see #getReplyToAddress(Message, Object, Object)
	 * @see #sendResponse
	 */
	protected void handleResult(Object resultArg, Message request, Channel channel, Object source) throws Exception {
		if (channel != null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Listener method returned result [" + resultArg
						+ "] - generating response message for it");
			}
			try {
				Object result = resultArg instanceof ResultHolder ? ((ResultHolder) resultArg).result : resultArg;
				Message response = buildMessage(channel, result);
				postProcessResponse(request, response);
				Address replyTo = getReplyToAddress(request, source, resultArg);
				sendResponse(channel, replyTo, response);
			}
			catch (Exception ex) {
				throw new ReplyFailureException("Failed to send reply with payload '" + resultArg + "'", ex);
			}
		}
		else if (this.logger.isWarnEnabled()) {
			this.logger.warn("Listener method returned result [" + resultArg
					+ "]: not generating response message for it because no Rabbit Channel given");
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
		String correlation = request.getMessageProperties().getCorrelationId();

		if (correlation == null) {
			String messageId = request.getMessageProperties().getMessageId();
			if (messageId != null) {
				correlation = messageId;
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
	 * @param request the original incoming Rabbit message.
	 * @param source the source data (e.g. {@code o.s.messaging.Message<?>}).
	 * @param result the result.
	 * @return the reply-to Address (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @throws org.springframework.amqp.AmqpException if no {@link Address} can be determined
	 * @see #setResponseAddress(String)
	 * @see #setResponseRoutingKey(String)
	 * @see org.springframework.amqp.core.Message#getMessageProperties()
	 * @see org.springframework.amqp.core.MessageProperties#getReplyTo()
	 */
	protected Address getReplyToAddress(Message request, Object source, Object result) throws Exception {
		Address replyTo = request.getMessageProperties().getReplyToAddress();
		if (replyTo == null) {
			if (this.responseAddress == null && this.responseExchange != null) {
				this.responseAddress = new Address(this.responseExchange, this.responseRoutingKey);
			}
			if (result instanceof ResultHolder) {
				replyTo = evaluateReplyTo(request, source, result, ((ResultHolder) result).sendTo);
			}
			else if (this.responseExpression != null) {
				replyTo = evaluateReplyTo(request, source, result, this.responseExpression);
			}
			else if (this.responseAddress == null) {
				throw new AmqpException(
						"Cannot determine ReplyTo message property value: " +
								"Request message does not contain reply-to property, " +
								"and no default response Exchange was set.");
			}
			else {
				replyTo = this.responseAddress;
			}
		}
		return replyTo;
	}

	private Address evaluateReplyTo(Message request, Object source, Object result, Expression expression) {
		Address replyTo = null;
		Object value = expression.getValue(this.evalContext, new ReplyExpressionRoot(request, source, result));
		Assert.state(value instanceof String || value instanceof Address,
				"response expression must evaluate to a String or Address");
		if (value instanceof String) {
			replyTo = new Address((String) value);
		}
		else {
			replyTo = (Address) value;
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
		Message message = messageIn;
		if (this.beforeSendReplyPostProcessors != null) {
			for (MessagePostProcessor postProcessor : this.beforeSendReplyPostProcessors) {
				message = postProcessor.postProcessMessage(message);
			}
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

	/**
	 * Result holder.
	 */
	public static final class ResultHolder {

		private final Object result;

		private final Expression sendTo;

		public ResultHolder(Object result, Expression sendTo) {
			this.result = result;
			this.sendTo = sendTo;
		}

		@Override
		public String toString() {
			return this.result.toString();
		}

	}

	/**
	 * Root object for reply expression evaluation.
	 */
	public static final class ReplyExpressionRoot {

		private final Message request;

		private final Object source;

		private final Object result;

		public ReplyExpressionRoot(Message request, Object source, Object result) {
			this.request = request;
			this.source = source;
			this.result = result;
		}

		public Message getRequest() {
			return this.request;
		}

		public Object getSource() {
			return this.source;
		}

		public Object getResult() {
			return this.result;
		}

	}

}
