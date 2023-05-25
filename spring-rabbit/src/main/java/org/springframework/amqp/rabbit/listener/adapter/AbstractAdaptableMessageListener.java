/*
 * Copyright 2014-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.support.SendRetryContextAccessor;
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
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.rabbitmq.client.Channel;

/**
 * An abstract {@link org.springframework.amqp.core.MessageListener} adapter providing the
 * necessary infrastructure to extract the payload of a {@link Message}.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Johan Haleby
 *
 * @since 1.4
 *
 * @see ChannelAwareMessageListener
 */
public abstract class AbstractAdaptableMessageListener implements ChannelAwareMessageListener {

	private static final String DEFAULT_RESPONSE_ROUTING_KEY = "";

	private static final String DEFAULT_ENCODING = "UTF-8";

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	static final boolean monoPresent = // NOSONAR - lower case, protected
			ClassUtils.isPresent("reactor.core.publisher.Mono", ChannelAwareMessageListener.class.getClassLoader());

	/**
	 * Logger available to subclasses.
	 */
	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR protected

	private final StandardEvaluationContext evalContext = new StandardEvaluationContext();

	private String responseRoutingKey = DEFAULT_RESPONSE_ROUTING_KEY;

	private String responseExchange = null;

	private Address responseAddress = null;

	private Expression responseExpression;

	private boolean mandatoryPublish;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private String encoding = DEFAULT_ENCODING;

	private MessagePostProcessor[] beforeSendReplyPostProcessors;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<?> recoveryCallback;

	private boolean isManualAck;

	private boolean defaultRequeueRejected = true;

	private ReplyPostProcessor replyPostProcessor;

	private String replyContentType;

	private boolean converterWinsContentType = true;

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
	 * It is parsed in {@link Address} so should be of the form exchange/rk.
	 * <p>
	 * It can be a string surrounded by "!{...}" in which case the expression is
	 * evaluated at runtime; see the reference manual for more information.
	 * @param defaultReplyTo The replyTo address.
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
	 * Set a {@link RetryTemplate} to use when sending replies.
	 * @param retryTemplate the template.
	 * @since 2.0.6
	 * @see #setRecoveryCallback(RecoveryCallback)
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	/**
	 * Set a {@link RecoveryCallback} to invoke when retries are exhausted.
	 * @param recoveryCallback the recovery callback.
	 * @since 2.0.6
	 * @see #setRetryTemplate(RetryTemplate)
	 */
	public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
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
	 * Set a {@link ReplyPostProcessor} to post process a response message before it is
	 * sent. It is called after {@link #postProcessResponse(Message, Message)} which sets
	 * up the correlationId header.
	 * @param replyPostProcessor the post processor.
	 * @since 2.2.5
	 */
	public void setReplyPostProcessor(ReplyPostProcessor replyPostProcessor) {
		this.replyPostProcessor = replyPostProcessor;
	}

	/**
	 * Get the reply content type.
	 * @return the content type.
	 * @since 2.3
	 */
	protected String getReplyContentType() {
		return this.replyContentType;
	}

	/**
	 * Set the reply content type.
	 * @param replyContentType the content type.
	 * @since 2.3
	 */
	public void setReplyContentType(String replyContentType) {
		this.replyContentType = replyContentType;
	}

	/**
	 * Return whether the content type set by a converter prevails or not.
	 * @return false to always apply the reply content type.
	 * @since 2.3
	 */
	protected boolean isConverterWinsContentType() {
		return this.converterWinsContentType;
	}

	/**
	 * Set whether the content type set by a converter prevails or not.
	 * @param converterWinsContentType false to always apply the reply content type.
	 * @since 2.3
	 */
	public void setConverterWinsContentType(boolean converterWinsContentType) {
		this.converterWinsContentType = converterWinsContentType;
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
	 * Set to the value of this listener's container equivalent property. Used when
	 * rejecting from an async listener.
	 * @param defaultRequeueRejected false to not requeue.
	 * @since 2.1.8
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	@Override
	public void containerAckMode(AcknowledgeMode mode) {
		this.isManualAck = AcknowledgeMode.MANUAL.equals(mode);
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
	 * @see #buildMessage
	 * @see #postProcessResponse
	 * @see #getReplyToAddress(Message, Object, InvocationResult)
	 * @see #sendResponse
	 */
	protected void handleResult(InvocationResult resultArg, Message request, Channel channel) {
		handleResult(resultArg, request, channel, null);
	}

	/**
	 * Handle the given result object returned from the listener method, sending a
	 * response message back.
	 * @param resultArg the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param channel the Rabbit channel to operate on (maybe <code>null</code>)
	 * @param source the source data for the method invocation - e.g.
	 * {@code o.s.messaging.Message<?>}; may be null
	 * @see #buildMessage
	 * @see #postProcessResponse
	 * @see #getReplyToAddress(Message, Object, InvocationResult)
	 * @see #sendResponse
	 */
	protected void handleResult(InvocationResult resultArg, Message request, Channel channel, Object source) {
		if (channel != null) {
			if (resultArg.getReturnValue() instanceof CompletableFuture<?> completable) {
				if (!this.isManualAck) {
					this.logger.warn("Container AcknowledgeMode must be MANUAL for a Future<?> return type; "
							+ "otherwise the container will ack the message immediately");
				}
				completable.whenComplete((r, t) -> {
						if (t == null) {
							asyncSuccess(resultArg, request, channel, source, r);
							basicAck(request, channel);
						}
						else {
							asyncFailure(request, channel, t);
						}
				});
			}
			else if (monoPresent && MonoHandler.isMono(resultArg.getReturnValue())) {
				if (!this.isManualAck) {
					this.logger.warn("Container AcknowledgeMode must be MANUAL for a Mono<?> return type" +
							"(or Kotlin suspend function); otherwise the container will ack the message immediately");
				}
				MonoHandler.subscribe(resultArg.getReturnValue(),
						r -> asyncSuccess(resultArg, request, channel, source, r),
						t -> asyncFailure(request, channel, t),
						() -> basicAck(request, channel));
			}
			else {
				doHandleResult(resultArg, request, channel, source);
			}
		}
		else if (this.logger.isWarnEnabled()) {
			this.logger.warn("Listener method returned result [" + resultArg
					+ "]: not generating response message for it because no Rabbit Channel given");
		}
	}

	private void asyncSuccess(InvocationResult resultArg, Message request, Channel channel, Object source,
			Object deferredResult) {

		if (deferredResult == null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Async result is null, ignoring");
			}
		}
		else {
			// We only get here with Mono<?> and ListenableFuture<?> which have exactly one type argument
			Type returnType = resultArg.getReturnType();
			if (returnType != null) {
				Type[] actualTypeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
				if (actualTypeArguments.length > 0) {
					returnType = actualTypeArguments[0]; // NOSONAR
					if (returnType instanceof WildcardType) {
						// Set the return type to null so the converter will use the actual returned
						// object's class for type info
						returnType = null;
					}
				}
			}
			doHandleResult(
					new InvocationResult(deferredResult, resultArg.getSendTo(), returnType, resultArg.getBean(),
							resultArg.getMethod()),
					request, channel, source);
		}
	}

	private void basicAck(Message request, Channel channel) {
		try {
			channel.basicAck(request.getMessageProperties().getDeliveryTag(), false);
		}
		catch (IOException e) {
			this.logger.error("Failed to ack message", e);
		}
	}

	private void asyncFailure(Message request, Channel channel, Throwable t) {
		this.logger.error("Future, Mono, or suspend function was completed with an exception for " + request, t);
		try {
			channel.basicNack(request.getMessageProperties().getDeliveryTag(), false,
					ContainerUtils.shouldRequeue(this.defaultRequeueRejected, t, this.logger));
		}
		catch (IOException e) {
			this.logger.error("Failed to nack message", e);
		}
	}

	protected void doHandleResult(InvocationResult resultArg, Message request, Channel channel, Object source) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Listener method returned result [" + resultArg
					+ "] - generating response message for it");
		}
		try {
			Message response = buildMessage(channel, resultArg.getReturnValue(), resultArg.getReturnType());
			MessageProperties props = response.getMessageProperties();
			props.setTargetBean(resultArg.getBean());
			props.setTargetMethod(resultArg.getMethod());
			postProcessResponse(request, response);
			if (this.replyPostProcessor != null) {
				response = this.replyPostProcessor.apply(request, response);
			}
			Address replyTo = getReplyToAddress(request, source, resultArg);
			sendResponse(channel, replyTo, response);
		}
		catch (Exception ex) {
			throw new ReplyFailureException("Failed to send reply with payload '" + resultArg + "'", ex);
		}
	}

	protected String getReceivedExchange(Message request) {
		return request.getMessageProperties().getReceivedExchange();
	}

	/**
	 * Build a Rabbit message to be sent as response based on the given result object.
	 * @param channel the Rabbit Channel to operate on.
	 * @param result the content of the message, as returned from the listener method.
	 * @param genericType the generic type to populate type headers.
	 * @return the Rabbit <code>Message</code> (never <code>null</code>).
	 * @see #setMessageConverter
	 */
	protected Message buildMessage(Channel channel, Object result, Type genericType) {
		MessageConverter converter = getMessageConverter();
		if (converter != null && !(result instanceof Message)) {
			return convert(result, genericType, converter);
		}
		else {
			if (result instanceof Message msg) {
				return msg;
			}
			else {
				throw new MessageConversionException("No MessageConverter specified - cannot handle message ["
						+ result + "]");
			}
		}
	}

	/**
	 * Convert to a message, with reply content type based on settings.
	 * @param result the result.
	 * @param genericType the type.
	 * @param converter the converter.
	 * @return the message.
	 * @since 2.3
	 */
	protected Message convert(Object result, Type genericType, MessageConverter converter) {
		MessageProperties messageProperties = new MessageProperties();
		if (this.replyContentType != null) {
			messageProperties.setContentType(this.replyContentType);
		}
		Message message = converter.toMessage(result, messageProperties, genericType);
		if (this.replyContentType != null && !this.converterWinsContentType) {
			message.getMessageProperties().setContentType(this.replyContentType);
		}
		return message;
	}

	/**
	 * Post-process the given response message before it will be sent.
	 * <p>
	 * The default implementation sets the response's correlation id to the request message's correlation id, if any;
	 * otherwise to the request message id.
	 * @param request the original incoming Rabbit message
	 * @param response the outgoing Rabbit message about to be sent
	 */
	protected void postProcessResponse(Message request, Message response) {
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
	 * @throws org.springframework.amqp.AmqpException if no {@link Address} can be determined
	 * @see #setResponseAddress(String)
	 * @see #setResponseRoutingKey(String)
	 * @see org.springframework.amqp.core.Message#getMessageProperties()
	 * @see org.springframework.amqp.core.MessageProperties#getReplyTo()
	 */
	protected Address getReplyToAddress(Message request, Object source, InvocationResult result) {
		Address replyTo = request.getMessageProperties().getReplyToAddress();
		if (replyTo == null) {
			if (this.responseAddress == null && this.responseExchange != null) {
				this.responseAddress = new Address(this.responseExchange, this.responseRoutingKey);
			}
			if (result.getSendTo() != null) {
				replyTo = evaluateReplyTo(request, source, result.getReturnValue(), result.getSendTo());
			}
			else if (this.responseExpression != null) {
				replyTo = evaluateReplyTo(request, source, result.getReturnValue(), this.responseExpression);
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
		Address replyTo;
		Object value = expression.getValue(this.evalContext, new ReplyExpressionRoot(request, source, result));
		Assert.state(value instanceof String || value instanceof Address,
				"response expression must evaluate to a String or Address");
		if (value instanceof String sValue) {
			replyTo = new Address(sValue);
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
	 * @see #postProcessResponse(Message, Message)
	 * @see #setReplyPostProcessor(ReplyPostProcessor)
	 */
	protected void sendResponse(Channel channel, Address replyTo, Message messageIn) {
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
			if (this.retryTemplate == null) {
				doPublish(channel, replyTo, message);
			}
			else {
				final Message messageToSend = message;
				this.retryTemplate.execute(ctx -> {
					doPublish(channel, replyTo, messageToSend);
					return null;
				}, ctx -> {
					if (this.recoveryCallback != null) {
						ctx.setAttribute(SendRetryContextAccessor.MESSAGE, messageToSend);
						ctx.setAttribute(SendRetryContextAccessor.ADDRESS, replyTo);
						this.recoveryCallback.recover(ctx);
						return null;
					}
					else {
						throw RabbitExceptionTranslator.convertRabbitAccessException(ctx.getLastThrowable());
					}
				});
			}
		}
		catch (Exception ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
	}

	protected void doPublish(Channel channel, Address replyTo, Message message) throws IOException {
		channel.basicPublish(replyTo.getExchangeName(), replyTo.getRoutingKey(), this.mandatoryPublish,
				this.messagePropertiesConverter.fromMessageProperties(message.getMessageProperties(), this.encoding),
				message.getBody());
	}

	/**
	 * Post-process the given message before sending the response.
	 * <p>
	 * The default implementation is empty.
	 *
	 * @param channel The channel.
	 * @param response the outgoing Rabbit message about to be sent
	 */
	protected void postProcessChannel(Channel channel, Message response) {
	}

	/**
	 * Root object for reply expression evaluation.
	 */
	public static final class ReplyExpressionRoot {

		private final Message request;

		private final Object source;

		private final Object result;

		protected ReplyExpressionRoot(Message request, Object source, Object result) {
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
