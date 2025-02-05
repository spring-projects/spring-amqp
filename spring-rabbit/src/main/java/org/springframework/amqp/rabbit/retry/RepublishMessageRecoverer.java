/*
 * Copyright 2014-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.ValueExpression;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * {@link MessageRecoverer} implementation that republishes recovered messages
 * to a specified exchange with the exception stack trace stored in the
 * message header x-exception.
 * <p>
 * If no routing key is provided, the original routing key for the message,
 * prefixed with {@link #setErrorRoutingKeyPrefix(String)} (default "error.")
 * will be used to publish the message to the exchange provided in
 * name, or the template's default exchange if none is set.
 *
 * @author James Carr
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 */
public class RepublishMessageRecoverer implements MessageRecoverer {

	private static final int ELLIPSIS_LENGTH = 3;

	public static final String X_EXCEPTION_STACKTRACE = "x-exception-stacktrace";

	public static final String X_EXCEPTION_MESSAGE = "x-exception-message";

	public static final String X_ORIGINAL_EXCHANGE = "x-original-exchange";

	public static final String X_ORIGINAL_ROUTING_KEY = "x-original-routingKey";

	public static final int DEFAULT_FRAME_MAX_HEADROOM = 20_000;

	private static final int MAX_EXCEPTION_MESSAGE_SIZE_IN_TRACE = 100 - ELLIPSIS_LENGTH;

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	protected final AmqpTemplate errorTemplate; // NOSONAR

	protected final Expression errorRoutingKeyExpression;

	protected final Expression errorExchangeNameExpression;

	protected final EvaluationContext evaluationContext = new StandardEvaluationContext();

	private String errorRoutingKeyPrefix = "error.";

	private int frameMaxHeadroom = DEFAULT_FRAME_MAX_HEADROOM;

	private volatile Integer maxStackTraceLength = -1;

	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	/**
	 * Create an instance with the provided template.
	 * @param errorTemplate the template.
	 */
	public RepublishMessageRecoverer(AmqpTemplate errorTemplate) {
		this(errorTemplate, null, (String) null);
	}

	/**
	 * Create an instance with the provided properties.
	 * @param errorTemplate the template.
	 * @param errorExchange the exchange.
	 */
	public RepublishMessageRecoverer(AmqpTemplate errorTemplate, String errorExchange) {
		this(errorTemplate, errorExchange, null);
	}

	/**
	 * Create an instance with the provided properties. If the exchange or routing key is null,
	 * the template's default will be used.
	 * @param errorTemplate the template.
	 * @param errorExchange the exchange.
	 * @param errorRoutingKey the routing key.
	 */
	public RepublishMessageRecoverer(AmqpTemplate errorTemplate, @Nullable String errorExchange,
			@Nullable String errorRoutingKey) {

		this(errorTemplate, new ValueExpression<>(errorExchange), new ValueExpression<>(errorRoutingKey));
	}

	/**
	 * Create an instance with the provided properties. If the exchange or routing key
	 * evaluate to null, the template's default will be used.
	 * @param errorTemplate the template.
	 * @param errorExchange the exchange expression, evaluated against the message.
	 * @param errorRoutingKey the routing key, evaluated against the message.
	 */
	public RepublishMessageRecoverer(AmqpTemplate errorTemplate, @Nullable Expression errorExchange,
			@Nullable Expression errorRoutingKey) {

		Assert.notNull(errorTemplate, "'errorTemplate' cannot be null");
		this.errorTemplate = errorTemplate;
		this.errorExchangeNameExpression = errorExchange != null ? errorExchange : new ValueExpression<>(null);
		this.errorRoutingKeyExpression = errorRoutingKey != null ? errorRoutingKey : new ValueExpression<>(null);
		if (!(this.errorTemplate instanceof RabbitTemplate)) {
			this.maxStackTraceLength = Integer.MAX_VALUE;
		}
	}

	/**
	 * Apply a prefix to the outbound routing key, which will be prefixed to the original message
	 * routing key (if no explicit routing key was provided in the constructor; ignored otherwise.
	 * Use an empty string ("") for no prefixing.
	 * @param errorRoutingKeyPrefix The prefix (default "error.").
	 * @return this.
	 */
	public RepublishMessageRecoverer errorRoutingKeyPrefix(String errorRoutingKeyPrefix) {
		this.setErrorRoutingKeyPrefix(errorRoutingKeyPrefix);
		return this;
	}

	/**
	 * Set the amount by which the negotiated frame_max is to be reduced when considering
	 * truncating the stack trace header. Defaults to
	 * {@value #DEFAULT_FRAME_MAX_HEADROOM}.
	 * @param headroom the headroom
	 * @return this.
	 * @since 2.0.5
	 */
	public RepublishMessageRecoverer frameMaxHeadroom(int headroom) {
		this.frameMaxHeadroom = headroom;
		return this;
	}

	/**
	 * @param errorRoutingKeyPrefix The prefix (default "error.").
	 * @see #errorRoutingKeyPrefix(String)
	 */
	public void setErrorRoutingKeyPrefix(String errorRoutingKeyPrefix) {
		Assert.notNull(errorRoutingKeyPrefix, "'errorRoutingKeyPrefix' cannot be null");
		this.errorRoutingKeyPrefix = errorRoutingKeyPrefix;
	}

	protected String getErrorRoutingKeyPrefix() {
		return this.errorRoutingKeyPrefix;
	}

	/**
	 * Specify a {@link MessageDeliveryMode} to set into the message to republish
	 * if the message doesn't have it already.
	 * @param deliveryMode the delivery mode to set to message.
	 * @since 2.0
	 */
	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		Assert.notNull(deliveryMode, "'deliveryMode' cannot be null");
		this.deliveryMode = deliveryMode;
	}

	protected MessageDeliveryMode getDeliveryMode() {
		return this.deliveryMode;
	}

	@Override
	public void recover(Message message, Throwable cause) {
		MessageProperties messageProperties = message.getMessageProperties();
		Map<String, @Nullable Object> headers = messageProperties.getHeaders();
		String exceptionMessage = cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage();
		@Nullable String[] processed = processStackTrace(cause, exceptionMessage);
		String stackTraceAsString = processed[0];
		String truncatedExceptionMessage = processed[1];
		if (truncatedExceptionMessage != null) {
			exceptionMessage = truncatedExceptionMessage;
		}
		headers.put(X_EXCEPTION_STACKTRACE, stackTraceAsString);
		headers.put(X_EXCEPTION_MESSAGE, exceptionMessage);
		headers.put(X_ORIGINAL_EXCHANGE, messageProperties.getReceivedExchange());
		headers.put(X_ORIGINAL_ROUTING_KEY, messageProperties.getReceivedRoutingKey());
		Map<? extends String, ?> additionalHeaders = additionalHeaders(message, cause);
		if (additionalHeaders != null) {
			headers.putAll(additionalHeaders);
		}

		if (messageProperties.getDeliveryMode() == null) {
			messageProperties.setDeliveryMode(this.deliveryMode);
		}

		String exchangeName = this.errorExchangeNameExpression.getValue(this.evaluationContext, message, String.class);
		String rk = this.errorRoutingKeyExpression.getValue(this.evaluationContext, message, String.class);
		String routingKey = rk != null ? rk : this.prefixedOriginalRoutingKey(message);
		if (null != exchangeName) {
			doSend(exchangeName, routingKey, message);
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("Republishing failed message to exchange '" + exchangeName
						+ "' with routing key " + routingKey);
			}
		}
		else {
			doSend(null, routingKey, message);
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("Republishing failed message to the template's default exchange with routing key "
						+ routingKey);
			}
		}
	}

	/**
	 * Send the message.
	 * @param exchange the exchange or null to use the template's default.
	 * @param routingKey the routing key.
	 * @param message the message.
	 * @since 2.3.3
	 */
	protected void doSend(@Nullable String exchange, String routingKey, Message message) {
		if (exchange != null) {
			this.errorTemplate.send(exchange, routingKey, message);
		}
		else {
			this.errorTemplate.send(routingKey, message);
		}
	}

	private @Nullable String[] processStackTrace(Throwable cause, @Nullable String exceptionMessage) {
		String stackTraceAsString = getStackTraceAsString(cause);
		if (this.maxStackTraceLength < 0) {
			int maxStackTraceLen = RabbitUtils
					.getMaxFrame(((RabbitTemplate) this.errorTemplate).getConnectionFactory());
			if (maxStackTraceLen > 0) {
				maxStackTraceLen -= this.frameMaxHeadroom;
				this.maxStackTraceLength = maxStackTraceLen;
			}
		}
		return truncateIfNecessary(cause, exceptionMessage, stackTraceAsString);
	}

	private @Nullable String[] truncateIfNecessary(Throwable cause, @Nullable String exception, String stackTrace) {
		boolean truncated = false;
		String stackTraceAsString = stackTrace;
		String exceptionMessage = exception == null ? "" : exception;
		String truncatedExceptionMessage = exceptionMessage.length() <= MAX_EXCEPTION_MESSAGE_SIZE_IN_TRACE
				? exceptionMessage
				: (exceptionMessage.substring(0, MAX_EXCEPTION_MESSAGE_SIZE_IN_TRACE) + "...");
		if (this.maxStackTraceLength > 0 &&
				stackTraceAsString.length() + exceptionMessage.length() > this.maxStackTraceLength) {

			if (!exceptionMessage.equals(truncatedExceptionMessage)) {
				int start = stackTraceAsString.indexOf(exceptionMessage);
				stackTraceAsString = stackTraceAsString.substring(0, start)
						+ truncatedExceptionMessage
						+ stackTraceAsString.substring(start + exceptionMessage.length());
			}
			int adjustedStackTraceLen = this.maxStackTraceLength - truncatedExceptionMessage.length();
			if (adjustedStackTraceLen > 0) {
				if (stackTraceAsString.length() > adjustedStackTraceLen) {
					stackTraceAsString = stackTraceAsString.substring(0, adjustedStackTraceLen);
					this.logger.warn("Stack trace in republished message header truncated due to frame_max "
							+ "limitations; "
							+ "consider increasing frame_max on the broker or reduce the stack trace depth", cause);
					truncated = true;
				}
				else if (stackTraceAsString.length() + exceptionMessage.length() > this.maxStackTraceLength) {
					this.logger.warn("Exception message in republished message header truncated due to frame_max "
							+ "limitations; consider increasing frame_max on the broker or reduce the exception "
							+ "message size", cause);
					truncatedExceptionMessage = exceptionMessage.substring(0,
							this.maxStackTraceLength - stackTraceAsString.length() - ELLIPSIS_LENGTH) + "...";
					truncated = true;
				}
			}
		}
		return new @Nullable String[] {stackTraceAsString, truncated ? truncatedExceptionMessage : null};
	}

	/**
	 * Subclasses can override this method to add more headers to the republished message.
	 * @param message The failed message.
	 * @param cause The cause.
	 * @return A {@link Map} of additional headers to add.
	 */
	protected @Nullable Map<? extends String, ?> additionalHeaders(Message message, Throwable cause) {
		return null;
	}

	/**
	 * The default behavior of this method is to append the received routing key to the
	 * {@link #setErrorRoutingKeyPrefix(String) routingKeyPrefix}. This is only invoked
	 * if the routing key is null.
	 * @param message the message.
	 * @return the routing key.
	 */
	protected String prefixedOriginalRoutingKey(Message message) {
		return this.errorRoutingKeyPrefix + message.getMessageProperties().getReceivedRoutingKey();
	}

	/**
	 * Create a String representation of the stack trace.
	 * @param cause the throwable.
	 * @return the String.
	 * @since 2.4.8
	 */
	protected String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

}
