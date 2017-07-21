/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
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

	public static final String X_EXCEPTION_STACKTRACE = "x-exception-stacktrace";

	public static final String X_EXCEPTION_MESSAGE = "x-exception-message";

	public static final String X_ORIGINAL_EXCHANGE = "x-original-exchange";

	public static final String X_ORIGINAL_ROUTING_KEY = "x-original-routingKey";

	protected final Log logger = LogFactory.getLog(getClass());

	protected final AmqpTemplate errorTemplate;

	protected final String errorRoutingKey;

	protected final String errorExchangeName;

	private String errorRoutingKeyPrefix = "error.";

	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	public RepublishMessageRecoverer(AmqpTemplate errorTemplate) {
		this(errorTemplate, null, null);
	}

	public RepublishMessageRecoverer(AmqpTemplate errorTemplate, String errorExchange) {
		this(errorTemplate, errorExchange, null);
	}

	public RepublishMessageRecoverer(AmqpTemplate errorTemplate, String errorExchange, String errorRoutingKey) {
		Assert.notNull(errorTemplate, "'errorTemplate' cannot be null");
		this.errorTemplate = errorTemplate;
		this.errorExchangeName = errorExchange;
		this.errorRoutingKey = errorRoutingKey;
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
		Map<String, Object> headers = messageProperties.getHeaders();
		headers.put(X_EXCEPTION_STACKTRACE, getStackTraceAsString(cause));
		headers.put(X_EXCEPTION_MESSAGE, cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage());
		headers.put(X_ORIGINAL_EXCHANGE, messageProperties.getReceivedExchange());
		headers.put(X_ORIGINAL_ROUTING_KEY, messageProperties.getReceivedRoutingKey());
		Map<? extends String, ? extends Object> additionalHeaders = additionalHeaders(message, cause);
		if (additionalHeaders != null) {
			headers.putAll(additionalHeaders);
		}

		if (messageProperties.getDeliveryMode() == null) {
			messageProperties.setDeliveryMode(this.deliveryMode);
		}

		if (null != this.errorExchangeName) {
			String routingKey = this.errorRoutingKey != null ? this.errorRoutingKey : this.prefixedOriginalRoutingKey(message);
			this.errorTemplate.send(this.errorExchangeName, routingKey, message);
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("Republishing failed message to exchange " + this.errorExchangeName);
			}
		}
		else {
			final String routingKey = this.prefixedOriginalRoutingKey(message);
			this.errorTemplate.send(routingKey, message);
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("Republishing failed message to the template's default exchange with routing key " + routingKey);
			}
		}
	}

	/**
	 * Subclasses can override this method to add more headers to the republished message.
	 * @param message The failed message.
	 * @param cause The cause.
	 * @return A {@link Map} of additional headers to add.
	 */
	protected Map<? extends String, ? extends Object> additionalHeaders(Message message, Throwable cause) {
		return null;
	}

	private String prefixedOriginalRoutingKey(Message message) {
		return this.errorRoutingKeyPrefix + message.getMessageProperties().getReceivedRoutingKey();
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

}
