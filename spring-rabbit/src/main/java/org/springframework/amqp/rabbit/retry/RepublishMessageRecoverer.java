/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
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
 * @since 1.3
 */
public class RepublishMessageRecoverer implements MessageRecoverer {

	private static final Log logger = LogFactory.getLog(RepublishMessageRecoverer.class);

	private final AmqpTemplate errorTemplate;

	private final String errorRoutingKey;

	private volatile String errorRoutingKeyPrefix = "error.";

	private final String errorExchangeName;

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
	 * @see #errorRoutingKeyPrefix(String)
	 * @param errorRoutingKeyPrefix The prefix (default "error.").
	 */
	public void setErrorRoutingKeyPrefix(String errorRoutingKeyPrefix) {
		Assert.notNull(errorRoutingKeyPrefix, "'errorRoutingKeyPrefix' cannot be null");
		this.errorRoutingKeyPrefix = errorRoutingKeyPrefix;
	}

	@Override
	public void recover(Message message, Throwable cause) {
		Map<String, Object> headers = message.getMessageProperties().getHeaders();
		headers.put("x-exception-stacktrace", getStackTraceAsString(cause));
		headers.put("x-exception-message", cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage());
		headers.put("x-original-exchange", message.getMessageProperties().getReceivedExchange());
		headers.put("x-original-routingKey", message.getMessageProperties().getReceivedRoutingKey());

		if (null != errorExchangeName) {
			String routingKey = errorRoutingKey != null ? errorRoutingKey : this.prefixedOriginalRoutingKey(message);
			this.errorTemplate.send(errorExchangeName, routingKey, message);
			if (logger.isWarnEnabled()) {
				logger.warn("Republishing failed message to exchange " + errorExchangeName);
			}
		}
		else {
			final String routingKey = this.prefixedOriginalRoutingKey(message);
			this.errorTemplate.send(routingKey, message);
			if (logger.isWarnEnabled()) {
				logger.warn("Republishing failed message to the template's default exchange with routing key " + routingKey);
			}
		}
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
