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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.util.Assert;

/**
 * MessageRecoverer implementation that republishes recovered messages
 * to a specified exchange with the exception stacktrace stored in the
 * message header x-exception.
 * <p/>
 * If no exchange is specified for the errorExchangeName then the message will
 * simply be published to the default exchange with "error." prepended
 * to the original routing key.
 *
 * @author James Carr
 * @author Gary Russell
 */
public class RepublishMessageRecoverer implements MessageRecoverer {

	private static final Log logger = LogFactory.getLog(RepublishMessageRecoverer.class);

	private final AmqpTemplate errorTemplate;

	private final String errorRoutingKey;

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

	@Override
	public void recover(Message message, Throwable cause) {
		Map<String, Object> headers = message.getMessageProperties().getHeaders();
		headers.put("x-exception-stacktrace", getStackTraceAsString(cause));
		headers.put("x-exception-message", cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage());
		headers.put("x-original-exchange", message.getMessageProperties().getReceivedExchange());
		headers.put("x-original-routingKey", message.getMessageProperties().getReceivedRoutingKey());

		if (null != errorExchangeName) {
			String routingKey = errorRoutingKey != null ? errorRoutingKey : message.getMessageProperties().getReceivedRoutingKey();
			this.errorTemplate.send(errorExchangeName, routingKey, message);
			if (logger.isWarnEnabled()) {
				logger.warn("Republishing failed message to exchange " + errorExchangeName);
			}
		}
		else {
			final String routingKey = "error." + message.getMessageProperties().getReceivedRoutingKey();
			this.errorTemplate.send(routingKey, message);
			if (logger.isWarnEnabled()) {
				logger.warn("Republishing failed message to the template's default exchange with routing key " + routingKey);
			}
		}
	}

	private String getStackTraceAsString(Throwable cause) {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		cause.printStackTrace(new PrintStream(byteArrayOutputStream));
		String exceptionAsString = byteArrayOutputStream.toString();
		return exceptionAsString;
	}

}
