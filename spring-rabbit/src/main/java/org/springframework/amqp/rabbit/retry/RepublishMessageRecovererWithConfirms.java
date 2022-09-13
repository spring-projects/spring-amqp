/*
 * Copyright 2021 the original author or authors.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.CorrelationData.Confirm;
import org.springframework.amqp.rabbit.core.AmqpNackReceivedException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.lang.Nullable;

/**
 * A {@link RepublishMessageRecoverer} supporting publisher confirms and returns.
 *
 * @author Gary Russell
 * @since 2.3.3
 *
 */
public class RepublishMessageRecovererWithConfirms extends RepublishMessageRecoverer {

	private static final long DEFAULT_TIMEOUT = 10_000;

	private final RabbitTemplate template;

	private final ConfirmType confirmType;

	private long confirmTimeout = DEFAULT_TIMEOUT;

	/**
	 * Use the supplied template to publish the messsage with the provided confirm type.
	 * The template and its connection factory must be suitably configured to support the
	 * confirm type.
	 * @param errorTemplate the template.
	 * @param confirmType the confirmType.
	 */
	public RepublishMessageRecovererWithConfirms(RabbitTemplate errorTemplate, ConfirmType confirmType) {
		this(errorTemplate, null, null, confirmType);
	}

	/**
	 * Use the supplied template to publish the messsage with the provided confirm type to
	 * the provided exchange with the default routing key. The template and its connection
	 * factory must be suitably configured to support the confirm type.
	 * @param errorTemplate the template.
	 * @param confirmType the confirmType.
	 * @param errorExchange the exchange.
	 */
	public RepublishMessageRecovererWithConfirms(RabbitTemplate errorTemplate, String errorExchange,
			ConfirmType confirmType) {

		this(errorTemplate, errorExchange, null, confirmType);
	}

	/**
	 * Use the supplied template to publish the messsage with the provided confirm type to
	 * the provided exchange with the provided routing key. The template and its
	 * connection factory must be suitably configured to support the confirm type.
	 * @param errorTemplate the template.
	 * @param confirmType the confirmType.
	 * @param errorExchange the exchange.
	 * @param errorRoutingKey the routing key.
	 */
	public RepublishMessageRecovererWithConfirms(RabbitTemplate errorTemplate, String errorExchange,
			String errorRoutingKey, ConfirmType confirmType) {

		super(errorTemplate, errorExchange, errorRoutingKey);
		this.template = errorTemplate;
		this.confirmType = confirmType;
	}

	/**
	 * Set the confirm timeout; default 10 seconds.
	 * @param confirmTimeout the timeout.
	 */
	public void setConfirmTimeout(long confirmTimeout) {
		this.confirmTimeout = confirmTimeout;
	}

	@Override
	protected void doSend(@Nullable
	String exchange, String routingKey, Message message) {
		if (ConfirmType.CORRELATED.equals(this.confirmType)) {
			doSendCorrelated(exchange, routingKey, message);
		}
		else {
			doSendSimple(exchange, routingKey, message);
		}
	}

	private void doSendCorrelated(String exchange, String routingKey, Message message) {
		CorrelationData cd = new CorrelationData();
		if (exchange != null) {
			this.template.send(exchange, routingKey, message, cd);
		}
		else {
			this.template.send(routingKey, message, cd);
		}
		try {
			Confirm confirm = cd.getFuture().get(this.confirmTimeout, TimeUnit.MILLISECONDS);
			if (cd.getReturned() != null) {
				throw new AmqpMessageReturnedException("Message returned", cd.getReturned());
			}
			if (!confirm.isAck()) {
				throw new AmqpNackReceivedException("Negative acknowledgment received", message);
			}
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
		catch (ExecutionException ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex.getCause()); // NOSONAR (stack trace)
		}
		catch (TimeoutException ex) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
		}
	}

	private void doSendSimple(String exchange, String routingKey, Message message) {
		this.template.invoke(sender -> {
			if (exchange != null) {
				sender.send(exchange, routingKey, message);
			}
			else {
				sender.send(routingKey, message);
			}
			sender.waitForConfirmsOrDie(this.confirmTimeout);
			return null;
		});
	}

}
