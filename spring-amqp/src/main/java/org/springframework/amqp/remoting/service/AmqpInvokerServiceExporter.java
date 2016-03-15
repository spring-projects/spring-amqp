/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.remoting.service;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.remoting.client.AmqpProxyFactoryBean;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationBasedExporter;
import org.springframework.remoting.support.RemoteInvocationResult;

/**
 * This message listener exposes a plain java service via AMQP. Such services can be accessed via plain AMQP or via
 * {@link AmqpProxyFactoryBean}.
 *
 * To configure this message listener so that it actually receives method calls via AMQP, it needs to be put into a
 * listener container. See {@link MessageListener}.
 *
 * <p>
 * When receiving a message, a service method is called according to the contained {@link RemoteInvocation}. The result
 * of that invocation is returned as a {@link RemoteInvocationResult} contained in a message that is sent according to
 * the <code>ReplyToAddress</code> of the received message.
 *
 * <p>
 * Please note that this exporter does not use the {@link MessageConverter} of the injected {@link AmqpTemplate} to
 * convert incoming calls and their results. Instead you have to directly inject the <code>MessageConverter</code> into
 * this class.
 *
 * <p>
 * This listener responds to "Request/Reply"-style messages as described <a href=
 * "http://static.springsource.org/spring-amqp/reference/html/amqp.html#request-reply" >here</a>.
 *
 * @author David Bilge
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.2
 */
public class AmqpInvokerServiceExporter extends RemoteInvocationBasedExporter implements MessageListener {

	private AmqpTemplate amqpTemplate;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	@Override
	public void onMessage(Message message) {
		Address replyToAddress = message.getMessageProperties().getReplyToAddress();
		if (replyToAddress == null) {
			throw new AmqpRejectAndDontRequeueException("No replyToAddress in inbound AMQP Message");
		}

		Object invocationRaw = this.messageConverter.fromMessage(message);

		RemoteInvocationResult remoteInvocationResult;
		if (invocationRaw == null || !(invocationRaw instanceof RemoteInvocation)) {
			remoteInvocationResult =  new RemoteInvocationResult(
					new IllegalArgumentException("The message does not contain a RemoteInvocation payload"));
		}
		else {
			RemoteInvocation invocation = (RemoteInvocation) invocationRaw;
			remoteInvocationResult = invokeAndCreateResult(invocation, getService());
		}
		send(remoteInvocationResult, replyToAddress);
	}

	private void send(Object object, Address replyToAddress) {
		Message message = this.messageConverter.toMessage(object, new MessageProperties());

		getAmqpTemplate().send(replyToAddress.getExchangeName(), replyToAddress.getRoutingKey(), message);
	}

	public AmqpTemplate getAmqpTemplate() {
		return this.amqpTemplate;
	}

	/**
	 * The AMQP template to use for sending the return value.
	 *
	 * <p>
	 * Note that the exchange and routing key parameters on this template are ignored for these return messages. Instead
	 * of those the respective parameters from the original message's <code>returnAddress</code> are being used.
	 * <p>
	 * Also, the template's {@link MessageConverter} is not used for the reply.
	 *
	 * @param amqpTemplate The amqp template.
	 *
	 * @see #setMessageConverter(MessageConverter)
	 */
	public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
		this.amqpTemplate = amqpTemplate;
	}

	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set the message converter for this remote service. Used to deserialize remote method calls and to serialize their
	 * return values.
	 * <p>
	 * The default converter is a SimpleMessageConverter, which is able to handle byte arrays, Strings, and Serializable
	 * Objects depending on the message content type header.
	 * <p>
	 * Note that this class never uses the message converter of the underlying {@link AmqpTemplate}!
	 *
	 * @param messageConverter The message converter.
	 *
	 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

}
