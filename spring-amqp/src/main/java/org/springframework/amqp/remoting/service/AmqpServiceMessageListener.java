/*
 * Copyright 2002-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.remoting.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.remoting.client.AmqpProxyFactoryBean;
import org.springframework.amqp.remoting.common.Constants;
import org.springframework.amqp.remoting.common.MethodHeaderNamingStrategy;
import org.springframework.amqp.remoting.common.SimpleHeaderNamingStrategy;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.remoting.support.RemoteExporter;

/**
 * This message listener exposes a plain java service via AMQP. Such services can be accessed via plain AMQP or via
 * {@link AmqpProxyFactoryBean}.
 * 
 * To configure this message listener so that it actually receives method calls via AMQP, it needs to be put into a
 * listener container. That could be spring-rabbit's {@link SimpleMessageConverter}, for example (see below for an
 * example configuration).
 * 
 * <p>
 * When receiving a message, a service method is called - which is determined by the
 * {@link Constants#INVOKED_METHOD_HEADER_NAME} header. An exception thrown by the invoked method is serialized and
 * returned to the client as is a regular method return value.
 * 
 * <p>
 * This listener responds to "Request/Reply"-style messages as described <a href=
 * "http://static.springsource.org/spring-amqp/reference/html/amqp.html#request-reply" >here</a>.
 * 
 * <p>
 * You could use an (xml) configuration like this:
 * 
 * <pre>
 * &lt;bean id="amqpMessageListener" class="org.springframework.amqp.remoting.service.AmqpServiceMessageListener"&gt;
 *    &lt;property name="amqpTemplate" ref="amqpTemplate" /&gt;
 *    &lt;property name="service" ref="xyzService" /&gt;
 *    &lt;property name="serviceInterface" value="x.y.z.ServiceInterface" /&gt;
 * &lt;/bean&gt;
 * &lt;rabbit:queue name="ServiceQueue" /&gt;
 * &lt;rabbit:listener-container&gt;
 *   &lt;rabbit:listener ref="amqpMessageListener" queues="ServiceQueue" /&gt;
 * &lt;/rabbit:listener-container&gt;
 * </pre>
 * 
 * @author David Bilge
 * @since 13.04.2013
 */
public class AmqpServiceMessageListener extends RemoteExporter implements MessageListener, InitializingBean {

	private AmqpTemplate amqpTemplate;

	private MethodHeaderNamingStrategy methodHeaderNamingStrategy = new SimpleHeaderNamingStrategy();

	private Map<String, Method> methodCache = new HashMap<String, Method>();

	private MessageConverter messageConverter = new SimpleMessageConverter();

	@Override
	public void afterPropertiesSet() throws Exception {
		Method[] methods = getService().getClass().getMethods();
		for (Method method : methods) {
			methodCache.put(getMethodHeaderNamingStrategy().generateMethodName(method), method);
		}
	}

	@Override
	public void onMessage(Message message) {
		Address replyToAddress = message.getMessageProperties().getReplyToAddress();

		Map<String, Object> headers = message.getMessageProperties().getHeaders();
		Object invokedMethodRaw = headers.get(Constants.INVOKED_METHOD_HEADER_NAME);
		if (invokedMethodRaw == null || !(invokedMethodRaw instanceof String)) {
			send(new RuntimeException("The 'invoked method' header is missing (expected name '"
					+ Constants.INVOKED_METHOD_HEADER_NAME + "')"), replyToAddress);
			return;
		}

		String invokedMethodName = (String) invokedMethodRaw;
		Method invokedMethod = methodCache.get(invokedMethodName);
		if (invokedMethod == null) {
			send(new RuntimeException("The invoked method does not exist on the service (Method: '" + invokedMethodName
					+ "')"), replyToAddress);
			return;
		}

		Object argumentsRaw = messageConverter.fromMessage(message);
		Object[] arguments;
		if (argumentsRaw instanceof Object[]) {
			arguments = (Object[]) argumentsRaw;
		} else {
			send(new RuntimeException("The message does not contain an argument array"), replyToAddress);
			return;
		}

		Object retVal;
		try {
			retVal = invokedMethod.invoke(getService(), arguments);
		} catch (InvocationTargetException ite) {
			send(ite.getCause(), replyToAddress);
			return;
		} catch (Throwable e) {
			send(e, replyToAddress);
			return;
		}

		send(retVal, replyToAddress);
	}

	private void send(Object o, Address replyToAddress) {
		Message m = messageConverter.toMessage(o, new MessageProperties());

		getAmqpTemplate().send(replyToAddress.getExchangeName(), replyToAddress.getRoutingKey(), m);
	}

	public AmqpTemplate getAmqpTemplate() {
		return amqpTemplate;
	}

	/**
	 * The AMQP template to use for sending the return value.
	 * 
	 * <p>
	 * Note that the exchange and routing key parameters on this template are ignored for these return messages. Instead
	 * of those the respective parameters from the original message's <code>returnAddress</code> are being used.
	 */
	public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
		this.amqpTemplate = amqpTemplate;
	}

	public MessageConverter getMessageConverter() {
		return messageConverter;
	}

	/**
	 * Set the message converter for this remote service. Used to deserialize arguments to called methods and to
	 * serialize their return values.
	 * <p>
	 * The default converter is a SimpleMessageConverter, which is able to handle byte arrays, Strings, and Serializable
	 * Objects depending on the message content type header.
	 * 
	 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	public MethodHeaderNamingStrategy getMethodHeaderNamingStrategy() {
		return methodHeaderNamingStrategy;
	}

	/**
	 * A strategy to name methods in the message header for lookup in the service. Make sure to use the same strategy on
	 * the client side.
	 */
	public void setMethodHeaderNamingStrategy(MethodHeaderNamingStrategy methodHeaderNamingStrategy) {
		this.methodHeaderNamingStrategy = methodHeaderNamingStrategy;
	}

}
