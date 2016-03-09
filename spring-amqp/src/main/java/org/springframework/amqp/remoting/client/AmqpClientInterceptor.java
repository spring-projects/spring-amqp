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

package org.springframework.amqp.remoting.client;

import java.util.Arrays;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.remoting.service.AmqpInvokerServiceExporter;
import org.springframework.remoting.RemoteProxyFailureException;
import org.springframework.remoting.support.DefaultRemoteInvocationFactory;
import org.springframework.remoting.support.RemoteAccessor;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationFactory;
import org.springframework.remoting.support.RemoteInvocationResult;

/**
 * {@link org.aopalliance.intercept.MethodInterceptor} for accessing RMI-style AMQP services.
 *
 * @author David Bilge
 * @author Gary Russell
 * @since 1.2
 * @see AmqpInvokerServiceExporter
 * @see AmqpProxyFactoryBean
 * @see org.springframework.remoting.RemoteAccessException
 */
public class AmqpClientInterceptor extends RemoteAccessor implements MethodInterceptor {

	private AmqpTemplate amqpTemplate;

	private String routingKey = null;

	private RemoteInvocationFactory remoteInvocationFactory = new DefaultRemoteInvocationFactory();

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		RemoteInvocation remoteInvocation = getRemoteInvocationFactory().createRemoteInvocation(invocation);

		Object rawResult;
		if (getRoutingKey() == null) {
			// Use the template's default routing key
			rawResult = this.amqpTemplate.convertSendAndReceive(remoteInvocation);
		}
		else {
			rawResult = this.amqpTemplate.convertSendAndReceive(this.routingKey, remoteInvocation);
		}

		if (rawResult == null) {
			throw new RemoteProxyFailureException("No reply received from '" +
					remoteInvocation.getMethodName() +
					"' with arguments '" +
					Arrays.asList(remoteInvocation.getArguments()) +
					"' - perhaps a timeout in the template?", null);
		}
		else if (!(rawResult instanceof RemoteInvocationResult)) {
			throw new RemoteProxyFailureException("Expected a result of type "
					+ RemoteInvocationResult.class.getCanonicalName() + " but found "
					+ rawResult.getClass().getCanonicalName(), null);
		}

		RemoteInvocationResult result = (RemoteInvocationResult) rawResult;
		return result.recreate();
	}

	public AmqpTemplate getAmqpTemplate() {
		return this.amqpTemplate;
	}

	/**
	 * The AMQP template to be used for sending messages and receiving results. This class is using "Request/Reply" for
	 * sending messages as described <a href=
	 * "http://static.springsource.org/spring-amqp/reference/html/amqp.html#request-reply" >in the Spring-AMQP
	 * documentation</a>.
	 *
	 * @param amqpTemplate The amqp template.
	 */
	public void setAmqpTemplate(AmqpTemplate amqpTemplate) {
		this.amqpTemplate = amqpTemplate;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	/**
	 * The routing key to send calls to the service with. Use this to route the messages to a specific queue on the
	 * broker. If not set, the {@link AmqpTemplate}'s default routing key will be used.
	 * <p>
	 * This property is useful if you want to use the same AmqpTemplate to talk to multiple services.
	 *
	 * @param routingKey The routing key.
	 */
	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public RemoteInvocationFactory getRemoteInvocationFactory() {
		return this.remoteInvocationFactory;
	}

	/**
	 * Set the RemoteInvocationFactory to use for this accessor. Default is a {@link DefaultRemoteInvocationFactory}.
	 * <p>
	 * A custom invocation factory can add further context information to the invocation, for example user credentials.
	 *
	 * @param remoteInvocationFactory The remote invocation factory.
	 */
	public void setRemoteInvocationFactory(RemoteInvocationFactory remoteInvocationFactory) {
		this.remoteInvocationFactory = remoteInvocationFactory;
	}

}
