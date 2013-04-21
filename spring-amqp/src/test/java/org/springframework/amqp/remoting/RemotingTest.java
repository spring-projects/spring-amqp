/*
 * Copyright 2002-2013 the original author or authors.
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

package org.springframework.amqp.remoting;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.remoting.client.AmqpProxyFactoryBean;
import org.springframework.amqp.remoting.service.AmqpInvokerServiceExporter;
import org.springframework.amqp.remoting.testhelper.AbstractAmqpTemplate;
import org.springframework.amqp.remoting.testhelper.SentSavingTemplate;
import org.springframework.amqp.remoting.testservice.GeneralException;
import org.springframework.amqp.remoting.testservice.SpecialException;
import org.springframework.amqp.remoting.testservice.TestServiceImpl;
import org.springframework.amqp.remoting.testservice.TestServiceInterface;
import org.springframework.amqp.support.converter.MessageConverter;

/**
 * @author David Bilge
 * @since 1.2
 */
public class RemotingTest {

	private TestServiceInterface riggedProxy;

	/**
	 * Set up a rig of directly wired-up proxy and service listener so that both can be tested together without needing
	 * a running rabbit.
	 */
	@Before
	public void initializeTestRig() throws Exception {
		// Set up the service
		TestServiceInterface testService = new TestServiceImpl();
		final AmqpInvokerServiceExporter serviceExporter = new AmqpInvokerServiceExporter();
		final SentSavingTemplate sentSavingTemplate = new SentSavingTemplate();
		serviceExporter.setAmqpTemplate(sentSavingTemplate);
		serviceExporter.setService(testService);
		serviceExporter.setServiceInterface(TestServiceInterface.class);

		// Set up the client
		AmqpProxyFactoryBean amqpProxyFactoryBean = new AmqpProxyFactoryBean();
		amqpProxyFactoryBean.setServiceInterface(TestServiceInterface.class);
		AmqpTemplate directForwardingTemplate = new AbstractAmqpTemplate() {
			@Override
			public Object convertSendAndReceive(Object payload) throws AmqpException {
				MessageConverter messageConverter = serviceExporter.getMessageConverter();

				Address replyTo = new Address("fakeExchange", "fakeExchangeName", "fakeRoutingKey");
				MessageProperties messageProperties = new MessageProperties();
				messageProperties.setReplyToAddress(replyTo);
				Message message = messageConverter.toMessage(payload, messageProperties);

				serviceExporter.onMessage(message);

				Message resultMessage = sentSavingTemplate.getLastMessage();
				return messageConverter.fromMessage(resultMessage);
			}
		};
		amqpProxyFactoryBean.setAmqpTemplate(directForwardingTemplate);
		amqpProxyFactoryBean.afterPropertiesSet();
		Object rawProxy = amqpProxyFactoryBean.getObject();
		riggedProxy = (TestServiceInterface) rawProxy;
	}

	@Test
	public void testEcho() {
		Assert.assertEquals("Echo Test", riggedProxy.simpleStringReturningTestMethod("Test"));
	}

	@Test(expected = RuntimeException.class)
	public void testExceptionPropagation() {
		riggedProxy.exceptionThrowingMethod();
	}

	@Test(expected = GeneralException.class)
	public void testExceptionReturningMethod() {
		riggedProxy.notReallyExceptionReturningMethod();
	}

	@Test
	public void testActuallyExceptionReturningMethod() {
		SpecialException returnedException = riggedProxy.actuallyExceptionReturningMethod();

		Assert.assertNotNull(returnedException);
		Assert.assertTrue(returnedException instanceof SpecialException);
	}
}
