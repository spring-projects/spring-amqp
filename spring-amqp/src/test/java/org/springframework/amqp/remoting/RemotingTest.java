/*
 * Copyright 2002-2019 the original author or authors.
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

package org.springframework.amqp.remoting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.remoting.testhelper.AbstractAmqpTemplate;
import org.springframework.amqp.remoting.testhelper.SentSavingTemplate;
import org.springframework.amqp.remoting.testservice.GeneralException;
import org.springframework.amqp.remoting.testservice.SpecialException;
import org.springframework.amqp.remoting.testservice.TestServiceImpl;
import org.springframework.amqp.remoting.testservice.TestServiceInterface;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.remoting.RemoteProxyFailureException;
import org.springframework.remoting.support.RemoteInvocation;

/**
 * @author David Bilge
 * @author Artem Bilan
 * @author Gary Russell
 * @since 1.2
 */
@SuppressWarnings("deprecation")
public class RemotingTest {

	private TestServiceInterface riggedProxy;

	private org.springframework.amqp.remoting.service.AmqpInvokerServiceExporter serviceExporter;

	/**
	 * Set up a rig of directly wired-up proxy and service listener so that both can be tested together without needing
	 * a running rabbit.
	 */
	@BeforeEach
	public void initializeTestRig() {
		// Set up the service
		TestServiceInterface testService = new TestServiceImpl();
		this.serviceExporter = new org.springframework.amqp.remoting.service.AmqpInvokerServiceExporter();
		final SentSavingTemplate sentSavingTemplate = new SentSavingTemplate();
		this.serviceExporter.setAmqpTemplate(sentSavingTemplate);
		this.serviceExporter.setService(testService);
		this.serviceExporter.setServiceInterface(TestServiceInterface.class);

		// Set up the client
		org.springframework.amqp.remoting.client.AmqpProxyFactoryBean amqpProxyFactoryBean =
				new org.springframework.amqp.remoting.client.AmqpProxyFactoryBean();
		amqpProxyFactoryBean.setServiceInterface(TestServiceInterface.class);
		AmqpTemplate directForwardingTemplate = new AbstractAmqpTemplate() {
			@Override
			public Object convertSendAndReceive(Object payload) throws AmqpException {
				Object[] arguments = ((RemoteInvocation) payload).getArguments();
				if (arguments.length == 1 && arguments[0].equals("timeout")) {
					return null;
				}

				MessageConverter messageConverter = serviceExporter.getMessageConverter();

				Address replyTo = new Address("fakeExchangeName", "fakeRoutingKey");
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
		assertThat(riggedProxy.simpleStringReturningTestMethod("Test")).isEqualTo("Echo Test");
	}

	@Test
	public void testSimulatedTimeout() {
		try {
			this.riggedProxy.simulatedTimeoutMethod("timeout");
		}
		catch (RemoteProxyFailureException e) {
			assertThat(e.getMessage()).contains("'simulatedTimeoutMethod' with arguments '[timeout]'");
		}
	}

	@Test
	public void testExceptionPropagation() {
		assertThatExceptionOfType(AmqpException.class).isThrownBy(() -> riggedProxy.exceptionThrowingMethod());
	}

	@Test
	public void testExceptionReturningMethod() {
		assertThatExceptionOfType(GeneralException.class)
				.isThrownBy(() -> riggedProxy.notReallyExceptionReturningMethod());
	}

	@Test
	public void testActuallyExceptionReturningMethod() {
		SpecialException returnedException = riggedProxy.actuallyExceptionReturningMethod();
		assertThat(returnedException).isNotNull();
	}

	@Test
	public void testWrongRemoteInvocationArgument() {
		MessageConverter messageConverter = this.serviceExporter.getMessageConverter();
		this.serviceExporter.setMessageConverter(new SimpleMessageConverter() {

			private final AtomicBoolean invoked = new AtomicBoolean();

			@Override
			protected Message createMessage(Object object, MessageProperties messageProperties)
					throws MessageConversionException {
				Message message = super.createMessage(object, messageProperties);
				if (!invoked.getAndSet(true)) {
					messageProperties.setContentType(null);
				}
				return message;
			}

		});

		try {
			riggedProxy.simpleStringReturningTestMethod("Test");
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(IllegalArgumentException.class);
			assertThat(e.getMessage()).contains("The message does not contain a RemoteInvocation payload");
		}

		this.serviceExporter.setMessageConverter(messageConverter);
	}

}
