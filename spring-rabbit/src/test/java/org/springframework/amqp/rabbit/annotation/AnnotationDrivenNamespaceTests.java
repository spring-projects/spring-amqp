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

package org.springframework.amqp.rabbit.annotation;


import org.hamcrest.core.Is;
import org.junit.Test;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class AnnotationDrivenNamespaceTests extends AbstractRabbitAnnotationDrivenTests {

	@Override
	@Test
	public void sampleConfiguration() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-sample-config.xml", getClass());
		testSampleConfiguration(context, 1);
	}

	@Override
	@Test
	public void fullConfiguration() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-full-config.xml", getClass());
		testFullConfiguration(context);
	}

	@Override
	@Test
	public void fullConfigurableConfiguration() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-full-configurable-config.xml", getClass());
		testFullConfiguration(context);
	}

	@Override
	public void noRabbitAdminConfiguration() {
		thrown.expect(BeanCreationException.class);
		thrown.expectMessage("'rabbitAdmin'");
		new ClassPathXmlApplicationContext("annotation-driven-no-rabbit-admin-config.xml", getClass()).close();
	}

	@Override
	@Test
	public void customConfiguration() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-custom-registry.xml", getClass());
		testCustomConfiguration(context);
	}

	@Override
	@Test
	public void explicitContainerFactory() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-custom-container-factory.xml", getClass());
		testExplicitContainerFactoryConfiguration(context);
	}

	@Override
	public void defaultContainerFactory() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-default-container-factory.xml", getClass());
		testDefaultContainerFactoryConfiguration(context);
	}

	@Override
	public void rabbitHandlerMethodFactoryConfiguration() throws Exception {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-custom-handler-method-factory.xml", getClass());

		thrown.expect(ListenerExecutionFailedException.class);
		thrown.expectCause(Is.<MethodArgumentNotValidException>isA(MethodArgumentNotValidException.class));
		testRabbitHandlerMethodFactoryConfiguration(context);
	}

	@Override
	@Test
	public void rabbitListeners() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-no-rabbit-admin-listeners-config.xml", getClass());
		testRabbitListenerRepeatable(context);
	}

	static class CustomRabbitListenerConfigurer implements RabbitListenerConfigurer {

		private MessageListener messageListener;

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
			endpoint.setId("myCustomEndpointId");
			endpoint.setQueueNames("myQueue");
			endpoint.setMessageListener(this.messageListener);
			registrar.registerEndpoint(endpoint);
		}

		public void setMessageListener(MessageListener messageListener) {
			this.messageListener = messageListener;
		}

	}
}
