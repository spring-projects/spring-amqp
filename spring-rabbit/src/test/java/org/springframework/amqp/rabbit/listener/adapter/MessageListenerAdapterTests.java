/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.amqp.rabbit.listener.adapter;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.aop.framework.ProxyFactory;

/**
 * @author Dave Syer
 *
 */
public class MessageListenerAdapterTests {

	private MessageProperties messageProperties;
	private MessageListenerAdapter adapter;

	@Before
	public void init() {
		SimpleService.called = false;
		messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		adapter = new MessageListenerAdapter() {
			protected void handleListenerException(Throwable ex) {
				if (ex instanceof RuntimeException) {
					throw (RuntimeException) ex;
				}
				throw new IllegalStateException(ex);
			};
		};
		adapter.setMessageConverter(new SimpleMessageConverter());
	}

	@Test
	public void testDefaultListenerMethod() throws Exception {
		final AtomicBoolean called = new AtomicBoolean(false);
		class Delegate {
			@SuppressWarnings("unused")
			public String handleMessage(String input) {
				called.set(true);
				return "processed" + input;
			}
		}
		adapter.setDelegate(new Delegate());
		adapter.onMessage(new Message("foo".getBytes(), messageProperties));
		assertTrue(called.get());
	}

	@Test
	public void testExplicitListenerMethod() throws Exception {
		adapter.setDefaultListenerMethod("handle");
		adapter.setDelegate(new SimpleService());
		adapter.onMessage(new Message("foo".getBytes(), messageProperties));
		assertTrue(SimpleService.called);
	}

	@Test
	public void testProxyListener() throws Exception {
		adapter.setDefaultListenerMethod("notDefinedOnInterface");
		ProxyFactory factory = new ProxyFactory(new SimpleService());
		factory.setProxyTargetClass(true);
		adapter.setDelegate(factory.getProxy());
		adapter.onMessage(new Message("foo".getBytes(), messageProperties));
		assertTrue(SimpleService.called);
	}

	@Test
	public void testJdkProxyListener() throws Exception {
		adapter.setDefaultListenerMethod("handle");
		ProxyFactory factory = new ProxyFactory(new SimpleService());
		factory.setProxyTargetClass(false);
		adapter.setDelegate(factory.getProxy());
		adapter.onMessage(new Message("foo".getBytes(), messageProperties));
		assertTrue(SimpleService.called);
	}

	public static interface Service {
		String handle(String input);
	}

	public static class SimpleService implements Service {

		private static boolean called;

		public String handle(String input) {
			called = true;
			return "processed" + input;
		}

		public String notDefinedOnInterface(String input) {
			called = true;
			return "processed" + input;
		}

	}
}
