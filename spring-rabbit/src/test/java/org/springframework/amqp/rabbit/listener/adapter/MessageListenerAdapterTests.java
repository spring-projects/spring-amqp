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

package org.springframework.amqp.rabbit.listener.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.aop.framework.ProxyFactory;

/**
 * @author Dave Syer
 * @author Greg Turnquist
 * @author Gary Russell
 *
 */
public class MessageListenerAdapterTests {

	private MessageProperties messageProperties;

	private MessageListenerAdapter adapter;

	private final SimpleService simpleService = new SimpleService();

	@Before
	public void init() {
		this.messageProperties = new MessageProperties();
		this.messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);
		this.adapter = new MessageListenerAdapter();
		this.adapter.setMessageConverter(new SimpleMessageConverter());
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
		this.adapter.setDelegate(new Delegate());
		this.adapter.onMessage(new Message("foo".getBytes(), messageProperties));
		assertTrue(called.get());
	}

	@Test
	public void testAlternateConstructor() throws Exception {
		final AtomicBoolean called = new AtomicBoolean(false);
		class Delegate {
			@SuppressWarnings("unused")
			public String myPojoMessageMethod(String input) {
				called.set(true);
				return "processed" + input;
			}
		}
		this.adapter = new MessageListenerAdapter(new Delegate(), "myPojoMessageMethod");
		this.adapter.onMessage(new Message("foo".getBytes(), messageProperties));
		assertTrue(called.get());
	}

	@Test
	public void testExplicitListenerMethod() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		this.adapter.setDelegate(this.simpleService);
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties));
		assertEquals("handle", this.simpleService.called);
	}

	@Test
	public void testMappedListenerMethod() throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("foo", "handle");
		map.put("bar", "notDefinedOnInterface");
		this.adapter.setDefaultListenerMethod("anotherHandle");
		this.adapter.setQueueOrTagToMethodName(map);
		this.adapter.setDelegate(this.simpleService);
		this.messageProperties.setConsumerQueue("foo");
		this.messageProperties.setConsumerTag("bar");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties));
		assertEquals("handle", this.simpleService.called);
		this.messageProperties.setConsumerQueue("junk");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties));
		assertEquals("notDefinedOnInterface", this.simpleService.called);
		this.messageProperties.setConsumerTag("junk");
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties));
		assertEquals("anotherHandle", this.simpleService.called);
	}

	@Test
	public void testProxyListener() throws Exception {
		this.adapter.setDefaultListenerMethod("notDefinedOnInterface");
		ProxyFactory factory = new ProxyFactory(this.simpleService);
		factory.setProxyTargetClass(true);
		this.adapter.setDelegate(factory.getProxy());
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties));
		assertEquals("notDefinedOnInterface", this.simpleService.called);
	}

	@Test
	public void testJdkProxyListener() throws Exception {
		this.adapter.setDefaultListenerMethod("handle");
		ProxyFactory factory = new ProxyFactory(this.simpleService);
		factory.setProxyTargetClass(false);
		this.adapter.setDelegate(factory.getProxy());
		this.adapter.onMessage(new Message("foo".getBytes(), this.messageProperties));
		assertEquals("handle", this.simpleService.called);
	}

	public interface Service {

		String handle(String input);

		String anotherHandle(String input);

	}

	public static class SimpleService implements Service {

		private String called;

		@Override
		public String handle(String input) {
			called = "handle";
			return "processed" + input;
		}

		@Override
		public String anotherHandle(String input) {
			called = "anotherHandle";
			return "processed" + input;
		}

		public String notDefinedOnInterface(String input) {
			called = "notDefinedOnInterface";
			return "processed" + input;
		}

	}
}
