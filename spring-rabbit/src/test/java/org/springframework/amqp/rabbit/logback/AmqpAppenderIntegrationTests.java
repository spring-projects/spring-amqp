/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.amqp.rabbit.logback;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import ch.qos.logback.classic.Logger;

/**
 * @author Artem Bilan
 * @since 1.4
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AmqpAppenderConfiguration.class)
@DirtiesContext
public class AmqpAppenderIntegrationTests {

	/* logback will automatically find lockback-test.xml */
	private final static Logger log = (Logger) LoggerFactory.getLogger(AmqpAppenderIntegrationTests.class);

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Autowired
	private ApplicationContext applicationContext;

	private SimpleMessageListenerContainer listenerContainer;

	@Before
	public void setUp() throws Exception {
		listenerContainer = applicationContext.getBean(SimpleMessageListenerContainer.class);
	}

	@After
	public void tearDown() {
		listenerContainer.shutdown();
	}

	@Test
	public void testAppender() throws InterruptedException {
		TestListener testListener = (TestListener) applicationContext.getBean("testListener", 4);
		listenerContainer.setMessageListener(testListener);
		listenerContainer.start();

		log.debug("This is a DEBUG message");
		log.info("This is an INFO message");
		log.warn("This is a WARN message");
		log.error("This is an ERROR message", new RuntimeException("Test exception"));

		assertTrue(testListener.getLatch().await(5, TimeUnit.SECONDS));
		assertNotNull(testListener.getId());
	}

	@Test
	public void testAppenderWithProps() throws InterruptedException {
		TestListener testListener = (TestListener) applicationContext.getBean("testListener", 4);
		listenerContainer.setMessageListener(testListener);
		listenerContainer.start();

		String propertyName = "someproperty";
		String propertyValue = "property.value";
		MDC.put(propertyName, propertyValue);
		log.debug("This is a DEBUG message with properties");
		log.info("This is an INFO message with properties");
		log.warn("This is a WARN message with properties");
		log.error("This is an ERROR message with properties", new RuntimeException("Test exception"));
		MDC.remove(propertyName);

		assertTrue(testListener.getLatch().await(5, TimeUnit.SECONDS));
		MessageProperties messageProperties = testListener.getMessageProperties();
		assertNotNull(messageProperties);
		assertNotNull(messageProperties.getHeaders().get(propertyName));
		assertEquals(propertyValue, messageProperties.getHeaders().get(propertyName));
		Object location = messageProperties.getHeaders().get("location");
		assertNotNull(location);
		assertThat(location, instanceOf(String.class));
		assertThat((String) location,
				startsWith("org.springframework.amqp.rabbit.logback.AmqpAppenderIntegrationTests.testAppenderWithProps()"));
		assertEquals("bar", messageProperties.getHeaders().get("foo"));
	}

	@Test
	public void testCharset() throws InterruptedException {
		TestListener testListener = (TestListener) applicationContext.getBean("testListener", 1);
		listenerContainer.setMessageListener(testListener);
		listenerContainer.start();

		String foo = "\u0fff"; // UTF-8 -> 0xe0bfbf
		log.info(foo);
		assertTrue(testListener.getLatch().await(5, TimeUnit.SECONDS));
		byte[] body = testListener.getMessage().getBody();
		int lineSeparatorExtraBytes = System.getProperty("line.separator").getBytes().length - 1;
		assertEquals(0xe0, body[body.length - 5 - lineSeparatorExtraBytes] & 0xff);
		assertEquals(0xbf, body[body.length - 4 - lineSeparatorExtraBytes] & 0xff);
		assertEquals(0xbf, body[body.length - 3 - lineSeparatorExtraBytes] & 0xff);
	}

	public static class EnhancedAppender extends AmqpAppender {

		private String foo;

		@Override
		public Message postProcessMessageBeforeSend(Message message, Event event) {
			message.getMessageProperties().setHeader("foo", this.foo);
			return message;
		}

		public String getFoo() {
			return this.foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		@Override
		protected void updateConnectionClientProperties(Map<String, Object> clientProperties) {
			assertEquals("bar", clientProperties.get("foo"));
			assertEquals("qux", clientProperties.get("baz"));
			clientProperties.put("foo", this.foo.toUpperCase());
		}

	}

}
