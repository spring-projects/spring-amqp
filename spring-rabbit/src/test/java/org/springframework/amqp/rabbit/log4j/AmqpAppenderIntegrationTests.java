/*
 * Copyright (c) 2011-2014 by the original author(s).
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

package org.springframework.amqp.rabbit.log4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Log4jConfigurer;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "org.springframework.amqp.rabbit.log4j" }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext
public class AmqpAppenderIntegrationTests {

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Autowired
	private ApplicationContext applicationContext;

	private Logger log;

	private SimpleMessageListenerContainer listenerContainer;

	@Before
	public void setUp() throws Exception {
		Log4jConfigurer.initLogging("classpath:log4j-amqp.properties");
		log = Logger.getLogger(getClass());
		listenerContainer = applicationContext.getBean(SimpleMessageListenerContainer.class);
	}

	@After
	public void tearDown() {
		listenerContainer.shutdown();
	}

	@AfterClass
	public static void reset() throws Exception {
		Log4jConfigurer.initLogging("classpath:log4j.properties");
	}

	@Test
	public void testAppender() throws InterruptedException {
		TestListener testListener = (TestListener) applicationContext.getBean("testListener", 4);
		listenerContainer.setMessageListener(testListener);
		listenerContainer.start();

		AmqpAppender appender = (AmqpAppender) log.getParent().getAllAppenders().nextElement();
		assertFalse(appender.isDurable());
		assertEquals(MessageDeliveryMode.NON_PERSISTENT.toString(), appender.getDeliveryMode());

		Logger log = Logger.getLogger(getClass());

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
		assertEquals("bar", messageProperties.getHeaders().get("foo"));
	}

	@Test
	public void testCharset() throws InterruptedException {
		Logger packageLogger = Logger.getLogger("org.springframework.amqp.rabbit.log4j");
		AmqpAppender appender = (AmqpAppender) packageLogger.getAppender("amqp");
		assertEquals("UTF-8", appender.getCharset());

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

	@Test
	public void testIgnoresThrowableWithCustomLayout() throws Exception {
		Logger customLayoutLogger = Logger.getLogger("org.springframework.amqp.rabbit.logging.customLayout");

		TestListener testListener = (TestListener) applicationContext.getBean("testListener", 1);
		listenerContainer.setMessageListener(testListener);
		listenerContainer.start();

		customLayoutLogger.error("This is an ERROR message", new RuntimeException("Test exception"));
		assertTrue(testListener.getLatch().await(5, TimeUnit.SECONDS));
		assertNotNull(testListener.getId());

		//This code parses an XML and ends up with exception without the general fix for AMQP-363
		DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(testListener.getMessage().getBody()));
		listenerContainer.destroy();
	}


	/*
	 * When running as main(); should shutdown cleanly.
	 */
	public static void main(String[] args) throws Exception {
		Log4jConfigurer.initLogging("classpath:log4j-amqp.properties");
		Log logger = LogFactory.getLog(AmqpAppenderIntegrationTests.class);
		logger.info("foo");
		Thread.sleep(1000);
		Log4jConfigurer.shutdownLogging();
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

	}

}
