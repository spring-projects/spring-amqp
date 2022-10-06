/*
 * Copyright 2014-2019 the original author or authors.
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

package org.springframework.amqp.rabbit.logback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import ch.qos.logback.classic.Logger;

/**
 * @author Artem Bilan
 * @author Nicolas Ristock
 * @author Eugene Gusev
 *
 * @since 1.4
 */
@SpringJUnitConfig(classes = AmqpAppenderConfiguration.class)
@DirtiesContext
@RabbitAvailable
@Disabled("Temporary")
public class AmqpAppenderIntegrationTests {

	/* logback will automatically find lockback-test.xml */
	private static final Logger log = (Logger) LoggerFactory.getLogger(AmqpAppenderIntegrationTests.class);

	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private Queue encodedQueue;

	@Autowired
	private Queue testQueue;

	private SimpleMessageListenerContainer listenerContainer;

	@BeforeEach
	public void setUp() {
		this.listenerContainer = this.applicationContext.getBean(SimpleMessageListenerContainer.class);
		MDC.clear();
	}

	@AfterEach
	public void tearDown() {
		MDC.clear();
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

		assertThat(testListener.getLatch().await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(testListener.getId()).isNotNull();
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

		assertThat(testListener.getLatch().await(5, TimeUnit.SECONDS)).isTrue();
		MessageProperties messageProperties = testListener.getMessageProperties();
		assertThat(messageProperties).isNotNull();
		assertThat(messageProperties.getHeaders().get(propertyName)).isNotNull();
		assertThat(messageProperties.getHeaders().get(propertyName)).isEqualTo(propertyValue);
		Object location = messageProperties.getHeaders().get("location");
		assertThat(location).isNotNull();
		assertThat(location).isInstanceOf(String.class);
		assertThat((String) location)
				.startsWith(
						"org.springframework.amqp.rabbit.logback.AmqpAppenderIntegrationTests.testAppenderWithProps()");
		Object threadName = messageProperties.getHeaders().get("thread");
		assertThat(threadName).isNotNull();
		assertThat(threadName).isInstanceOf(String.class);
		assertThat(threadName).isEqualTo(Thread.currentThread().getName());
		assertThat(messageProperties.getHeaders().get("foo")).isEqualTo("bar");
	}

	@Test
	public void testCharset() throws InterruptedException {
		TestListener testListener = (TestListener) applicationContext.getBean("testListener", 1);
		listenerContainer.setMessageListener(testListener);
		listenerContainer.start();

		String foo = "\u0fff"; // UTF-8 -> 0xe0bfbf
		log.info(foo);
		assertThat(testListener.getLatch().await(5, TimeUnit.SECONDS)).isTrue();
		byte[] body = testListener.getMessage().getBody();
		int lineSeparatorExtraBytes = System.getProperty("line.separator").getBytes().length - 1;
		assertThat(body[body.length - 5 - lineSeparatorExtraBytes] & 0xff).isEqualTo(0xe0);
		assertThat(body[body.length - 4 - lineSeparatorExtraBytes] & 0xff).isEqualTo(0xbf);
		assertThat(body[body.length - 3 - lineSeparatorExtraBytes] & 0xff).isEqualTo(0xbf);
	}

	@Test
	public void testWithEncoder() {
		this.applicationContext.getBean(SingleConnectionFactory.class).createConnection().close();
		Logger log = (Logger) LoggerFactory.getLogger("encoded");
		log.info("foo");
		log.info("bar");
		Message received = this.template.receive(this.encodedQueue.getName());
		assertThat(received).isNotNull();
		assertThat(new String(received.getBody())).contains("%d %p %t [%c] - <%m>%n");
		assertThat(received.getMessageProperties().getAppId()).isEqualTo("AmqpAppenderTest");
		assertThat(this.template.receive(this.encodedQueue.getName())).isNotNull();
		assertThat(new String(this.template.receive(this.encodedQueue.getName()).getBody()))
			.doesNotContainPattern("%d %p %t [%c] - <%m>%n");
	}

	@Test
	public void customQueueIsUsedIfProvided() throws Exception {
		this.applicationContext.getBean(SingleConnectionFactory.class).createConnection().close();
		Logger log = (Logger) LoggerFactory.getLogger("customQueue");

		String testMessage = String.valueOf(ThreadLocalRandom.current().nextLong());
		log.info(testMessage);

		BlockingQueue<AmqpAppender.Event> appenderQueue =
				((CustomQueueAppender) log.getAppender("AMQPWithCustomQueue")).mockedQueue;
		verify(appenderQueue).add(argThat(arg -> arg.getEvent().getMessage().equals(testMessage)));
	}

	@Test
	public void testAddMdcAsHeaders() {
		this.applicationContext.getBean(SingleConnectionFactory.class).createConnection().close();

		Logger logWithMdc = (Logger) LoggerFactory.getLogger("withMdc");
		Logger logWithoutMdc = (Logger) LoggerFactory.getLogger("withoutMdc");
		MDC.put("mdc1", "test1");
		MDC.put("mdc2", "test2");

		logWithMdc.info("test message with MDC in headers");
		Message received1 = this.template.receive(this.testQueue.getName());

		assertThat(received1).isNotNull();
		assertThat(new String(received1.getBody())).isEqualTo("test message with MDC in headers");
		assertThat(received1.getMessageProperties().getHeaders())
				.contains(entry("mdc1", "test1"), entry("mdc1", "test1"));

		logWithoutMdc.info("test message without MDC in headers");
		Message received2 = this.template.receive(this.testQueue.getName());

		assertThat(received2).isNotNull();
		assertThat(new String(received2.getBody())).isEqualTo("test message without MDC in headers");
		assertThat(received2.getMessageProperties().getHeaders())
				.doesNotContain(entry("mdc1", "test1"), entry("mdc1", "test1"));
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
			assertThat(clientProperties.get("foo")).isEqualTo("bar");
			assertThat(clientProperties.get("baz")).isEqualTo("qux");
			clientProperties.put("foo", this.foo.toUpperCase());
		}

	}

	public static class CustomQueueAppender extends AmqpAppender {

		private BlockingQueue<Event> mockedQueue;

		@Override
		@SuppressWarnings("unchecked")
		protected BlockingQueue<Event> createEventQueue() {
			mockedQueue = mock(BlockingQueue.class);
			return mockedQueue;
		}
	}

}
