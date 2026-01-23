/*
 * Copyright 2020-present the original author or authors.
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

package org.springframework.amqp.rabbit.log4j2;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Francesco Scipioni
 *
 * @since 2.2.4
 */
@RabbitAvailable
public class ExtendAmqpAppenderTests {

	private static final LoggerContext LOGGER_CONTEXT = (LoggerContext) LogManager.getContext(false);

	private static final URI ORIGINAL_LOGGER_CONFIG = LOGGER_CONTEXT.getConfigLocation();

	@BeforeAll
	public static void setup() throws IOException {
		LOGGER_CONTEXT.setConfigLocation(new ClassPathResource("log4j2-extend-amqp-appender.xml").getURI());
		LOGGER_CONTEXT.reconfigure();
	}

	@AfterAll
	public static void teardown() {
		LOGGER_CONTEXT.setConfigLocation(ORIGINAL_LOGGER_CONFIG);
		LOGGER_CONTEXT.reconfigure();
		RabbitAvailableCondition.getBrokerRunning().deleteQueues("log4jTest", "log4j2Test");
		RabbitAvailableCondition.getBrokerRunning().deleteExchanges("log4j2Test", "log4j2Test_uri");
	}

	@Test
	public void test() {
		CachingConnectionFactory ccf = new CachingConnectionFactory("localhost");
		RabbitTemplate template = new RabbitTemplate(ccf);
		RabbitAdmin admin = new RabbitAdmin(ccf);
		FanoutExchange fanout = new FanoutExchange("log4j2Test");
		admin.declareExchange(fanout);
		Queue queue = new Queue("log4j2Test");
		admin.declareQueue(queue);
		admin.declareBinding(BindingBuilder.bind(queue).to(fanout));
		Logger logger = LogManager.getLogger("foo");
		logger.info("foo");
		logger.info("bar");
		template.setReceiveTimeout(10000);
		Message received = template.receive(queue.getName());
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey()).isEqualTo("testAppId.foo.INFO");
		Object foo = received.getMessageProperties().getHeader("foo");
		assertThat(foo).isNotNull();
		assertThat(foo).isInstanceOf(String.class);
		assertThat(foo).isEqualTo("bar");
		// Cross-platform string comparison. Windows expects \n\r in the end of line
		assertThat(new String(received.getBody())).startsWith("foo");
		received = template.receive(queue.getName());
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getReceivedRoutingKey()).isEqualTo("testAppId.foo.INFO");
		assertThat(new String(received.getBody())).startsWith("bar");
		Object threadName = received.getMessageProperties().getHeaders().get("thread");
		assertThat(threadName).isNotNull();
		assertThat(threadName).isInstanceOf(String.class);
		assertThat(threadName).isEqualTo(Thread.currentThread().getName());
		foo = received.getMessageProperties().getHeaders().get("foo");
		assertThat(foo).isNotNull();
		assertThat(foo).isInstanceOf(String.class);
		assertThat(foo).isEqualTo("bar");
		ccf.destroy();
	}

	@Test
	@Disabled("weird - this.events.take() in appender is returning null")
	public void testProperties() {
		Logger logger = LogManager.getLogger("foo");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>getPropertyValue(logger, "context.configuration.appenders")
						.get("rabbitmq");
		Object manager = TestUtils.getPropertyValue(appender, "manager");
		// <RabbitMQ name="rabbitmq"
		// addresses="localhost:5672"
		// host="localhost" port="5672" user="guest" password="guest" virtualHost="/"
		// exchange="log4j2Test" exchangeType="fanout" declareExchange="true"
		// durable="true" autoDelete="false"
		// applicationId="testAppId" routingKeyPattern="%X{applicationId}.%c.%p"
		// contentType="text/plain" contentEncoding="UTF-8" generateId="true"
		// deliveryMode="NON_PERSISTENT"
		// charset="UTF-8"
		// async="false"
		// senderPoolSize="3" maxSenderRetries="5"
		// foo="foo"
		// bar="bar">
		// </RabbitMQ>
		assertThat(TestUtils.<String>getPropertyValue(manager, "addresses")).isEqualTo("localhost:5672");
		assertThat(TestUtils.<String>getPropertyValue(manager, "host")).isEqualTo("localhost");
		assertThat(TestUtils.<Integer>getPropertyValue(manager, "port")).isEqualTo(5672);
		assertThat(TestUtils.<String>getPropertyValue(manager, "username")).isEqualTo("guest");
		assertThat(TestUtils.<String>getPropertyValue(manager, "password")).isEqualTo("guest");
		assertThat(TestUtils.<String>getPropertyValue(manager, "virtualHost")).isEqualTo("/");
		assertThat(TestUtils.<String>getPropertyValue(manager, "exchangeName")).isEqualTo("log4j2Test");
		assertThat(TestUtils.<String>getPropertyValue(manager, "exchangeType")).isEqualTo("fanout");
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "declareExchange")).isTrue();
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "durable")).isTrue();
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "autoDelete")).isFalse();
		assertThat(TestUtils.<String>getPropertyValue(manager, "applicationId")).isEqualTo("testAppId");
		assertThat(TestUtils.<String>getPropertyValue(manager, "routingKeyPattern")).isEqualTo("%X{applicationId}.%c.%p");
		assertThat(TestUtils.<String>getPropertyValue(manager, "contentType")).isEqualTo("text/plain");
		assertThat(TestUtils.<String>getPropertyValue(manager, "contentEncoding")).isEqualTo("UTF-8");
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "generateId")).isTrue();
		assertThat(TestUtils.<MessageDeliveryMode>getPropertyValue(manager, "deliveryMode"))
				.isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(TestUtils.<String>getPropertyValue(manager, "contentEncoding")).isEqualTo("UTF-8");
		assertThat(TestUtils.<Integer>getPropertyValue(manager, "senderPoolSize")).isEqualTo(3);
		assertThat(TestUtils.<Integer>getPropertyValue(manager, "maxSenderRetries")).isEqualTo(5);
		// change the property to true, and this fails and test() randomly fails too.
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "async")).isFalse();
		// default value
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "addMdcAsHeaders")).isTrue();

		java.util.Queue<?> queue = TestUtils.getPropertyValue(appender, "events");
		int i = 0;
		while (queue.poll() != null) {
			i++;
		}
		assertThat(i).isEqualTo(0);

		assertThat(TestUtils.<String>getPropertyValue(appender, "foo")).isEqualTo("foo");
		assertThat(TestUtils.<String>getPropertyValue(appender, "bar")).isEqualTo("bar");

		Object events = TestUtils.getPropertyValue(appender, "events");
		assertThat(events).isInstanceOf(ArrayBlockingQueue.class);
	}

	@Test
	public void testAmqpAppenderEventQueueTypeDefaultsToLinkedBlockingQueue() {
		Logger logger = LogManager.getLogger("default_queue_logger");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>getPropertyValue(logger, "context.configuration.appenders")
						.get("rabbitmq_default_queue");

		Object events = TestUtils.getPropertyValue(appender, "events");

		assertThat(TestUtils.<String>getPropertyValue(appender, "foo")).isEqualTo("defaultFoo");
		assertThat(TestUtils.<String>getPropertyValue(appender, "bar")).isEqualTo("defaultBar");

		assertThat(TestUtils.<Boolean>getPropertyValue(appender, "manager.addMdcAsHeaders")).isTrue();

		assertThat(events).isInstanceOf(LinkedBlockingQueue.class);
	}

	@Test
	public void testUriProperties() {
		Logger logger = LogManager.getLogger("bar");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>getPropertyValue(logger, "context.configuration.appenders")
						.get("rabbitmq_uri");
		Object manager = TestUtils.getPropertyValue(appender, "manager");
		assertThat(TestUtils.getPropertyValue(manager, "uri").toString())
				.isEqualTo("amqp://guest:guest@localhost:5672/");

		assertThat(TestUtils.<Object>getPropertyValue(manager, "host")).isNull();
		assertThat(TestUtils.<Object>getPropertyValue(manager, "port")).isNull();
		assertThat(TestUtils.<Object>getPropertyValue(manager, "username")).isNull();
		assertThat(TestUtils.<Object>getPropertyValue(manager, "password")).isNull();
		assertThat(TestUtils.<Object>getPropertyValue(manager, "virtualHost")).isNull();
		assertThat(TestUtils.<Boolean>getPropertyValue(manager, "addMdcAsHeaders")).isFalse();

		assertThat(TestUtils.<String>getPropertyValue(appender, "foo")).isEqualTo("foo_uri");
		assertThat(TestUtils.<String>getPropertyValue(appender, "bar")).isEqualTo("bar_uri");
	}

	@Test
	public void testDefaultConfiguration() {
		@SuppressWarnings("resource")
		AmqpAppender.AmqpManager manager = new ExtendAmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean, never()).setUseSSL(anyBoolean());
	}

	@Test
	public void testCustomHostInformation() {
		AmqpAppender.AmqpManager manager = new ExtendAmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");

		String host = "rabbitmq.com";
		int port = 5671;
		String username = "user";
		String password = "password";
		String virtualHost = "vhost";

		ReflectionTestUtils.setField(manager, "host", host);
		ReflectionTestUtils.setField(manager, "port", port);
		ReflectionTestUtils.setField(manager, "username", username);
		ReflectionTestUtils.setField(manager, "password", password);
		ReflectionTestUtils.setField(manager, "virtualHost", virtualHost);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verify(bean).setHost(host);
		verify(bean).setPort(port);
		verify(bean).setUsername(username);
		verify(bean).setPassword(password);
		verify(bean).setVirtualHost(virtualHost);
	}

	private void verifyDefaultHostProperties(RabbitConnectionFactoryBean bean) {
		verify(bean, never()).setHost("localhost");
		verify(bean, never()).setPort(5672);
		verify(bean, never()).setUsername("guest");
		verify(bean, never()).setPassword("guest");
		verify(bean, never()).setVirtualHost("/");
	}

}
