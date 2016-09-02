/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.log4j2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Gary Russell
 * @author Stephen Oakey
 *
 * @since 1.6
 */
public class AmqpAppenderTests {

	@ClassRule
	public static BrokerRunning brokerRunning = BrokerRunning.isRunning();

	private static final LoggerContext LOGGER_CONTEXT = (LoggerContext) LogManager.getContext(false);

	private static final URI ORIGINAL_LOGGER_CONFIG = LOGGER_CONTEXT.getConfigLocation();

	@BeforeClass
	public static void setup() throws IOException {
		LOGGER_CONTEXT.setConfigLocation(new ClassPathResource("log4j2-amqp-appender.xml").getURI());
		LOGGER_CONTEXT.reconfigure();
	}

	@AfterClass
	public static void teardown() {
		LOGGER_CONTEXT.setConfigLocation(ORIGINAL_LOGGER_CONFIG);
		LOGGER_CONTEXT.reconfigure();
		brokerRunning.getAdmin().deleteQueue("log4jTest");
		brokerRunning.getAdmin().deleteQueue("log4j2Test");
		brokerRunning.getAdmin().deleteExchange("log4j2Test");
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
		template.setReceiveTimeout(10000);
		Message received = template.receive(queue.getName());
		assertNotNull(received);
		assertEquals("testAppId.foo.INFO", received.getMessageProperties().getReceivedRoutingKey());
	}

	@Test
	public void testProperties() {
		Logger logger = LogManager.getLogger("foo");
		AmqpAppender appender = (AmqpAppender) TestUtils.getPropertyValue(logger, "context.configuration.appenders",
				Map.class).get("rabbitmq");
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
		// senderPoolSize="3" maxSenderRetries="5">
		// </RabbitMQ>
		assertEquals("localhost:5672", TestUtils.getPropertyValue(manager, "addresses"));
		assertEquals("localhost", TestUtils.getPropertyValue(manager, "host"));
		assertEquals(5672, TestUtils.getPropertyValue(manager, "port"));
		assertEquals("guest", TestUtils.getPropertyValue(manager, "username"));
		assertEquals("guest", TestUtils.getPropertyValue(manager, "password"));
		assertEquals("/", TestUtils.getPropertyValue(manager, "virtualHost"));
		assertEquals("log4j2Test", TestUtils.getPropertyValue(manager, "exchangeName"));
		assertEquals("fanout", TestUtils.getPropertyValue(manager, "exchangeType"));
		assertTrue(TestUtils.getPropertyValue(manager, "declareExchange", Boolean.class));
		assertTrue(TestUtils.getPropertyValue(manager, "durable", Boolean.class));
		assertFalse(TestUtils.getPropertyValue(manager, "autoDelete", Boolean.class));
		assertEquals("testAppId", TestUtils.getPropertyValue(manager, "applicationId"));
		assertEquals("%X{applicationId}.%c.%p", TestUtils.getPropertyValue(manager, "routingKeyPattern"));
		assertEquals("text/plain", TestUtils.getPropertyValue(manager, "contentType"));
		assertEquals("UTF-8", TestUtils.getPropertyValue(manager, "contentEncoding"));
		assertTrue(TestUtils.getPropertyValue(manager, "generateId", Boolean.class));
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, TestUtils.getPropertyValue(manager, "deliveryMode"));
		assertEquals("UTF-8", TestUtils.getPropertyValue(manager, "contentEncoding"));
		assertEquals(3, TestUtils.getPropertyValue(manager, "senderPoolSize"));
		assertEquals(5, TestUtils.getPropertyValue(manager, "maxSenderRetries"));
	}

	@Test
	public void testDefaultConfiguration() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean, never()).setUseSSL(anyBoolean());
	}

	@Test
	public void testCustomHostInformation() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");

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

	@Test
	public void testDefaultSslConfiguration() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");
		ReflectionTestUtils.setField(manager, "useSsl", true);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean, never()).setSslAlgorithm(anyString());
	}

	@Test
	public void testSslConfigurationWithAlgorithm() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");
		ReflectionTestUtils.setField(manager, "useSsl", true);
		String sslAlgorithm = "TLSv2";
		ReflectionTestUtils.setField(manager, "sslAlgorithm", sslAlgorithm);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean).setSslAlgorithm(eq(sslAlgorithm));
	}

	@Test
	public void testSslConfigurationWithSslPropertiesResource() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");
		ReflectionTestUtils.setField(manager, "useSsl", true);

		String path = "ssl.properties";
		ReflectionTestUtils.setField(manager, "sslPropertiesLocation", "classpath:" + path);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean).setSslPropertiesLocation(eq(new ClassPathResource(path)));
		verify(bean, never()).setKeyStore(anyString());
		verify(bean, never()).setKeyStorePassphrase(anyString());
		verify(bean, never()).setKeyStoreType(anyString());
		verify(bean, never()).setTrustStore(anyString());
		verify(bean, never()).setTrustStorePassphrase(anyString());
		verify(bean, never()).setTrustStoreType(anyString());
	}

	@Test
	public void testSslConfigurationWithKeyAndTrustStore() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");
		ReflectionTestUtils.setField(manager, "useSsl", true);

		String keyStore = "file:/path/to/client/keycert.p12";
		String keyStorePassphrase = "secret";
		String keyStoreType = "foo";
		String trustStore = "file:/path/to/client/truststore";
		String trustStorePassphrase = "secret2";
		String trustStoreType = "bar";

		ReflectionTestUtils.setField(manager, "keyStore", keyStore);
		ReflectionTestUtils.setField(manager, "keyStorePassphrase", keyStorePassphrase);
		ReflectionTestUtils.setField(manager, "keyStoreType", keyStoreType);
		ReflectionTestUtils.setField(manager, "trustStore", trustStore);
		ReflectionTestUtils.setField(manager, "trustStorePassphrase", trustStorePassphrase);
		ReflectionTestUtils.setField(manager, "trustStoreType", trustStoreType);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean, never()).setSslPropertiesLocation(any());
		verify(bean).setKeyStore(keyStore);
		verify(bean).setKeyStorePassphrase(keyStorePassphrase);
		verify(bean).setKeyStoreType(keyStoreType);
		verify(bean).setTrustStore(trustStore);
		verify(bean).setTrustStorePassphrase(trustStorePassphrase);
		verify(bean).setTrustStoreType(trustStoreType);
	}

	@Test
	public void testSslConfigurationWithKeyAndTrustStoreDefaultTypes() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager("test");
		ReflectionTestUtils.setField(manager, "useSsl", true);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean).setKeyStoreType("JKS");
		verify(bean).setTrustStoreType("JKS");
	}

	private void verifyDefaultHostProperties(RabbitConnectionFactoryBean bean) {
		verify(bean).setHost("localhost");
		verify(bean).setPort(5672);
		verify(bean).setUsername("guest");
		verify(bean).setPassword("guest");
		verify(bean).setVirtualHost("/");
	}

}
