/*
 * Copyright 2016-present the original author or authors.
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.JDKSaslConfig;
import com.rabbitmq.client.impl.CRDemoMechanism;
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
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Stephen Oakey
 * @author Artem Bilan
 * @author Dominique Villard
 * @author Nicolas Ristock
 * @author Eugene Gusev
 *
 * @since 1.6
 */
@RabbitAvailable
public class AmqpAppenderTests {

	private static final LoggerContext LOGGER_CONTEXT = (LoggerContext) LogManager.getContext(false);

	private static final URI ORIGINAL_LOGGER_CONFIG = LOGGER_CONTEXT.getConfigLocation();

	@BeforeAll
	public static void setup() throws IOException {
		LOGGER_CONTEXT.setConfigLocation(new ClassPathResource("log4j2-amqp-appender.xml").getURI());
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
		ccf.destroy();
	}

	@Test
	@Disabled("weird - this.events.take() in appender is returning null")
	public void testProperties() {
		Logger logger = LogManager.getLogger("foo");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
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
		// senderPoolSize="3" maxSenderRetries="5">
		// </RabbitMQ>
		assertThat(TestUtils.getPropertyValue(manager, "addresses")).isEqualTo("localhost:5672");
		assertThat(TestUtils.getPropertyValue(manager, "host")).isEqualTo("localhost");
		assertThat(TestUtils.getPropertyValue(manager, "port")).isEqualTo(5672);
		assertThat(TestUtils.getPropertyValue(manager, "username")).isEqualTo("guest");
		assertThat(TestUtils.getPropertyValue(manager, "password")).isEqualTo("guest");
		assertThat(TestUtils.getPropertyValue(manager, "virtualHost")).isEqualTo("/");
		assertThat(TestUtils.getPropertyValue(manager, "exchangeName")).isEqualTo("log4j2Test");
		assertThat(TestUtils.getPropertyValue(manager, "exchangeType")).isEqualTo("fanout");
		assertThat(TestUtils.<Boolean>propertyValue(manager, "declareExchange")).isTrue();
		assertThat(TestUtils.<Boolean>propertyValue(manager, "durable")).isTrue();
		assertThat(TestUtils.<Boolean>propertyValue(manager, "autoDelete")).isFalse();
		assertThat(TestUtils.getPropertyValue(manager, "applicationId")).isEqualTo("testAppId");
		assertThat(TestUtils.getPropertyValue(manager, "routingKeyPattern")).isEqualTo("%X{applicationId}.%c.%p");
		assertThat(TestUtils.getPropertyValue(manager, "contentType")).isEqualTo("text/plain");
		assertThat(TestUtils.getPropertyValue(manager, "contentEncoding")).isEqualTo("UTF-8");
		assertThat(TestUtils.<Boolean>propertyValue(manager, "generateId")).isTrue();
		assertThat(TestUtils.getPropertyValue(manager, "deliveryMode")).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
		assertThat(TestUtils.getPropertyValue(manager, "contentEncoding")).isEqualTo("UTF-8");
		assertThat(TestUtils.getPropertyValue(manager, "senderPoolSize")).isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(manager, "maxSenderRetries")).isEqualTo(5);
		// change the property to true, and this fails and test() randomly fails too.
		assertThat(TestUtils.<Boolean>propertyValue(manager, "async")).isFalse();
		// default value
		assertThat(TestUtils.<Boolean>propertyValue(manager, "addMdcAsHeaders")).isTrue();

		java.util.Queue<?> queue = TestUtils.propertyValue(appender, "events");
		int i = 0;
		while (queue.poll() != null) {
			i++;
		}
		assertThat(i).isEqualTo(10);

		Object events = TestUtils.getPropertyValue(appender, "events");
		assertThat(events.getClass()).isEqualTo(ArrayBlockingQueue.class);
	}

	@Test
	public void testSaslConfig() {
		Logger logger = LogManager.getLogger("sasl");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
						.get("sasl1");
		assertThat(RabbitUtils.stringToSaslConfig(TestUtils.propertyValue(appender, "manager.saslConfig"), mock()))
				.isInstanceOf(DefaultSaslConfig.class)
				.hasFieldOrPropertyWithValue("mechanism", "PLAIN");
		appender = TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
				.get("sasl2");
		assertThat(RabbitUtils.stringToSaslConfig(TestUtils.propertyValue(appender, "manager.saslConfig"), mock()))
				.isInstanceOf(DefaultSaslConfig.class)
				.hasFieldOrPropertyWithValue("mechanism", "EXTERNAL");
		appender = TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
				.get("sasl3");
		assertThat(RabbitUtils.stringToSaslConfig(TestUtils.propertyValue(appender, "manager.saslConfig"), mock()))
				.isInstanceOf(JDKSaslConfig.class);
		appender = TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
				.get("sasl4");
		assertThat(RabbitUtils.stringToSaslConfig(TestUtils.propertyValue(appender, "manager.saslConfig"), mock()))
				.isInstanceOf(CRDemoMechanism.CRDemoSaslConfig.class);
	}

	@Test
	public void testAmqpAppenderEventQueueTypeDefaultsToLinkedBlockingQueue() {
		Logger logger = LogManager.getLogger("default_queue_logger");
		logger.info("test");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
						.get("rabbitmq_default_queue");

		Object events = TestUtils.getPropertyValue(appender, "events");

		Object manager = TestUtils.getPropertyValue(appender, "manager");
		assertThat(TestUtils.<Boolean>propertyValue(manager, "addMdcAsHeaders")).isTrue();

		assertThat(events.getClass()).isEqualTo(LinkedBlockingQueue.class);
		BlockingQueue<?> queue = (BlockingQueue<?>) events;
		await().until(() -> queue.size() == 0);
	}

	@Test
	public void testUriProperties() {
		Logger logger = LogManager.getLogger("bar");
		AmqpAppender appender =
				TestUtils.<Map<String, AmqpAppender>>propertyValue(logger, "context.configuration.appenders")
						.get("rabbitmq_uri");
		Object manager = TestUtils.getPropertyValue(appender, "manager");
		assertThat(TestUtils.getPropertyValue(manager, "uri").toString())
				.isEqualTo("amqp://guest:guest@localhost:5672/");

		assertThat(TestUtils.getPropertyValue(manager, "host")).isNull();
		assertThat(TestUtils.getPropertyValue(manager, "port")).isNull();
		assertThat(TestUtils.getPropertyValue(manager, "username")).isNull();
		assertThat(TestUtils.getPropertyValue(manager, "password")).isNull();
		assertThat(TestUtils.getPropertyValue(manager, "virtualHost")).isNull();
		assertThat(TestUtils.<Boolean>propertyValue(manager, "addMdcAsHeaders")).isFalse();
	}

	@Test
	public void testDefaultConfiguration() {
		@SuppressWarnings("resource")
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean, never()).setUseSSL(anyBoolean());
	}

	@Test
	public void testCustomHostInformation() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");

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
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");
		ReflectionTestUtils.setField(manager, "useSsl", true);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean, never()).setSslAlgorithm(anyString());
	}

	@Test
	public void testSslConfigurationWithAlgorithm() {
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");
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
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");
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
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");
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
		AmqpAppender.AmqpManager manager = new AmqpAppender.AmqpManager(LOGGER_CONTEXT, "test");
		ReflectionTestUtils.setField(manager, "useSsl", true);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		manager.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean).setKeyStoreType("JKS");
		verify(bean).setTrustStoreType("JKS");
	}

	private void verifyDefaultHostProperties(RabbitConnectionFactoryBean bean) {
		verify(bean, never()).setHost("localhost");
		verify(bean, never()).setPort(5672);
		verify(bean, never()).setUsername("guest");
		verify(bean, never()).setPassword("guest");
		verify(bean, never()).setVirtualHost("/");
	}

}
