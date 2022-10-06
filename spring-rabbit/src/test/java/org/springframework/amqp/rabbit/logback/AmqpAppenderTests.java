/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.JDKSaslConfig;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.impl.CRDemoMechanism;

/**
 *
 * @author Stephen Oakey
 * @author Artem Bilan
 *
 * @since 2.0
 */
@Disabled("Temporary")
public class AmqpAppenderTests {

	@Test
	public void testDefaultConfiguration() {
		AmqpAppender appender = new AmqpAppender();

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean, never()).setUseSSL(anyBoolean());
	}

	@Test
	public void testCustomHostInformation() throws URISyntaxException {
		AmqpAppender appender = new AmqpAppender();

		String host = "rabbitmq.com";
		int port = 5671;
		String username = "user";
		String password = "password";
		String virtualHost = "vhost";
		URI uri = new URI("amqps://user:password@rabbitmq.com/vhost");

		appender.setHost(host);
		appender.setPassword(password);
		appender.setPort(port);
		appender.setUsername(username);
		appender.setVirtualHost(virtualHost);
		appender.setUri(uri);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

		verify(bean).setHost(host);
		verify(bean).setPort(port);
		verify(bean).setUsername(username);
		verify(bean).setPassword(password);
		verify(bean).setVirtualHost(virtualHost);
		verify(bean).setUri(uri);
	}

	@Test
	public void testDefaultSslConfiguration() {
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean, never()).setSslAlgorithm(anyString());
	}

	@Test
	public void testSslConfigurationWithAlgorithm() {
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);
		appender.setVerifyHostname(false);
		String sslAlgorithm = "TLSv2";
		appender.setSslAlgorithm(sslAlgorithm);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(true);
		verify(bean).setSslAlgorithm(sslAlgorithm);
		verify(bean).setEnableHostnameVerification(false);
	}

	@Test
	public void testSslConfigurationWithSslPropertiesResource() {
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);

		String path = "ssl.properties";
		appender.setSslPropertiesLocation("classpath:" + path);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

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
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);

		String keyStore = "file:/path/to/client/keycert.p12";
		String keyStorePassphrase = "secret";
		String keyStoreType = "foo";
		String trustStore = "file:/path/to/client/truststore";
		String trustStorePassphrase = "secret2";
		String trustStoreType = "bar";

		appender.setKeyStore(keyStore);
		appender.setKeyStorePassphrase(keyStorePassphrase);
		appender.setKeyStoreType(keyStoreType);
		appender.setTrustStore(trustStore);
		appender.setTrustStorePassphrase(trustStorePassphrase);
		appender.setTrustStoreType(trustStoreType);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

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
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		verify(bean).setKeyStoreType("JKS");
		verify(bean).setTrustStoreType("JKS");
	}

	@Test
	public void testStartWithInvalidConnectionConfiguration() {
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);
		appender.setKeyStore("foo");
		appender.start();

		assertThat((boolean) ReflectionTestUtils.getField(appender, "started")).isFalse();
	}

	@Test
	public void testSasl() {
		AmqpAppender appender = new AmqpAppender();
		appender.setUseSsl(true);
		appender.setSaslConfig("DefaultSaslConfig.PLAIN");

		RabbitConnectionFactoryBean bean = mock(RabbitConnectionFactoryBean.class);
		appender.configureRabbitConnectionFactory(bean);

		verifyDefaultHostProperties(bean);
		verify(bean).setUseSSL(eq(true));
		ArgumentCaptor<SaslConfig> captor = ArgumentCaptor.forClass(SaslConfig.class);
		verify(bean).setSaslConfig(captor.capture());
		assertThat(captor.getValue())
				.isInstanceOf(DefaultSaslConfig.class)
				.hasFieldOrPropertyWithValue("mechanism", "PLAIN");
		appender.setSaslConfig("DefaultSaslConfig.EXTERNAL");
		appender.configureRabbitConnectionFactory(bean);
		verify(bean, times(2)).setSaslConfig(captor.capture());
		assertThat(captor.getValue())
				.isInstanceOf(DefaultSaslConfig.class)
				.hasFieldOrPropertyWithValue("mechanism", "EXTERNAL");
		appender.setSaslConfig("JDKSaslConfig");
		appender.configureRabbitConnectionFactory(bean);
		verify(bean, times(3)).setSaslConfig(captor.capture());
		assertThat(captor.getValue())
				.isInstanceOf(JDKSaslConfig.class);
		appender.setSaslConfig("CRDemoSaslConfig");
		appender.configureRabbitConnectionFactory(bean);
		verify(bean, times(4)).setSaslConfig(captor.capture());
		assertThat(captor.getValue())
				.isInstanceOf(CRDemoMechanism.CRDemoSaslConfig.class);
		appender.setSaslConfig("junk");
		assertThatThrownBy(() -> appender.configureRabbitConnectionFactory(bean))
			.isInstanceOf(UncategorizedAmqpException.class)
			.hasCauseInstanceOf(IllegalStateException.class)
			.withFailMessage("Unrecognized SaslConfig: junk");
	}


	private void verifyDefaultHostProperties(RabbitConnectionFactoryBean bean) {
		verify(bean, never()).setHost("localhost");
		verify(bean, never()).setPort(5672);
		verify(bean, never()).setUsername("guest");
		verify(bean, never()).setPassword("guest");
		verify(bean, never()).setVirtualHost("/");
	}

}
