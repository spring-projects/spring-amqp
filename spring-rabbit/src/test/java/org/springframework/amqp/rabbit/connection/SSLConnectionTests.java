/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.security.SecureRandom;
import java.util.Collections;

import javax.net.ssl.SSLContext;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.CredentialsRefreshService;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;



/**
 * @author Gary Russell
 * @author Heath Abelson
 * @author Hareendran
 * @author Ngoc Nhan
 *
 * @since 1.4.4
 *
 */
public class SSLConnectionTests {

	@Test
	@Disabled
	public void test() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setUseSSL(true);
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.setClientProperties(Collections.<String, Object>singletonMap("foo", "bar"));
		fb.afterPropertiesSet();
		ConnectionFactory cf = fb.getObject();
		assertThat(cf.getClientProperties().get("foo")).isEqualTo("bar");
		Connection conn = cf.newConnection();
		Channel chan = conn.createChannel();
		chan.close();
		conn.close();
	}

	@Test
	public void testAlgNoProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setMaxInboundMessageBodySize(1000);
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.setSslAlgorithm("TLSv1.2");
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol(Mockito.any(SSLContext.class));
		assertThat(rabbitCf).hasFieldOrPropertyWithValue("maxInboundMessageBodySize", 1000);
	}

	@Test
	public void testNoAlgNoProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol(Mockito.any(SSLContext.class));
	}

	@Test
	public void testUseSslProtocolShouldNotBeCalled() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf, never()).useSslProtocol();
		ArgumentCaptor<SSLContext> captor = ArgumentCaptor.forClass(SSLContext.class);
		verify(rabbitCf).useSslProtocol(captor.capture());
		assertThat(captor.getValue().getProtocol()).isEqualTo("TLSv1.2");
	}

	@Test
	public void testSkipServerCertificate() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.setSkipServerCertificateValidation(true);
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol();
		verify(rabbitCf).useSslProtocol("TLSv1.2");
	}

	@Test
	public void testSkipServerCertificateWithAlgorithm() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.setSslAlgorithm("TLSv1.1");
		fb.setSkipServerCertificateValidation(true);
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol("TLSv1.1");
	}



	@Test
	public void testUseSslProtocolWithProtocolShouldNotBeCalled() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.setSslAlgorithm("TLSv1.2");
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf, never()).useSslProtocol("TLSv1.2");
	}


	@Test
	public void testKSTS() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		Log logger = spy(TestUtils.getPropertyValue(fb, "logger", Log.class));
		given(logger.isDebugEnabled()).willReturn(true);
		new DirectFieldAccessor(fb).setPropertyValue("logger", logger);
		fb.setUseSSL(true);
		fb.setKeyStoreType("JKS");
		fb.setKeyStoreResource(new ClassPathResource("test.ks"));
		fb.setKeyStorePassphrase("secret");
		fb.setTrustStoreResource(new ClassPathResource("test.truststore.ks"));
		fb.setKeyStorePassphrase("secret");
		fb.setSecureRandom(SecureRandom.getInstanceStrong());
		fb.afterPropertiesSet();
		fb.getObject();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).debug(captor.capture());
		final String log = captor.getValue();
		assertThat(log).contains("KM: [").contains("TM: [").contains("random: ");
	}

	@Test
	public void testNullTS() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		Log logger = spy(TestUtils.getPropertyValue(fb, "logger", Log.class));
		given(logger.isDebugEnabled()).willReturn(true);
		new DirectFieldAccessor(fb).setPropertyValue("logger", logger);
		fb.setUseSSL(true);
		fb.setKeyStoreType("JKS");
		fb.setKeyStoreResource(new ClassPathResource("test.ks"));
		fb.setKeyStorePassphrase("secret");
		fb.afterPropertiesSet();
		fb.getObject();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).debug(captor.capture());
		final String log = captor.getValue();
		assertThat(log).contains("KM: [").contains("TM: null").contains("random: null");
	}

	@Test
	public void testNullKS() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		Log logger = spy(TestUtils.getPropertyValue(fb, "logger", Log.class));
		given(logger.isDebugEnabled()).willReturn(true);
		new DirectFieldAccessor(fb).setPropertyValue("logger", logger);
		fb.setUseSSL(true);
		fb.setKeyStoreType("JKS");
		fb.setTrustStoreResource(new ClassPathResource("test.truststore.ks"));
		fb.setKeyStorePassphrase("secret");
		fb.afterPropertiesSet();
		fb.getObject();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).debug(captor.capture());
		final String log = captor.getValue();
		assertThat(log).contains("KM: null").contains("TM: [").contains("random: null");
	}

	@Test
	public void testTypeDefault() {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		assertThat(fb.getKeyStoreType()).isEqualTo("PKCS12");
		assertThat(fb.getTrustStoreType()).isEqualTo("JKS");
	}

	@Test
	public void testTypeProps() {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.afterPropertiesSet();
		assertThatException().isThrownBy(fb::setUpSSL);
		assertThat(fb.getKeyStoreType()).isEqualTo("foo");
		assertThat(fb.getTrustStoreType()).isEqualTo("bar");
	}

	@Test
	public void testTypeSettersNoProps() {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setKeyStoreType("alice");
		fb.setTrustStoreType("bob");
		assertThat(fb.getKeyStoreType()).isEqualTo("alice");
		assertThat(fb.getTrustStoreType()).isEqualTo("bob");
	}

	@Test
	public void testTypeSettersOverrideProps() {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.afterPropertiesSet();
		fb.setKeyStoreType("alice");
		fb.setTrustStoreType("bob");
		assertThatException().isThrownBy(fb::setUpSSL);
		assertThat(fb.getKeyStoreType()).isEqualTo("alice");
		assertThat(fb.getTrustStoreType()).isEqualTo("bob");
	}

	@Test
	public void credentials() {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		CredentialsProvider provider = mock(CredentialsProvider.class);
		fb.setCredentialsProvider(provider);
		CredentialsRefreshService service = mock(CredentialsRefreshService.class);
		fb.setCredentialsRefreshService(service);
		assertThat(TestUtils.getPropertyValue(fb.getRabbitConnectionFactory(), "credentialsProvider"))
				.isSameAs(provider);
		assertThat(TestUtils.getPropertyValue(fb.getRabbitConnectionFactory(), "credentialsRefreshService"))
				.isSameAs(service);
	}


}
