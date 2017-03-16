/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.security.SecureRandom;
import java.util.Collections;

import javax.net.ssl.SSLContext;

import org.apache.commons.logging.Log;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.io.ClassPathResource;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;



/**
 * @author Gary Russell
 * @author Heath Abelson
 * @author Hareendran
 *
 * @since 1.4.4
 *
 */
public class SSLConnectionTests {

	@Test
	@Ignore
	public void test() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setUseSSL(true);
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.setClientProperties(Collections.<String, Object>singletonMap("foo", "bar"));
		fb.afterPropertiesSet();
		ConnectionFactory cf = fb.getObject();
		assertEquals("bar", cf.getClientProperties().get("foo"));
		Connection conn = cf.newConnection();
		Channel chan = conn.createChannel();
		chan.close();
		conn.close();
	}

	@Test
	public void testAlgNoProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		ConnectionFactory rabbitCf = spy(TestUtils.getPropertyValue(fb, "connectionFactory", ConnectionFactory.class));
		new DirectFieldAccessor(fb).setPropertyValue("connectionFactory", rabbitCf);
		fb.setUseSSL(true);
		fb.setSslAlgorithm("TLSv1.2");
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol(Mockito.any(SSLContext.class));
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
		fb.setSslAlgorithm("TLSv1.2");
		fb.setSkipServerCertificateValidation(true);
		fb.afterPropertiesSet();
		fb.getObject();
		verify(rabbitCf).useSslProtocol("TLSv1.2");
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
		assertThat(log, allOf(containsString("KM: ["), containsString("TM: ["), containsString("random: java.")));
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
		assertThat(log, allOf(containsString("KM: ["), containsString("TM: null"), containsString("random: null")));
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
		assertThat(log, allOf(containsString("KM: null"), containsString("TM: ["), containsString("random: null")));
	}

	@Test
	public void testTypeDefault() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		assertEquals("PKCS12", fb.getKeyStoreType());
		assertEquals("JKS", fb.getTrustStoreType());
	}

	@Test
	public void testTypeProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.afterPropertiesSet();
		try {
			fb.setUpSSL();
			//Here we make sure the exception is thrown because setUpSSL() will fail.
			// But we only care about having it load the props
			fail("setupSSL should fail");
		}
		catch (Exception e) {
			assertEquals("foo", fb.getKeyStoreType());
			assertEquals("bar", fb.getTrustStoreType());
		}
	}

	@Test
	public void testTypeSettersNoProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setKeyStoreType("alice");
		fb.setTrustStoreType("bob");
		assertEquals("alice", fb.getKeyStoreType());
		assertEquals("bob", fb.getTrustStoreType());
	}

	@Test
	public void testTypeSettersOverrideProps() throws Exception {
		RabbitConnectionFactoryBean fb = new RabbitConnectionFactoryBean();
		fb.setSslPropertiesLocation(new ClassPathResource("ssl.properties"));
		fb.afterPropertiesSet();
		fb.setKeyStoreType("alice");
		fb.setTrustStoreType("bob");
		try {
			fb.setUpSSL();
			// Here we make sure the exception is thrown because setUpSSL() will fail.
			//But we only care about having it load the props
			fail("setupSSL should fail");
		}
		catch (Exception e) {
			assertEquals("alice", fb.getKeyStoreType());
			assertEquals("bob", fb.getTrustStoreType());
		}
	}

}
