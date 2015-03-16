/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.SocketConfigurator;


/**
 * Factory bean to create a RabbitMQ ConnectionFactory, delegating most
 * setter methods and optionally enabling SSL, with or without
 * certificate validation. When {@link #setSslPropertiesLocation(Resource) sslPropertiesLocation}
 * is not null, the default implementation loads a {@code PKCS12} keystore and a
 * {@code JKS} truststore using the supplied properties and intializes {@code SunX509} key
 * and trust manager factories. These are then used to initialize an {@link SSLContext}
 * using the {@link #setSslAlgorithm(String) sslAlgorithm} (default TLSv1.1).
 * <p>
 * Override {@link #createSSLContext()} to create and/or perform further modification of the context.
 * <p>
 * Override {@link #setUpSSL()} to take complete control over setting up SSL.
 *
 * @author Gary Russell
 *
 * @since 1.4
 */
public class RabbitConnectionFactoryBean extends AbstractFactoryBean<ConnectionFactory> {

	protected final ConnectionFactory connectionFactory = new ConnectionFactory();

	private boolean useSSL;

	private Resource sslPropertiesLocation;

	private volatile String sslAlgorithm = "SSLv3";

	/**
	 * Whether or not the factory should be configured to use SSL.
	 * @param useSSL true to use SSL.
	 */
	public void setUseSSL(boolean useSSL) {
		this.useSSL = useSSL;
	}

	/**
	 * @return true to use ssl.
	 * @since 1.4.4.
	 */
	protected boolean isUseSSL() {
		return useSSL;
	}

	/**
	 * Set the algorithm to use; default SSLv3.
	 * @param sslAlgorithm the algorithm.
	 */
	public void setSslAlgorithm(String sslAlgorithm) {
		this.sslAlgorithm = sslAlgorithm;
	}

	/**
	 * @return the ssl algorithm.
	 * @since 1.4.4
	 */
	protected String getSslAlgorithm() {
		return sslAlgorithm;
	}

	/**
	 * When {@link #setUseSSL(boolean)} is true, the SSL properties to use (optional).
	 * Resource referencing a properties file with the following properties:
	 * <ul>
	 * <li>keyStore=file:/secret/keycert.p12</li>
	 * <li>trustStore=file:/secret/trustStore</li>
	 * <li>keyStore.passPhrase=secret</li>
	 * <li>trustStore.passPhrase=secret</li>
	 * </ul>
	 * @param sslPropertiesLocation the Resource to the ssl properties
	 */
	public void setSslPropertiesLocation(Resource sslPropertiesLocation) {
		this.sslPropertiesLocation = sslPropertiesLocation;
	}

	/**
	 * @return the properties location.
	 * @since 1.4.4
	 */
	protected Resource getSslPropertiesLocation() {
		return sslPropertiesLocation;
	}

	/**
	 * @param host the host.
	 * @see com.rabbitmq.client.ConnectionFactory#setHost(java.lang.String)
	 */
	public void setHost(String host) {
		this.connectionFactory.setHost(host);
	}

	/**
	 * @param port the port.
	 * @see com.rabbitmq.client.ConnectionFactory#setPort(int)
	 */
	public void setPort(int port) {
		this.connectionFactory.setPort(port);
	}

	/**
	 * @param username the user name.
	 * @see com.rabbitmq.client.ConnectionFactory#setUsername(java.lang.String)
	 */
	public void setUsername(String username) {
		this.connectionFactory.setUsername(username);
	}

	/**
	 * @param password the password.
	 * @see com.rabbitmq.client.ConnectionFactory#setPassword(java.lang.String)
	 */
	public void setPassword(String password) {
		this.connectionFactory.setPassword(password);
	}

	/**
	 * @param virtualHost the virtual host.
	 * @see com.rabbitmq.client.ConnectionFactory#setVirtualHost(java.lang.String)
	 */
	public void setVirtualHost(String virtualHost) {
		this.connectionFactory.setVirtualHost(virtualHost);
	}

	/**
	 * @param uri the uri.
	 * @throws URISyntaxException invalid syntax.
	 * @throws NoSuchAlgorithmException no such algorithm.
	 * @throws KeyManagementException key management.
	 * @see com.rabbitmq.client.ConnectionFactory#setUri(java.net.URI)
	 */
	public void setUri(URI uri) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
		this.connectionFactory.setUri(uri);
	}

	/**
	 * @param uriString the uri.
	 * @throws URISyntaxException invalid syntax.
	 * @throws NoSuchAlgorithmException no such algorithm.
	 * @throws KeyManagementException key management.
	 * @see com.rabbitmq.client.ConnectionFactory#setUri(java.lang.String)
	 */
	public void setUri(String uriString) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
		this.connectionFactory.setUri(uriString);
	}

	/**
	 * @param requestedChannelMax the max requested channels.
	 * @see com.rabbitmq.client.ConnectionFactory#setRequestedChannelMax(int)
	 */
	public void setRequestedChannelMax(int requestedChannelMax) {
		this.connectionFactory.setRequestedChannelMax(requestedChannelMax);
	}

	/**
	 * @param requestedFrameMax the requested max frames.
	 * @see com.rabbitmq.client.ConnectionFactory#setRequestedFrameMax(int)
	 */
	public void setRequestedFrameMax(int requestedFrameMax) {
		this.connectionFactory.setRequestedFrameMax(requestedFrameMax);
	}

	/**
	 * @param connectionTimeout the connection timeout.
	 * @see com.rabbitmq.client.ConnectionFactory#setConnectionTimeout(int)
	 */
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionFactory.setConnectionTimeout(connectionTimeout);
	}

	/**
	 * @param requestedHeartbeat the requested heartbeat.
	 * @see com.rabbitmq.client.ConnectionFactory#setRequestedHeartbeat(int)
	 */
	public void setRequestedHeartbeat(int requestedHeartbeat) {
		this.connectionFactory.setRequestedHeartbeat(requestedHeartbeat);
	}

	/**
	 * @param clientProperties the client properties.
	 * @see com.rabbitmq.client.ConnectionFactory#setClientProperties(java.util.Map)
	 */
	public void setClientProperties(Map<String, Object> clientProperties) {
		this.connectionFactory.setClientProperties(clientProperties);
	}

	/**
	 * @param saslConfig the sasl config.
	 * @see com.rabbitmq.client.ConnectionFactory#setSaslConfig(com.rabbitmq.client.SaslConfig)
	 */
	public void setSaslConfig(SaslConfig saslConfig) {
		this.connectionFactory.setSaslConfig(saslConfig);
	}

	/**
	 * @param factory the socket factory.
	 * @see com.rabbitmq.client.ConnectionFactory#setSocketFactory(javax.net.SocketFactory)
	 */
	public void setSocketFactory(SocketFactory factory) {
		this.connectionFactory.setSocketFactory(factory);
	}

	/**
	 * @param socketConfigurator the socket configurator.
	 * @see com.rabbitmq.client.ConnectionFactory#setSocketConfigurator(com.rabbitmq.client.SocketConfigurator)
	 */
	public void setSocketConfigurator(SocketConfigurator socketConfigurator) {
		this.connectionFactory.setSocketConfigurator(socketConfigurator);
	}

	/**
	 * @param executor the executor service
	 * @see com.rabbitmq.client.ConnectionFactory#setSharedExecutor(java.util.concurrent.ExecutorService)
	 */
	public void setSharedExecutor(ExecutorService executor) {
		this.connectionFactory.setSharedExecutor(executor);
	}

	/**
	 * @param threadFactory the thread factory.
	 * @see com.rabbitmq.client.ConnectionFactory#setThreadFactory(java.util.concurrent.ThreadFactory)
	 */
	public void setThreadFactory(ThreadFactory threadFactory) {
		this.connectionFactory.setThreadFactory(threadFactory);
	}

	/**
	 * @param exceptionHandler the exception handler.
	 * @see com.rabbitmq.client.ConnectionFactory#setExceptionHandler(com.rabbitmq.client.ExceptionHandler)
	 */
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.connectionFactory.setExceptionHandler(exceptionHandler);
	}

	@Override
	public Class<?> getObjectType() {
		return ConnectionFactory.class;
	}

	@Override
	protected ConnectionFactory createInstance() throws Exception {
		if (this.useSSL) {
			setUpSSL();
		}
		return this.connectionFactory;
	}

	/**
	 * Override this method to take complete control over the SSL setup.
	 * @throws Exception an Exception.
	 * @since 1.4.4
	 */
	protected void setUpSSL() throws Exception {
		if (this.sslPropertiesLocation == null) {
			this.connectionFactory.useSslProtocol();
		}
		else {
			Properties sslProperties = new Properties();
			sslProperties.load(this.sslPropertiesLocation.getInputStream());
			PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
			String keyStoreName = sslProperties.getProperty("keyStore");
			Assert.state(StringUtils.hasText(keyStoreName), "keyStore property required");
			String trustStoreName = sslProperties.getProperty("trustStore");
			Assert.state(StringUtils.hasText(trustStoreName), "trustStore property required");
			String keyStorePassword = sslProperties.getProperty("keyStore.passPhrase");
			Assert.state(StringUtils.hasText(keyStorePassword), "keyStore.passPhrase property required");
			String trustStorePassword = sslProperties.getProperty("trustStore.passPhrase");
			Assert.state(StringUtils.hasText(trustStorePassword), "trustStore.passPhrase property required");
			Resource keyStore = resolver.getResource(keyStoreName);
			Resource trustStore = resolver.getResource(trustStoreName);
			char[] keyPassphrase = keyStorePassword.toCharArray();
			char[] trustPassphrase = trustStorePassword.toCharArray();

			KeyStore ks = KeyStore.getInstance("PKCS12");
			ks.load(keyStore.getInputStream(), keyPassphrase);

			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(ks, keyPassphrase);

			KeyStore tks = KeyStore.getInstance("JKS");
			tks.load(trustStore.getInputStream(), trustPassphrase);

			TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(tks);

			SSLContext context = createSSLContext();
			context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
			this.connectionFactory.useSslProtocol(context);
		}
	}

	/**
	 * Override this method to create and/or configure the {@link SSLContext} used
	 * by the {@link ConnectionFactory}.
	 * @return The {@link SSLContext}.
	 * @throws NoSuchAlgorithmException if the algorithm is not available.
	 * @since 1.4.4
	 */
	protected SSLContext createSSLContext() throws NoSuchAlgorithmException {
		return SSLContext.getInstance(this.sslAlgorithm);
	}

}
