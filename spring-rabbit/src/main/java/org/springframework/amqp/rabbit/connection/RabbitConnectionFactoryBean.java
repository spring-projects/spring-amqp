/*
 * Copyright 2002-2016 the original author or authors.
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

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
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
 * @author Heath Abelson
 *
 * @since 1.4
 */
public class RabbitConnectionFactoryBean extends AbstractFactoryBean<ConnectionFactory> {

	private final Log logger = LogFactory.getLog(getClass());

	private static final String KEY_STORE = "keyStore";

	private static final String TRUST_STORE = "trustStore";

	private static final String KEY_STORE_PASS_PHRASE = "keyStore.passPhrase";

	private static final String TRUST_STORE_PASS_PHRASE = "trustStore.passPhrase";

	private static final String KEY_STORE_TYPE = "keyStore.type";

	private static final String TRUST_STORE_TYPE = "trustStore.type";

	private static final String TLS_V1_1 = "TLSv1.1";

	private static final String KEY_STORE_DEFAULT_TYPE = "PKCS12";

	private static final String TRUST_STORE_DEFAULT_TYPE = "JKS";

	protected final ConnectionFactory connectionFactory = new ConnectionFactory();

	private final Properties sslProperties = new Properties();

	private boolean useSSL;

	private Resource sslPropertiesLocation;

	private volatile String keyStore;

	private volatile String trustStore;

	private volatile Resource keyStoreResource;

	private volatile Resource trustStoreResource;

	private volatile String keyStorePassphrase;

	private volatile String trustStorePassphrase;

	private volatile String keyStoreType;

	private volatile String trustStoreType;

	private volatile String sslAlgorithm = TLS_V1_1;

	private volatile boolean sslAlgorithmSet;

	private volatile SecureRandom secureRandom;

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
		return this.useSSL;
	}

	/**
	 * Set the algorithm to use; default TLSv1.1.
	 * @param sslAlgorithm the algorithm.
	 */
	public void setSslAlgorithm(String sslAlgorithm) {
		this.sslAlgorithm = sslAlgorithm;
		this.sslAlgorithmSet = true;
	}

	/**
	 * @return the ssl algorithm.
	 * @since 1.4.4
	 */
	protected String getSslAlgorithm() {
		return this.sslAlgorithm;
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
	 * <p>
	 * If this is provided, its properties (if present) will override the explicitly
	 * set property in this bean.
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
		return this.sslPropertiesLocation;
	}

	/**
	 * @return the key store resource.
	 * @since 1.5
	 */
	protected String getKeyStore() {
		return this.keyStore == null ? this.sslProperties.getProperty(KEY_STORE) : this.keyStore;
	}

	/**
	 * Set the key store resource (e.g. file:/foo/keystore) - overrides
	 * the property in {@link #setSslPropertiesLocation(Resource)}.
	 * Ignored if {@link #setTrustStoreResource(Resource)} is called with a
	 * resource.
	 * @param keyStore the keystore resource.
	 * @since 1.5
	 */
	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}

	protected Resource getKeyStoreResource() {
		return this.keyStoreResource;
	}

	/**
	 * Set a Resource pointing to the key store.
	 * @param keyStoreResource the resource.
	 * @since 1.6.4
	 */
	public void setKeyStoreResource(Resource keyStoreResource) {
		this.keyStoreResource = keyStoreResource;
	}

	/**
	 * @return the trust store resource.
	 * @since 1.5
	 */
	protected String getTrustStore() {
		return this.trustStore == null ? this.sslProperties.getProperty(TRUST_STORE) : this.trustStore;
	}

	/**
	 * Set the key store resource (e.g. file:/foo/truststore) - overrides
	 * the property in {@link #setSslPropertiesLocation(Resource)}.
	 * Ignored if {@link #setTrustStoreResource(Resource)} is called with a
	 * resource.
	 * @param trustStore the truststore resource.
	 * @since 1.5
	 */
	public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}

	protected Resource getTrustStoreResource() {
		return this.trustStoreResource;
	}

	/**
	 * Set a Resource pointing to the trust store.
	 * @param trustStoreResource the resource.
	 * @since 1.6.4
	 */
	public void setTrustStoreResource(Resource trustStoreResource) {
		this.trustStoreResource = trustStoreResource;
	}

	/**
	 * @return the key store pass phrase.
	 * @since 1.5
	 */
	protected String getKeyStorePassphrase() {
		return this.keyStorePassphrase == null ? this.sslProperties.getProperty(KEY_STORE_PASS_PHRASE)
				: this.keyStorePassphrase;
	}

	/**
	 * Set the key store pass phrase - overrides
	 * the property in {@link #setSslPropertiesLocation(Resource)}.
	 * @param keyStorePassphrase the key store pass phrase.
	 * @since 1.5
	 */
	public void setKeyStorePassphrase(String keyStorePassphrase) {
		this.keyStorePassphrase = keyStorePassphrase;
	}

	/**
	 * @return the trust store pass phrase.
	 * @since 1.5
	 */
	protected String getTrustStorePassphrase() {
		return this.trustStorePassphrase == null ? this.sslProperties.getProperty(TRUST_STORE_PASS_PHRASE)
				: this.trustStorePassphrase;
	}

	/**
	 * Set the trust store pass phrase - overrides
	 * the property in {@link #setSslPropertiesLocation(Resource)}.
	 * @param trustStorePassphrase the trust store pass phrase.
	 * @since 1.5
	 */
	public void setTrustStorePassphrase(String trustStorePassphrase) {
		this.trustStorePassphrase = trustStorePassphrase;
	}

	/**
	 * Get the key store type - this defaults to PKCS12 if not overridden by
	 * {@link #setSslPropertiesLocation(Resource)} or {@link #setKeyStoreType}
	 * @return the key store type.
	 * @since 1.6.2
	 */
	protected String getKeyStoreType() {
		if (this.keyStoreType == null && this.sslProperties.getProperty(KEY_STORE_TYPE) == null) {
			return KEY_STORE_DEFAULT_TYPE;
		}
		else if (this.keyStoreType != null) {
			return this.keyStoreType;
		}
		else {
			return this.sslProperties.getProperty(KEY_STORE_TYPE);
		}
	}

	/**
	 * Set the key store type - overrides
	 * the property in {@link #setSslPropertiesLocation(Resource)}.
	 * @param keyStoreType the key store type.
	 * @see java.security.KeyStore#getInstance(String)
	 * @since 1.6.2
	 */
	public void setKeyStoreType(String keyStoreType) {
		this.keyStoreType = keyStoreType;
	}

	/**
	 * Get the trust store type - this defaults to JKS if not overridden by
	 * {@link #setSslPropertiesLocation(Resource)} or {@link #setTrustStoreType}
	 * @return the trust store type.
	 * @since 1.6.2
	 */
	protected String getTrustStoreType() {
		if (this.trustStoreType == null && this.sslProperties.getProperty(TRUST_STORE_TYPE) == null) {
			return TRUST_STORE_DEFAULT_TYPE;
		}
		else if (this.trustStoreType != null) {
			return this.trustStoreType;
		}
		else {
			return this.sslProperties.getProperty(TRUST_STORE_TYPE);
		}
	}

	/**
	 * Set the trust store type - overrides
	 * the property in {@link #setSslPropertiesLocation(Resource)}.
	 * @param trustStoreType the trust store type.
	 * @see java.security.KeyStore#getInstance(String)
	 * @since 1.6.2
	 */
	public void setTrustStoreType(String trustStoreType) {
		this.trustStoreType = trustStoreType;
	}

	protected SecureRandom getSecureRandom() {
		return this.secureRandom;
	}

	/**
	 * Set the secure random to use when initializing the {@link SSLContext}.
	 * Defaults to null, in which case the default implementation is used.
	 * @param secureRandom the secure random.
	 * @see SSLContext#init(KeyManager[], TrustManager[], SecureRandom)
	 * @since 1.6.4
	 */
	public void setSecureRandom(SecureRandom secureRandom) {
		this.secureRandom = secureRandom;
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
	 * Add custom client properties.
	 * @param clientProperties the client properties.
	 * @see com.rabbitmq.client.ConnectionFactory#setClientProperties(java.util.Map)
	 */
	public void setClientProperties(Map<String, Object> clientProperties) {
		this.connectionFactory.getClientProperties().putAll(clientProperties);
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
		if (this.sslPropertiesLocation == null && this.keyStore == null && this.trustStore == null
				&& this.keyStoreResource == null && this.trustStoreResource == null) {
			if (this.sslAlgorithmSet) {
				this.connectionFactory.useSslProtocol(this.sslAlgorithm);
			}
			else {
				this.connectionFactory.useSslProtocol();
			}
		}
		else {
			if (this.sslPropertiesLocation != null) {
				this.sslProperties.load(this.sslPropertiesLocation.getInputStream());
			}
			PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
			String keyStoreName = getKeyStore();
			String trustStoreName = getTrustStore();
			String keyStorePassword = getKeyStorePassphrase();
			String trustStorePassword = getTrustStorePassphrase();
			String keyStoreType = getKeyStoreType();
			String trustStoreType = getTrustStoreType();
			char[] keyPassphrase = null;
			if (StringUtils.hasText(keyStorePassword)) {
				keyPassphrase = keyStorePassword.toCharArray();
			}
			char[] trustPassphrase = null;
			if (StringUtils.hasText(trustStorePassword)) {
				trustPassphrase = trustStorePassword.toCharArray();
			}
			KeyManager[] keyManagers = null;
			TrustManager[] trustManagers = null;
			if (StringUtils.hasText(keyStoreName) || this.keyStoreResource != null) {
				Resource keyStoreResource = this.keyStoreResource != null ? this.keyStoreResource
						: resolver.getResource(keyStoreName);
				KeyStore ks = KeyStore.getInstance(keyStoreType);
				ks.load(keyStoreResource.getInputStream(), keyPassphrase);
				KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
				kmf.init(ks, keyPassphrase);
				keyManagers = kmf.getKeyManagers();
			}
			if (StringUtils.hasText(trustStoreName) || this.trustStoreResource != null) {
				Resource trustStoreResource = this.trustStoreResource != null ? this.trustStoreResource
						: resolver.getResource(trustStoreName);
				KeyStore tks = KeyStore.getInstance(trustStoreType);
				tks.load(trustStoreResource.getInputStream(), trustPassphrase);
				TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(tks);
				trustManagers = tmf.getTrustManagers();
			}

			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Initializing SSLContext with KM: "
						+ Arrays.toString(keyManagers)
						+ ", TM: " + Arrays.toString(trustManagers)
						+ ", random: " + this.secureRandom);
			}
			SSLContext context = createSSLContext();
			context.init(keyManagers, trustManagers, this.secureRandom);
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
