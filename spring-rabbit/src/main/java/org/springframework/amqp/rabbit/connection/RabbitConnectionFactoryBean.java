/*
 * Copyright 2002-2023 the original author or authors.
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
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

import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.SocketConfigurator;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.CredentialsRefreshService;
import com.rabbitmq.client.impl.nio.NioParams;

/**
 * Factory bean to create a RabbitMQ ConnectionFactory, delegating most setter methods and
 * optionally enabling SSL, with or without certificate validation. When
 * {@link #setSslPropertiesLocation(Resource) sslPropertiesLocation} is not null, the
 * default implementation loads a {@code PKCS12} keystore and a {@code JKS} truststore
 * using the supplied properties and intializes key and trust manager factories, using
 * algorithm {@code SunX509} by default. These are then used to initialize an
 * {@link SSLContext} using the {@link #setSslAlgorithm(String) sslAlgorithm} (default
 * TLSv1.2, falling back to TLSv1.1, if 1.2 is not available).
 * <p>
 * Override {@link #createSSLContext()} to create and/or perform further modification of
 * the context.
 * <p>
 * Override {@link #setUpSSL()} to take complete control over setting up SSL.
 *
 * @author Gary Russell
 * @author Heath Abelson
 * @author Arnaud Cogoluègnes
 * @author Hareendran
 * @author Dominique Villard
 * @author Zachary DeLuca
 *
 * @since 1.4
 */
public class RabbitConnectionFactoryBean extends AbstractFactoryBean<ConnectionFactory> {

	private static final String SUN_X509 = "SunX509";

	private static final String KEY_STORE = "keyStore";

	private static final String TRUST_STORE = "trustStore";

	private static final String KEY_STORE_PASS_PHRASE = "keyStore.passPhrase";

	private static final String TRUST_STORE_PASS_PHRASE = "trustStore.passPhrase";

	private static final String KEY_STORE_TYPE = "keyStore.type";

	private static final String TRUST_STORE_TYPE = "trustStore.type";

	private static final String DEFAULT_PROTOCOL;

	static {
		String protocol = "TLSv1.1";
		try {
			String[] protocols = SSLContext.getDefault().getSupportedSSLParameters().getProtocols();
			for (String prot : protocols) {
				if ("TLSv1.2".equals(prot)) {
					protocol = "TLSv1.2";
					break;
				}
			}
		}
		catch (NoSuchAlgorithmException e) {
			// nothing
		}
		DEFAULT_PROTOCOL = protocol;
	}

	private static final String KEY_STORE_DEFAULT_TYPE = "PKCS12";

	private static final String TRUST_STORE_DEFAULT_TYPE = "JKS";

	protected final ConnectionFactory connectionFactory = new ConnectionFactory(); // NOSONAR

	private final Properties sslProperties = new Properties();

	private ResourceLoader resourceLoader = new PathMatchingResourcePatternResolver();

	private boolean useSSL;

	private Resource sslPropertiesLocation;

	private String keyStore;

	private String trustStore;

	private Resource keyStoreResource;

	private Resource trustStoreResource;

	private String keyStorePassphrase;

	private String trustStorePassphrase;

	private String keyStoreType;

	private String trustStoreType;

	private String sslAlgorithm = DEFAULT_PROTOCOL;

	private boolean sslAlgorithmSet;

	private SecureRandom secureRandom;

	private boolean skipServerCertificateValidation;

	private boolean enableHostnameVerification = true;

	private String keyStoreAlgorithm = SUN_X509;

	private String trustStoreAlgorithm = SUN_X509;

	public RabbitConnectionFactoryBean() {
		this.connectionFactory.setAutomaticRecoveryEnabled(false);
	}

	/**
	 * Whether or not Server Side certificate has to be validated or not.
	 * @return true if Server Side certificate has to be skipped
	 * @since 1.6.6
	 */
	public boolean isSkipServerCertificateValidation() {
		return this.skipServerCertificateValidation;
	}

	/**
	 * Whether or not Server Side certificate has to be validated or not.
	 * This would be used if useSSL is set to true and should only be used on dev or Qa regions
	 * skipServerCertificateValidation should <b> never be set to true in production</b>
	 * @param skipServerCertificateValidation Flag to override Server side certificate checks;
	 * if set to {@code true} {@link com.rabbitmq.client.TrustEverythingTrustManager} would be used.
	 * @since 1.6.6
	 * @see com.rabbitmq.client.TrustEverythingTrustManager
	 */
	public void setSkipServerCertificateValidation(boolean skipServerCertificateValidation) {
		this.skipServerCertificateValidation = skipServerCertificateValidation;
	}

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
	 * {@link #setSslPropertiesLocation(Resource)} or {@link #setKeyStoreType}.
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
	 * @since 1.6.2
	 * @see java.security.KeyStore#getInstance(String)
	 */
	public void setKeyStoreType(String keyStoreType) {
		this.keyStoreType = keyStoreType;
	}

	/**
	 * Get the trust store type - this defaults to JKS if not overridden by
	 * {@link #setSslPropertiesLocation(Resource)} or {@link #setTrustStoreType}.
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
	 * @since 1.6.2
	 * @see java.security.KeyStore#getInstance(String)
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
	 * @since 1.6.4
	 * @see SSLContext#init(KeyManager[], TrustManager[], SecureRandom)
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
	 * Set a credentials provider (e.g. OAUTH2).
	 * @param provider the provider.
	 * @since 2.3
	 */
	public void setCredentialsProvider(CredentialsProvider provider) {
		this.connectionFactory.setCredentialsProvider(provider);
	}

	/**
	 * Set a refresh service.
	 * @param service the service.
	 * @since 2.3
	 */
	public void setCredentialsRefreshService(CredentialsRefreshService service) {
		this.connectionFactory.setCredentialsRefreshService(service);
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
	 * @see com.rabbitmq.client.ConnectionFactory#setUri(java.net.URI)
	 */
	public void setUri(URI uri) {
		try {
			this.connectionFactory.setUri(uri);
		}
		catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
			throw new IllegalArgumentException("Unable to set uri", e);
		}
	}

	/**
	 * @param uriString the uri.
	 * @see com.rabbitmq.client.ConnectionFactory#setUri(java.lang.String)
	 */
	public void setUri(String uriString) {
		try {
			this.connectionFactory.setUri(uriString);
		}
		catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
			throw new IllegalArgumentException("Unable to set uri", e);
		}
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

	/**
	 * Whether or not the factory should be configured to use Java NIO.
	 * @param useNio true to use Java NIO, false to use blocking IO
	 * @see com.rabbitmq.client.ConnectionFactory#useNio()
	 */
	public void setUseNio(boolean useNio) {
		if (useNio) {
			this.connectionFactory.useNio();
		}
		else {
			this.connectionFactory.useBlockingIo();
		}
	}

	/**
	 * @param nioParams the NIO parameters
	 * @see com.rabbitmq.client.ConnectionFactory#setNioParams(com.rabbitmq.client.impl.nio.NioParams)
	 */
	public void setNioParams(NioParams nioParams) {
		this.connectionFactory.setNioParams(nioParams);
	}

	/**
	 * @param metricsCollector the metrics collector instance
	 * @see com.rabbitmq.client.ConnectionFactory#setMetricsCollector(MetricsCollector)
	 */
	public void setMetricsCollector(MetricsCollector metricsCollector) {
		this.connectionFactory.setMetricsCollector(metricsCollector);
	}

	/**
	 * Set to true to enable amqp-client automatic recovery. Note: Spring AMQP
	 * implements its own connection recovery and this is generally not needed.
	 * @param automaticRecoveryEnabled true to enable.
	 * @since 1.7.1
	 */
	public void setAutomaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
		this.connectionFactory.setAutomaticRecoveryEnabled(automaticRecoveryEnabled);
	}

	/**
	 * Set to true to enable amqp-client topology recovery. Note: if there is a
	 * Rabbit admin in the application context, Spring AMQP
	 * implements its own recovery and this is generally not needed.
	 * @param topologyRecoveryEnabled true to enable.
	 * @since 1.7.1
	 */
	public void setTopologyRecoveryEnabled(boolean topologyRecoveryEnabled) {
		this.connectionFactory.setTopologyRecoveryEnabled(topologyRecoveryEnabled);
	}

	/**
	 * @param channelRpcTimeout continuation timeout for RPC calls in channels
	 * @since 2.0
	 * @see com.rabbitmq.client.ConnectionFactory#setChannelRpcTimeout(int)
	 */
	public void setChannelRpcTimeout(int channelRpcTimeout) {
		this.connectionFactory.setChannelRpcTimeout(channelRpcTimeout);
	}

	/**
	 * Enable server hostname verification for TLS connections.
	 * <p>
	 * This enables hostname verification regardless of the IO mode used (blocking or
	 * non-blocking IO).
	 * <p>
	 * This can be called typically after setting the {@link SSLContext} with one of the
	 * <code>useSslProtocol</code> methods. Requires amqp-client 5.4.0 or later.
	 * @param enable false to disable.
	 * @since 2.0.6
	 * @see com.rabbitmq.client.ConnectionFactory#enableHostnameVerification()
	 */
	public void setEnableHostnameVerification(boolean enable) {
		this.enableHostnameVerification = enable;
	}

	/**
	 * Set the maximum body size of inbound (received) messages in bytes.
	 * @param maxInboundMessageBodySize the maximum size.
	 * @since 2.4.15
	 * @see com.rabbitmq.client.ConnectionFactory#setMaxInboundMessageBodySize(int)
	 */
	public void setMaxInboundMessageBodySize(int maxInboundMessageBodySize) {
		this.connectionFactory.setMaxInboundMessageBodySize(maxInboundMessageBodySize);
	}

	protected String getKeyStoreAlgorithm() {
		return this.keyStoreAlgorithm;
	}

	/**
	 * Set the algorithm used when creating the key store, default {@code SunX509}.
	 * @param keyStoreAlgorithm the algorithm.
	 * @since 2.1.6
	 */
	public void setKeyStoreAlgorithm(String keyStoreAlgorithm) {
		this.keyStoreAlgorithm = keyStoreAlgorithm;
	}

	protected String getTrustStoreAlgorithm() {
		return this.trustStoreAlgorithm;
	}

	/**
	 * Set the algorithm used when creating the trust store, default {@code SunX509}.
	 * @param trustStoreAlgorithm the algorithm.
	 * @since 2.1.6
	 */
	public void setTrustStoreAlgorithm(String trustStoreAlgorithm) {
		this.trustStoreAlgorithm = trustStoreAlgorithm;
	}

	/**
	 * Get the resource loader; used to resolve the key store and trust store {@link Resource}s
	 * to input streams.
	 * @return the resource loader.
	 * @since 2.3
	 */
	protected ResourceLoader getResourceLoader() {
		return this.resourceLoader;
	}

	/**
	 * Set the resource loader; used to resolve the key store and trust store {@link Resource}s
	 * to input streams.
	 * @param resourceLoader the resource loader.
	 * @since 2.3
	 */
	public void setResourceLoader(ResourceLoader resourceLoader) {
		Assert.notNull(resourceLoader, "'resourceLoader' cannot be null");
		this.resourceLoader = resourceLoader;
	}

	/**
	 * Access the connection factory to set any other properties not supported by
	 * this factory bean.
	 * @return the connection factory.
	 * @since 1.7.14
	 */
	public ConnectionFactory getRabbitConnectionFactory() {
		return this.connectionFactory;
	}

	@Override
	public void afterPropertiesSet() {
		try {
			super.afterPropertiesSet();
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	public Class<?> getObjectType() {
		return ConnectionFactory.class;
	}

	@Override
	protected ConnectionFactory createInstance() {
		if (this.useSSL) {
			setUpSSL();
		}
		return this.connectionFactory;
	}

	/**
	 * Override this method to take complete control over the SSL setup.
	 * @since 1.4.4
	 */
	protected void setUpSSL() {
		try {
			if (this.sslPropertiesLocation == null && this.keyStore == null && this.trustStore == null // NOSONAR boolean complexity
					&& this.keyStoreResource == null && this.trustStoreResource == null) {
				setupBasicSSL();
			}
			else {
				if (this.sslPropertiesLocation != null) {
					this.sslProperties.load(this.sslPropertiesLocation.getInputStream());
				}
				KeyManager[] keyManagers = configureKeyManagers();
				TrustManager[] trustManagers = configureTrustManagers();

				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Initializing SSLContext with KM: "
							+ Arrays.toString(keyManagers)
							+ ", TM: " + Arrays.toString(trustManagers)
							+ ", random: " + this.secureRandom);
				}
				SSLContext context = createSSLContext();
				context.init(keyManagers, trustManagers, this.secureRandom);
				this.connectionFactory.useSslProtocol(context);
				if (this.enableHostnameVerification) {
					this.connectionFactory.enableHostnameVerification();
				}
			}
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	private void setupBasicSSL() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
		if (this.skipServerCertificateValidation) {
			if (this.sslAlgorithmSet) {
				this.connectionFactory.useSslProtocol(this.sslAlgorithm);
			}
			else {
				this.connectionFactory.useSslProtocol();
			}
		}
		else {
			useDefaultTrustStoreMechanism();
		}
	}

	@Nullable
	protected KeyManager[] configureKeyManagers() throws KeyStoreException, IOException, NoSuchAlgorithmException,
			CertificateException, UnrecoverableKeyException {
		String keyStoreName = getKeyStore();
		String keyStorePassword = getKeyStorePassphrase();
		String storeType = getKeyStoreType();
		char[] keyPassphrase = null;
		if (keyStorePassword != null) {
			keyPassphrase = keyStorePassword.toCharArray();
		}
		KeyManager[] keyManagers = null;
		if (StringUtils.hasText(keyStoreName) || this.keyStoreResource != null) {
			Resource resource = this.keyStoreResource != null ? this.keyStoreResource
					: this.resourceLoader.getResource(keyStoreName);
			KeyStore ks = KeyStore.getInstance(storeType);
			try (InputStream inputStream = resource.getInputStream()) {
				ks.load(inputStream, keyPassphrase);
			}
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(this.keyStoreAlgorithm);
			kmf.init(ks, keyPassphrase);
			keyManagers = kmf.getKeyManagers();
		}
		return keyManagers;
	}

	@Nullable
	protected TrustManager[] configureTrustManagers()
			throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
		String trustStoreName = getTrustStore();
		String trustStorePassword = getTrustStorePassphrase();
		String storeType = getTrustStoreType();
		char[] trustPassphrase = null;
		if (trustStorePassword != null) {
			trustPassphrase = trustStorePassword.toCharArray();
		}
		TrustManager[] trustManagers = null;
		if (StringUtils.hasText(trustStoreName) || this.trustStoreResource != null) {
			Resource resource = this.trustStoreResource != null ? this.trustStoreResource
					: this.resourceLoader.getResource(trustStoreName);
			KeyStore tks = KeyStore.getInstance(storeType);
			try (InputStream inputStream = resource.getInputStream()) {
				tks.load(inputStream, trustPassphrase);
			}
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(this.trustStoreAlgorithm);
			tmf.init(tks);
			trustManagers = tmf.getTrustManagers();
		}
		return trustManagers;
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


	private void useDefaultTrustStoreMechanism()
			throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
		SSLContext sslContext = SSLContext.getInstance(this.sslAlgorithm);
		TrustManagerFactory trustManagerFactory =
				TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init((KeyStore) null);
		sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
		this.connectionFactory.useSslProtocol(sslContext);
		if (this.enableHostnameVerification) {
			this.connectionFactory.enableHostnameVerification();
		}
	}

}
