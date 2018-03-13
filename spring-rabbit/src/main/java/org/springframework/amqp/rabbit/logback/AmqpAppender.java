/*
 * Copyright 2014-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.logback;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryConfigurationUtils;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.DeclareExchangeConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.pattern.TargetLengthBasedClassNameAbbreviator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.encoder.Encoder;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A Lockback appender that publishes logging events to an AMQP Exchange.
 * <p>
 * A fully-configured AmqpAppender, with every option set to their defaults, would look like this:
 *
 * <pre class="code">
 * {@code
 * <appender name="AMQP" class="org.springframework.amqp.rabbit.logback.AmqpAppender">
 *     <layout>
 *        <pattern><![CDATA[ %d %p %t [%c] - <%m>%n ]]></pattern>
 *     </layout>
 *     <!-- <abbreviation>36</abbreviation --> <!-- no category abbreviation by default -->
 *     <applicationId>AmqpAppenderTest</applicationId>
 *     <routingKeyPattern>%property{applicationId}.%c.%p</routingKeyPattern>
 *     <generateId>true</generateId>
 *     <charset>UTF-8</charset>
 *     <durable>false</durable>
 *     <deliveryMode>NON_PERSISTENT</deliveryMode>
 * </appender>
 * }
 * </pre>
 *
 * @author Artem Bilan
 * @author Gary Russell
 * @author Stephen Oakey
 * @author Dominique Villard
 * @author Nicolas Ristock
 *
 * @since 1.4
 */
public class AmqpAppender extends AppenderBase<ILoggingEvent> {

	/**
	 * Key name for the application id (if there is one set via the appender config) in the message properties.
	 */
	public static final String APPLICATION_ID = "applicationId";

	/**
	 * Key name for the logger category name in the message properties
	 */
	public static final String CATEGORY_NAME = "categoryName";

	/**
	 * Key name for the logger level name in the message properties
	 */
	public static final String CATEGORY_LEVEL = "level";

	/**
	 * Key name for the thread name in the message properties.
	 */
	public static final String THREAD_NAME = "thread";

	/**
	 * Name of the exchange to publish log events to.
	 */
	private String exchangeName = "logs";

	/**
	 * Type of the exchange to publish log events to.
	 */
	private String exchangeType = "topic";

	private final PatternLayout locationLayout = new PatternLayout();

	{
		this.locationLayout.setPattern("%nopex%class|%method|%line");
	}

	/**
	 * Logback Layout to use to generate routing key.
	 */
	private final PatternLayout routingKeyLayout = new PatternLayout();

	{
		this.routingKeyLayout.setPattern("%nopex%c.%p");
	}

	private final AtomicBoolean headerWritten = new AtomicBoolean();

	/**
	 * Configuration arbitrary application ID.
	 */
	private String applicationId = null;

	/**
	 * Where LoggingEvents are queued to send.
	 */
	private BlockingQueue<Event> events;

	/**
	 * The pool of senders.
	 */
	private ExecutorService senderPool = null;

	/**
	 * How many senders to use at once. Use more senders if you have lots of log output going through this appender.
	 */
	private int senderPoolSize = 2;

	/**
	 * How many times to retry sending a message if the broker is unavailable or there is some other error.
	 */
	private int maxSenderRetries = 30;

	/**
	 * Retries are delayed like: N ^ log(N), where N is the retry number.
	 */
	private final Timer retryTimer = new Timer("log-event-retry-delay", true);

	/**
	 * RabbitMQ ConnectionFactory.
	 */
	private AbstractConnectionFactory connectionFactory;

	/**
	 * Additional client connection properties added to the rabbit connection, with the form
	 * {@code key:value[,key:value]...}.
	 */
	private String clientConnectionProperties;

	/**
	 * A comma-delimited list of broker addresses: host:port[,host:port]*
	 *
	 * @since 1.5.6
	 */
	private String addresses;

	/**
	 * RabbitMQ host to connect to.
	 */
	private URI uri;

	/**
	 * RabbitMQ host to connect to.
	 */
	private String host;

	/**
	 * RabbitMQ virtual host to connect to.
	 */
	private String virtualHost;

	/**
	 * RabbitMQ port to connect to.
	 */
	private Integer port;

	/**
	 * RabbitMQ user to connect as.
	 */
	private String username;

	/**
	 * RabbitMQ password for this user.
	 */
	private String password;

	/**
	 * Use an SSL connection.
	 */
	private boolean useSsl;

	/**
	 * The SSL algorithm to use.
	 */
	private String sslAlgorithm;

	/**
	 * Location of resource containing keystore and truststore information.
	 */
	private String sslPropertiesLocation;

	/**
	 * Keystore location.
	 */
	private String keyStore;

	/**
	 * Keystore passphrase.
	 */
	private String keyStorePassphrase;

	/**
	 * Keystore type.
	 */
	private String keyStoreType = "JKS";

	/**
	 * Truststore location.
	 */
	private String trustStore;

	/**
	 * Truststore passphrase.
	 */
	private String trustStorePassphrase;

	/**
	 * Truststore type.
	 */
	private String trustStoreType = "JKS";

	/**
	 * Default content-type of log messages.
	 */
	private String contentType = "text/plain";

	/**
	 * Default content-encoding of log messages.
	 */
	private String contentEncoding = null;

	/**
	 * Whether or not to try and declare the configured exchange when this appender starts.
	 */
	private boolean declareExchange = false;

	/**
	 * charset to use when converting String to byte[], default null (system default charset used).
	 * If the charset is unsupported on the current platform, we fall back to using
	 * the system charset.
	 */
	private String charset;

	private boolean durable = true;

	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	private boolean autoDelete = false;

	/**
	 * Used to determine whether {@link MessageProperties#setMessageId(String)} is set.
	 */
	private boolean generateId = false;

	private Layout<ILoggingEvent> layout;

	private Encoder<ILoggingEvent> encoder;

	private TargetLengthBasedClassNameAbbreviator abbreviator;

	private boolean includeCallerData;

	public void setRoutingKeyPattern(String routingKeyPattern) {
		this.routingKeyLayout.setPattern("%nopex{}" + routingKeyPattern);
	}

	public URI getUri() {
		return this.uri;
	}

	public void setUri(URI uri) {
		this.uri = uri;
	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return this.port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public void setAddresses(String addresses) {
		this.addresses = addresses;
	}

	public String getAddresses() {
		return this.addresses;
	}

	public String getVirtualHost() {
		return this.virtualHost;
	}

	public void setVirtualHost(String virtualHost) {
		this.virtualHost = virtualHost;
	}

	public String getUsername() {
		return this.username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isUseSsl() {
		return this.useSsl;
	}

	public void setUseSsl(boolean ssl) {
		this.useSsl = ssl;
	}

	public String getSslAlgorithm() {
		return this.sslAlgorithm;
	}

	public void setSslAlgorithm(String sslAlgorithm) {
		this.sslAlgorithm = sslAlgorithm;
	}

	public String getSslPropertiesLocation() {
		return this.sslPropertiesLocation;
	}

	public void setSslPropertiesLocation(String sslPropertiesLocation) {
		this.sslPropertiesLocation = sslPropertiesLocation;
	}

	public String getKeyStore() {
		return this.keyStore;
	}

	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}

	public String getKeyStorePassphrase() {
		return this.keyStorePassphrase;
	}

	public void setKeyStorePassphrase(String keyStorePassphrase) {
		this.keyStorePassphrase = keyStorePassphrase;
	}

	public String getKeyStoreType() {
		return this.keyStoreType;
	}

	public void setKeyStoreType(String keyStoreType) {
		this.keyStoreType = keyStoreType;
	}

	public String getTrustStore() {
		return this.trustStore;
	}

	public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}

	public String getTrustStorePassphrase() {
		return this.trustStorePassphrase;
	}

	public void setTrustStorePassphrase(String trustStorePassphrase) {
		this.trustStorePassphrase = trustStorePassphrase;
	}

	public String getTrustStoreType() {
		return this.trustStoreType;
	}

	public void setTrustStoreType(String trustStoreType) {
		this.trustStoreType = trustStoreType;
	}

	public String getExchangeName() {
		return this.exchangeName;
	}

	public void setExchangeName(String exchangeName) {
		this.exchangeName = exchangeName;
	}

	public String getExchangeType() {
		return this.exchangeType;
	}

	public void setExchangeType(String exchangeType) {
		this.exchangeType = exchangeType;
	}

	public String getRoutingKeyPattern() {
		return this.routingKeyLayout.getPattern();
	}

	public boolean isDeclareExchange() {
		return this.declareExchange;
	}

	public void setDeclareExchange(boolean declareExchange) {
		this.declareExchange = declareExchange;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentEncoding() {
		return this.contentEncoding;
	}

	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	public String getApplicationId() {
		return this.applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public int getSenderPoolSize() {
		return this.senderPoolSize;
	}

	public void setSenderPoolSize(int senderPoolSize) {
		this.senderPoolSize = senderPoolSize;
	}

	public int getMaxSenderRetries() {
		return this.maxSenderRetries;
	}

	public void setMaxSenderRetries(int maxSenderRetries) {
		this.maxSenderRetries = maxSenderRetries;
	}

	public boolean isDurable() {
		return this.durable;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	public String getDeliveryMode() {
		return this.deliveryMode.toString();
	}

	public void setDeliveryMode(String deliveryMode) {
		this.deliveryMode = MessageDeliveryMode.valueOf(deliveryMode);
	}

	public boolean isAutoDelete() {
		return this.autoDelete;
	}

	public void setAutoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
	}

	public boolean isGenerateId() {
		return this.generateId;
	}

	public void setGenerateId(boolean generateId) {
		this.generateId = generateId;
	}

	public String getCharset() {
		return this.charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setLayout(Layout<ILoggingEvent> layout) {
		this.layout = layout;
	}

	public Encoder<ILoggingEvent> getEncoder() {
		return this.encoder;
	}

	public void setEncoder(final Encoder<ILoggingEvent> encoder) {
		this.encoder = encoder;
	}

	public void setAbbreviation(int len) {
		this.abbreviator = new TargetLengthBasedClassNameAbbreviator(len);
	}

	/**
	 * Return the number of events waiting to be sent.
	 * @return the number of events waiting to be sent.
	 */
	public int getQueuedEventCount() {
		return this.events.size();
	}

	/**
	 * Set additional client connection properties to be added to the rabbit connection,
	 * with the form {@code key:value[,key:value]...}.
	 *
	 * @param clientConnectionProperties the properties.
	 * @since 1.5.6
	 */
	public void setClientConnectionProperties(String clientConnectionProperties) {
		this.clientConnectionProperties = clientConnectionProperties;
	}

	public boolean isIncludeCallerData() {
		return this.includeCallerData;
	}

	/**
	 * If true, the caller data will be available in the target AMQP message.
	 * By default no caller data is sent to the RabbitMQ.
	 * @param includeCallerData include or on caller data
	 * @since 1.7.1
	 * @see ILoggingEvent#getCallerData()
	 */
	public void setIncludeCallerData(boolean includeCallerData) {
		this.includeCallerData = includeCallerData;
	}

	@Override
	public void start() {
		this.events = createEventQueue();

		ConnectionFactory rabbitConnectionFactory = createRabbitConnectionFactory();
		if (rabbitConnectionFactory != null) {
			super.start();
			this.routingKeyLayout.setPattern(this.routingKeyLayout.getPattern()
					.replaceAll("%property\\{applicationId\\}", this.applicationId));
			this.routingKeyLayout.setContext(getContext());
			this.routingKeyLayout.start();
			this.locationLayout.setContext(getContext());
			this.locationLayout.start();
			this.connectionFactory = new CachingConnectionFactory(rabbitConnectionFactory);
			if (this.addresses != null) {
				this.connectionFactory.setAddresses(this.addresses);
			}
			ConnectionFactoryConfigurationUtils.updateClientConnectionProperties(this.connectionFactory,
					this.clientConnectionProperties);
			updateConnectionClientProperties(this.connectionFactory.getRabbitConnectionFactory().getClientProperties());
			setUpExchangeDeclaration();
			this.senderPool = Executors.newCachedThreadPool();
			for (int i = 0; i < this.senderPoolSize; i++) {
				this.senderPool.submit(new EventSender());
			}
		}
	}

	/**
	 * Create the {@link ConnectionFactory}.
	 *
	 * @return a {@link ConnectionFactory}.
	 */
	protected ConnectionFactory createRabbitConnectionFactory() {
		RabbitConnectionFactoryBean factoryBean = new RabbitConnectionFactoryBean();
		configureRabbitConnectionFactory(factoryBean);
		try {
			factoryBean.afterPropertiesSet();
			return factoryBean.getObject();
		}
		catch (Exception e) {
			addError("Failed to create customized Rabbit ConnectionFactory.", e);
			return null;
		}
	}

	/**
	 * Configure the {@link RabbitConnectionFactoryBean}. Sub-classes may override to
	 * customize the configuration of the bean.
	 *
	 * @param factoryBean the {@link RabbitConnectionFactoryBean}.
	 */
	protected void configureRabbitConnectionFactory(RabbitConnectionFactoryBean factoryBean) {

		Optional.ofNullable(this.host).ifPresent(factoryBean::setHost);
		Optional.ofNullable(this.port).ifPresent(factoryBean::setPort);
		Optional.ofNullable(this.username).ifPresent(factoryBean::setUsername);
		Optional.ofNullable(this.password).ifPresent(factoryBean::setPassword);
		Optional.ofNullable(this.virtualHost).ifPresent(factoryBean::setVirtualHost);
		// overrides all preceding items when set
		Optional.ofNullable(this.uri).ifPresent(factoryBean::setUri);

		if (this.useSsl) {
			factoryBean.setUseSSL(true);
			if (this.sslAlgorithm != null) {
				factoryBean.setSslAlgorithm(this.sslAlgorithm);
			}
			if (this.sslPropertiesLocation != null) {
				PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
				Resource sslPropertiesResource = resolver.getResource(this.sslPropertiesLocation);
				factoryBean.setSslPropertiesLocation(sslPropertiesResource);
			}
			else {
				factoryBean.setKeyStore(this.keyStore);
				factoryBean.setKeyStorePassphrase(this.keyStorePassphrase);
				factoryBean.setKeyStoreType(this.keyStoreType);
				factoryBean.setTrustStore(this.trustStore);
				factoryBean.setTrustStorePassphrase(this.trustStorePassphrase);
				factoryBean.setTrustStoreType(this.trustStoreType);
			}
		}
		if (this.layout == null && this.encoder == null) {
			addError("Either a layout or encoder is required");
		}

		if (this.layout != null && this.encoder != null) {
			addError("Only one of layout or encoder is possible");
		}
	}

	/**
	 * Subclasses can override this method to add properties to the connection client
	 * properties.
	 *
	 * @param clientProperties the client properties.
	 * @since 1.5.6
	 */
	protected void updateConnectionClientProperties(Map<String, Object> clientProperties) {
	}

	/**
	 * Subclasses can override this method to inject a custom queue implementation.
	 *
	 * @return the queue to use for queueing logging events before processing them.
	 * @since 2.0.1
	 */
	protected BlockingQueue<Event> createEventQueue() {
		return new LinkedBlockingQueue<>();
	}

	@Override
	public void stop() {
		super.stop();
		if (null != this.senderPool) {
			this.senderPool.shutdownNow();
			this.senderPool = null;
		}
		if (null != this.connectionFactory) {
			this.connectionFactory.destroy();
		}
		this.retryTimer.cancel();
		this.routingKeyLayout.stop();
	}

	@Override
	protected void append(ILoggingEvent event) {
		if (isIncludeCallerData()) {
			event.getCallerData();
		}
		event.getThreadName();
		this.events.add(new Event(event));
	}

	protected void setUpExchangeDeclaration() {
		RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);
		if (this.declareExchange) {
			Exchange x;
			if ("topic".equals(this.exchangeType)) {
				x = new TopicExchange(this.exchangeName, this.durable, this.autoDelete);
			}
			else if ("direct".equals(this.exchangeType)) {
				x = new DirectExchange(this.exchangeName, this.durable, this.autoDelete);
			}
			else if ("fanout".equals(this.exchangeType)) {
				x = new FanoutExchange(this.exchangeName, this.durable, this.autoDelete);
			}
			else if ("headers".equals(this.exchangeType)) {
				x = new HeadersExchange(this.exchangeType, this.durable, this.autoDelete);
			}
			else {
				x = new TopicExchange(this.exchangeName, this.durable, this.autoDelete);
			}
			this.connectionFactory.addConnectionListener(new DeclareExchangeConnectionListener(x, admin));
		}
	}

	/**
	 * Subclasses may modify the final message before sending.
	 *
	 * @param message The message.
	 * @param event The event.
	 * @return The modified message.
	 * @since 1.4
	 */
	public Message postProcessMessageBeforeSend(Message message, Event event) {
		return message;
	}

	/**
	 * Helper class to actually send LoggingEvents asynchronously.
	 */
	protected class EventSender implements Runnable {

		@Override
		public void run() {
			try {
				RabbitTemplate rabbitTemplate = new RabbitTemplate(AmqpAppender.this.connectionFactory);
				while (true) {
					final Event event = AmqpAppender.this.events.take();
					ILoggingEvent logEvent = event.getEvent();

					String name = logEvent.getLoggerName();
					Level level = logEvent.getLevel();

					MessageProperties amqpProps = new MessageProperties();
					amqpProps.setDeliveryMode(AmqpAppender.this.deliveryMode);
					amqpProps.setContentType(AmqpAppender.this.contentType);
					if (null != AmqpAppender.this.contentEncoding) {
						amqpProps.setContentEncoding(AmqpAppender.this.contentEncoding);
					}
					amqpProps.setHeader(CATEGORY_NAME, name);
					amqpProps.setHeader(THREAD_NAME, logEvent.getThreadName());
					amqpProps.setHeader(CATEGORY_LEVEL, level.toString());
					if (AmqpAppender.this.generateId) {
						amqpProps.setMessageId(UUID.randomUUID().toString());
					}

					// Set timestamp
					Calendar tstamp = Calendar.getInstance();
					tstamp.setTimeInMillis(logEvent.getTimeStamp());
					amqpProps.setTimestamp(tstamp.getTime());

					// Copy properties in from MDC
					Map<String, String> props = event.getProperties();
					Set<Entry<String, String>> entrySet = props.entrySet();
					for (Entry<String, String> entry : entrySet) {
						amqpProps.setHeader(entry.getKey(), entry.getValue());
					}
					String[] location = AmqpAppender.this.locationLayout.doLayout(logEvent).split("\\|");
					if (!"?".equals(location[0])) {
						amqpProps.setHeader(
								"location",
								String.format("%s.%s()[%s]", location[0], location[1], location[2]));
					}
					byte[] msgBody;
					String routingKey = AmqpAppender.this.routingKeyLayout.doLayout(logEvent);
					// Set applicationId, if we're using one
					if (AmqpAppender.this.applicationId != null) {
						amqpProps.setAppId(AmqpAppender.this.applicationId);
					}

					if (AmqpAppender.this.encoder != null && AmqpAppender.this.headerWritten.compareAndSet(false, true)) {
						byte[] header = AmqpAppender.this.encoder.headerBytes();
						if (header != null && header.length > 0) {
							rabbitTemplate.convertAndSend(AmqpAppender.this.exchangeName, routingKey, header, m -> {
								if (AmqpAppender.this.applicationId != null) {
									m.getMessageProperties().setAppId(AmqpAppender.this.applicationId);
								}
								return m;
							});
						}
					}

					if (AmqpAppender.this.abbreviator != null && logEvent instanceof LoggingEvent) {
						((LoggingEvent) logEvent).setLoggerName(AmqpAppender.this.abbreviator.abbreviate(name));
						msgBody = encodeMessage(logEvent);
						((LoggingEvent) logEvent).setLoggerName(name);
					}
					else {
						msgBody = encodeMessage(logEvent);
					}

					// Send a message
					try {
						Message message = new Message(msgBody, amqpProps);

						message = postProcessMessageBeforeSend(message, event);
						rabbitTemplate.send(AmqpAppender.this.exchangeName, routingKey, message);
					}
					catch (AmqpException e) {
						int retries = event.incrementRetries();
						if (retries < AmqpAppender.this.maxSenderRetries) {
							// Schedule a retry based on the number of times I've tried to re-send this
							AmqpAppender.this.retryTimer.schedule(new TimerTask() {

								@Override
								public void run() {
									AmqpAppender.this.events.add(event);
								}

							}, (long) (Math.pow(retries, Math.log(retries)) * 1000));
						}
						else {
							addError("Could not send log message " + logEvent.getMessage()
									+ " after " + AmqpAppender.this.maxSenderRetries + " retries", e);
						}
					}
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		private byte[] encodeMessage(ILoggingEvent logEvent) {
			if (AmqpAppender.this.encoder != null) {
				return AmqpAppender.this.encoder.encode(logEvent);
			}

			String msgBody = AmqpAppender.this.layout.doLayout(logEvent);
			if (AmqpAppender.this.charset != null) {
				try {
					return msgBody.getBytes(AmqpAppender.this.charset);
				}
				catch (UnsupportedEncodingException e) {
					return msgBody.getBytes(); //NOSONAR (default charset)
				}
			}
			else {
				return msgBody.getBytes(); //NOSONAR (default charset)
			}
		}
	}

	/**
	 * Small helper class to encapsulate a LoggingEvent, its MDC properties, and the number of retries.
	 */
	protected static class Event {

		private final ILoggingEvent event;

		private final Map<String, String> properties;

		private final AtomicInteger retries = new AtomicInteger(0);

		public Event(ILoggingEvent event) {
			this.event = event;
			this.properties = this.event.getMDCPropertyMap();
		}

		public ILoggingEvent getEvent() {
			return this.event;
		}

		public Map<String, String> getProperties() {
			return this.properties;
		}

		public int incrementRetries() {
			return this.retries.incrementAndGet();
		}

	}

}
