/*
 * Copyright 2016-2025 the original author or authors.
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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.apache.logging.log4j.core.async.BlockingQueueFactory;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.Integers;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpApplicationContextClosedException;
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
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.DeclareExchangeConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A Log4j 2 appender that publishes logging events to an AMQP Exchange.
 *
 * @author Gary Russell
 * @author Stephen Oakey
 * @author Artem Bilan
 * @author Dominique Villard
 * @author Nicolas Ristock
 * @author Eugene Gusev
 * @author Francesco Scipioni
 *
 * @since 1.6
 */
@Plugin(name = "RabbitMQ", category = "Core", elementType = "appender", printObject = true)
public class AmqpAppender extends AbstractAppender {

	/**
	 * Key name for the application id (if there is one set via the appender config) in the message properties.
	 */
	public static final String APPLICATION_ID = "applicationId";

	/**
	 * Key name for the logger category name in the message properties.
	 */
	public static final String CATEGORY_NAME = "categoryName";

	/**
	 * Key name for the logger level name in the message properties.
	 */
	public static final String CATEGORY_LEVEL = "level";

	/**
	 * Key name for the thread name in the message properties.
	 */
	public static final String THREAD_NAME = "thread";

	/**
	 * Tha manager.
	 */
	private final AmqpManager manager;

	/**
	 * The template.
	 */
	private final RabbitTemplate rabbitTemplate = new RabbitTemplate();

	/**
	 * Where LoggingEvents are queued to send.
	 */
	private final BlockingQueue<Event> events;

	/**
	 * Used to synchronize access to pattern layouts.
	 */
	private final Lock layoutMutex = new ReentrantLock();

	/**
	 * Construct an instance with the provided properties.
	 * @param name the name.
	 * @param filter the filter.
	 * @param layout the layout.
	 * @param ignoreExceptions true to ignore exceptions.
	 * @param manager the manager.
	 * @param eventQueue the event queue.
	 * @param properties the properties.
	 */
	public AmqpAppender(String name, @Nullable Filter filter, Layout<? extends Serializable> layout,
			boolean ignoreExceptions, Property[] properties, AmqpManager manager, BlockingQueue<Event> eventQueue) {

		super(name, filter, layout, ignoreExceptions, properties);
		this.manager = manager;
		this.events = eventQueue;
	}

	/**
	 * Create a new builder.
	 * @return the builder.
	 */
	@PluginBuilderFactory
	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Submit the required number of senders into the pool.
	 */
	private void startSenders() {
		this.rabbitTemplate.setConnectionFactory(this.manager.connectionFactory);
		if (this.manager.async) {
			for (int i = 0; i < this.manager.senderPoolSize; i++) {
				this.manager.senderPool.submit(new EventSender());
			}
		}
		else if (this.manager.maxSenderRetries > 0) {
			RetryTemplate retryTemplate = new RetryTemplate();
			RetryPolicy retryPolicy = new SimpleRetryPolicy(this.manager.maxSenderRetries);
			retryTemplate.setRetryPolicy(retryPolicy);
			this.rabbitTemplate.setRetryTemplate(retryTemplate);
		}
	}

	@Override
	public void append(LogEvent event) {
		Event appenderEvent = new Event(event, event.getContextData().toMap());
		if (this.manager.async) {
			this.events.add(appenderEvent);
		}
		else {
			sendEvent(appenderEvent, appenderEvent.getProperties());
		}
	}

	/**
	 * Subclasses may modify the final message before sending.
	 *
	 * @param message The message.
	 * @param event The event.
	 * @return The modified message.
	 */
	protected Message postProcessMessageBeforeSend(Message message, Event event) {
		return message;
	}

	protected void sendEvent(Event event, Map<?, ?> properties) {
		LogEvent logEvent = event.getEvent();
		String name = logEvent.getLoggerName();
		Level level = logEvent.getLevel();

		MessageProperties amqpProps = new MessageProperties();
		JavaUtils.INSTANCE
			.acceptIfNotNull(this.manager.deliveryMode, amqpProps::setDeliveryMode)
			.acceptIfNotNull(this.manager.contentType, amqpProps::setContentType)
			.acceptIfNotNull(this.manager.contentEncoding, amqpProps::setContentEncoding);
		amqpProps.setHeader(CATEGORY_NAME, name);
		amqpProps.setHeader(THREAD_NAME, logEvent.getThreadName());
		amqpProps.setHeader(CATEGORY_LEVEL, level.toString());
		if (this.manager.generateId) {
			amqpProps.setMessageId(UUID.randomUUID().toString());
		}

		// Set applicationId, if we're using one
		if (this.manager.applicationId != null) {
			amqpProps.setAppId(this.manager.applicationId);
		}

		// Set timestamp
		Calendar tstamp = Calendar.getInstance();
		tstamp.setTimeInMillis(logEvent.getTimeMillis());
		amqpProps.setTimestamp(tstamp.getTime());

		// Copy properties in from MDC
		if (this.manager.addMdcAsHeaders) {
			for (Entry<?, ?> entry : properties.entrySet()) {
				amqpProps.setHeader(entry.getKey().toString(), entry.getValue());
			}
		}
		if (logEvent.getSource() != null) {
			amqpProps.setHeader(
					"location",
					String.format("%s.%s()[%s]", logEvent.getSource().getClassName(),
							logEvent.getSource().getMethodName(),
							logEvent.getSource().getLineNumber()));
		}

		doSend(event, logEvent, amqpProps);
	}

	protected void doSend(Event event, LogEvent logEvent, MessageProperties amqpProps) {
		StringBuilder msgBody;
		String routingKey;
		try {
			this.layoutMutex.lock();
			try {
				msgBody = new StringBuilder(new String(getLayout().toByteArray(logEvent), StandardCharsets.UTF_8));
				routingKey = new String(this.manager.routingKeyLayout.toByteArray(logEvent), StandardCharsets.UTF_8);
			}
			finally {
				this.layoutMutex.unlock();
			}
			Message message = null;
			try {
				message = new Message(msgBody.toString().getBytes(this.manager.charset), amqpProps);
			}
			catch (UnsupportedEncodingException e) {
				/* fall back to default */
			}
			if (message == null) {
				message = new Message(msgBody.toString().getBytes(), amqpProps); //NOSONAR (default charset)
			}
			message = postProcessMessageBeforeSend(message, event);
			this.rabbitTemplate.send(this.manager.exchangeName, routingKey, message);
		}
		catch (AmqpApplicationContextClosedException e) {
			getHandler().error("Could not send log message " + logEvent.getMessage() + " appender is stopped");
		}
		catch (AmqpException e) {
			int retries = event.incrementRetries();
			if (this.manager.async && retries < this.manager.maxSenderRetries) {
				// Schedule a retry based on the number of times I've tried to re-send this
				this.manager.retryTimer.schedule(new TimerTask() {

					@Override
					public void run() {
						AmqpAppender.this.events.add(event);
					}

				}, (long) (Math.pow(retries, Math.log(retries)) * 1000)); // NOSONAR - magic #
			}
			else {
				getHandler().error("Could not send log message " + logEvent.getMessage()
						+ " after " + this.manager.maxSenderRetries + " retries", logEvent, e);
			}
		}
		catch (Exception e) {
			getHandler().error("Could not send log message " + logEvent.getMessage(), logEvent, e);
		}
	}

	@Override
	protected boolean stop(long timeout, TimeUnit timeUnit, boolean changeLifeCycleState) {
		boolean stopped = super.stop(timeout, timeUnit, changeLifeCycleState);
		return this.manager.stop(timeout, timeUnit) || stopped;
	}

	/**
	 * Return the number of events waiting to be sent.
	 * @return the number of events waiting to be sent.
	 */
	public int getQueuedEventCount() {
		return this.events.size();
	}

	/**
	 * Helper class to actually send LoggingEvents asynchronously.
	 */
	protected class EventSender implements Runnable {

		@Override
		public void run() {
			try {
				// We leave the loop when thread is interrupted by the ExecutorService.shutdownNow()
				while (true) {
					final Event event = AmqpAppender.this.events.take();
					sendEvent(event, event.getProperties());
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

	}

	/**
	 * Helper class to encapsulate a LoggingEvent, its MDC properties, and the number of retries.
	 */
	public static class Event {

		private final LogEvent event;

		private final Map<?, ?> properties;

		private final AtomicInteger retries = new AtomicInteger(0);

		public Event(LogEvent event, Map<?, ?> properties) {
			this.event = event;
			this.properties = properties;
		}

		public LogEvent getEvent() {
			return this.event;
		}

		public Map<?, ?> getProperties() {
			return this.properties;
		}

		public int incrementRetries() {
			return this.retries.incrementAndGet();
		}

	}

	/**
	 * Manager class for the appender.
	 */
	public static class AmqpManager extends AbstractManager {

		private static final int DEFAULT_MAX_SENDER_RETRIES = 30;

		private final ApplicationContext context = new GenericApplicationContext();

		/**
		 * True to send events on separate threads.
		 */
		private boolean async;

		/**
		 * Name of the exchange to publish log events to.
		 */
		private String exchangeName = "logs";

		/**
		 * Type of the exchange to publish log events to.
		 */
		private String exchangeType = "topic";

		/**
		 * Log4J pattern format to use to generate a routing key.
		 */
		private String routingKeyPattern = "%c.%p";

		/**
		 * Log4J Layout to use to generate routing key.
		 */
		@SuppressWarnings("NullAway.Init")
		private Layout<String> routingKeyLayout;

		/**
		 * Configuration arbitrary application ID.
		 */
		private @Nullable String applicationId;

		/**
		 * How many senders to use at once. Use more senders if you have lots of log output going through this appender.
		 */
		private int senderPoolSize = 2;

		/**
		 * How many times to retry sending a message if the broker is unavailable or there is some other error.
		 */
		private int maxSenderRetries = DEFAULT_MAX_SENDER_RETRIES;

		/**
		 * RabbitMQ ConnectionFactory.
		 */
		@SuppressWarnings("NullAway.Init")
		private AbstractConnectionFactory connectionFactory;

		/**
		 * RabbitMQ host to connect to.
		 */
		private @Nullable URI uri;

		/**
		 * RabbitMQ host to connect to.
		 */
		private @Nullable String host;

		/**
		 * A comma-delimited list of broker addresses: host:port[,host:port]*.
		 */
		private @Nullable String addresses;

		/**
		 * RabbitMQ virtual host to connect to.
		 */
		private @Nullable String virtualHost;

		/**
		 * RabbitMQ port to connect to.
		 */
		private @Nullable Integer port;

		/**
		 * RabbitMQ user to connect as.
		 */
		private @Nullable String username;

		/**
		 * RabbitMQ password for this user.
		 */
		private @Nullable String password;

		/**
		 * Use an SSL connection.
		 */
		private boolean useSsl;

		private boolean verifyHostname = true;

		/**
		 * The SSL algorithm to use.
		 */
		private @Nullable String sslAlgorithm;

		/**
		 * Location of resource containing keystore and truststore information.
		 */
		private @Nullable String sslPropertiesLocation;

		/**
		 * Keystore location.
		 */
		private @Nullable String keyStore;

		/**
		 * Keystore passphrase.
		 */
		private @Nullable String keyStorePassphrase;

		/**
		 * Keystore type.
		 */
		private String keyStoreType = "JKS";

		/**
		 * Truststore location.
		 */
		private @Nullable String trustStore;

		/**
		 * Truststore passphrase.
		 */
		private @Nullable String trustStorePassphrase;

		/**
		 * Truststore type.
		 */
		private String trustStoreType = "JKS";

		/**
		 * SaslConfig.
		 * @see RabbitUtils#stringToSaslConfig(String, ConnectionFactory)
		 */
		private @Nullable String saslConfig;

		/**
		 * Default content-type of log messages.
		 */
		private String contentType = "text/plain";

		/**
		 * Default content-encoding of log messages.
		 */
		private @Nullable String contentEncoding;

		/**
		 * Whether to try and declare the configured exchange when this appender starts.
		 */
		private boolean declareExchange = false;

		/**
		 * A name for the connection (appears on the RabbitMQ Admin UI).
		 */
		private @Nullable String connectionName;

		/**
		 * Additional client connection properties to be added to the rabbit connection,
		 * with the form {@code key:value[,key:value]...}.
		 */
		private @Nullable String clientConnectionProperties;

		/**
		 * charset to use when converting String to byte[], default null (system default charset used).
		 * If the charset is unsupported on the current platform, we fall back to using
		 * the system charset.
		 */
		private String charset = Charset.defaultCharset().name();

		/**
		 * Whether add MDC properties into message headers. true by default for backward compatibility
		 */
		private boolean addMdcAsHeaders = true;

		private boolean durable = true;

		private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

		private boolean autoDelete = false;

		/**
		 * Used to determine whether {@link MessageProperties#setMessageId(String)} is set.
		 */
		private boolean generateId = false;

		/**
		 * The pool of senders.
		 */
		@SuppressWarnings("NullAway.Init")
		private ExecutorService senderPool;

		/**
		 * Retries are delayed like: N ^ log(N), where N is the retry number.
		 */
		private final Timer retryTimer = new Timer("log-event-retry-delay", true);

		protected AmqpManager(LoggerContext loggerContext, String name) {
			super(loggerContext, name);
		}

		@SuppressWarnings("NullAway") // Dataflow analysis limitation
		private boolean activateOptions() {
			ConnectionFactory rabbitConnectionFactory = createRabbitConnectionFactory();
			if (rabbitConnectionFactory != null) {
				Assert.state(this.applicationId != null, "applicationId is required");
				this.routingKeyLayout = PatternLayout.newBuilder()
						.withPattern(this.routingKeyPattern.replaceAll("%X\\{applicationId}", this.applicationId))
						.withCharset(Charset.forName(this.charset))
						.withAlwaysWriteExceptions(false)
						.withNoConsoleNoAnsi(true)
						.build();
				this.connectionFactory = new CachingConnectionFactory(rabbitConnectionFactory);
				this.connectionFactory.setApplicationContext(this.context);
				if (StringUtils.hasText(this.connectionName)) {
					this.connectionFactory.setConnectionNameStrategy(cf -> this.connectionName);
				}
				if (this.addresses != null) {
					this.connectionFactory.setAddresses(this.addresses);
				}
				ConnectionFactoryConfigurationUtils.updateClientConnectionProperties(this.connectionFactory,
							this.clientConnectionProperties);
				setUpExchangeDeclaration();
				this.senderPool = Executors.newCachedThreadPool();
				return true;
			}
			return false;
		}

		/**
		 * Create the {@link ConnectionFactory}.
		 * @return a {@link ConnectionFactory}.
		 */
		protected @Nullable ConnectionFactory createRabbitConnectionFactory() {
			RabbitConnectionFactoryBean factoryBean = new RabbitConnectionFactoryBean();
			configureRabbitConnectionFactory(factoryBean);
			try {
				factoryBean.afterPropertiesSet();
				return factoryBean.getObject();
			}
			catch (Exception e) {
				LOGGER.error("Failed to create customized Rabbit ConnectionFactory.",
						e);
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
			JavaUtils.INSTANCE
					.acceptIfNotNull(this.host, factoryBean::setHost)
					.acceptIfNotNull(this.port, factoryBean::setPort)
					.acceptIfNotNull(this.username, factoryBean::setUsername)
					.acceptIfNotNull(this.password, factoryBean::setPassword)
					.acceptIfNotNull(this.virtualHost, factoryBean::setVirtualHost)
					// overrides all preceding items when set
					.acceptIfNotNull(this.uri, factoryBean::setUri);

			if (this.useSsl) {
				factoryBean.setUseSSL(true);
				factoryBean.setEnableHostnameVerification(this.verifyHostname);
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
					JavaUtils.INSTANCE
							.acceptIfNotNull(this.saslConfig, config -> {
								try {
									factoryBean.setSaslConfig(RabbitUtils.stringToSaslConfig(config,
											factoryBean.getRabbitConnectionFactory()));
								}
								catch (Exception e) {
									throw RabbitExceptionTranslator.convertRabbitAccessException(e);
								}
							});
				}
			}
		}

		@Override
		protected boolean releaseSub(long timeout, TimeUnit timeUnit) {
			this.retryTimer.cancel();
			this.senderPool.shutdownNow();
			this.connectionFactory.destroy();
			this.connectionFactory.onApplicationEvent(new ContextClosedEvent(this.context));
			try {
				return this.senderPool.awaitTermination(timeout, timeUnit);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}

		protected void setUpExchangeDeclaration() {
			RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);
			if (this.declareExchange) {
				Exchange x = switch (this.exchangeType) {
					case "direct" -> new DirectExchange(this.exchangeName, this.durable, this.autoDelete);
					case "fanout" -> new FanoutExchange(this.exchangeName, this.durable, this.autoDelete);
					case "headers" -> new HeadersExchange(this.exchangeName, this.durable, this.autoDelete);
					default -> new TopicExchange(this.exchangeName, this.durable, this.autoDelete);
				};
				this.connectionFactory.addConnectionListener(new DeclareExchangeConnectionListener(x, admin));
			}
		}

	}

	public static class Builder implements org.apache.logging.log4j.core.util.Builder<AmqpAppender> {

		@PluginConfiguration
		@SuppressWarnings("NullAway.Init")
		private Configuration configuration;

		@PluginBuilderAttribute("name")
		private @Nullable String name;

		@PluginElement("Layout")
		private @Nullable Layout<? extends Serializable> layout;

		@PluginElement("Filter")
		private @Nullable Filter filter;

		@PluginBuilderAttribute("ignoreExceptions")
		private boolean ignoreExceptions;

		@PluginBuilderAttribute("uri")
		private @Nullable URI uri;

		@PluginBuilderAttribute("host")
		private @Nullable String host;

		@PluginBuilderAttribute("port")
		private @Nullable String port;

		@PluginBuilderAttribute("addresses")
		private @Nullable String addresses;

		@PluginBuilderAttribute("user")
		private @Nullable String user;

		@PluginBuilderAttribute("password")
		private @Nullable String password;

		@PluginBuilderAttribute("virtualHost")
		private @Nullable String virtualHost;

		@PluginBuilderAttribute("useSsl")
		private boolean useSsl;

		@PluginBuilderAttribute("verifyHostname")
		private boolean verifyHostname;

		@PluginBuilderAttribute("sslAlgorithm")
		private @Nullable String sslAlgorithm;

		@PluginBuilderAttribute("sslPropertiesLocation")
		private @Nullable String sslPropertiesLocation;

		@PluginBuilderAttribute("keyStore")
		private @Nullable String keyStore;

		@PluginBuilderAttribute("keyStorePassphrase")
		private @Nullable String keyStorePassphrase;

		@PluginBuilderAttribute("keyStoreType")
		private @Nullable String keyStoreType;

		@PluginBuilderAttribute("trustStore")
		private @Nullable String trustStore;

		@PluginBuilderAttribute("trustStorePassphrase")
		private @Nullable String trustStorePassphrase;

		@PluginBuilderAttribute("trustStoreType")
		private @Nullable String trustStoreType;

		@PluginBuilderAttribute("saslConfig")
		private @Nullable String saslConfig;

		@PluginBuilderAttribute("senderPoolSize")
		private int senderPoolSize;

		@PluginBuilderAttribute("maxSenderRetries")
		private int maxSenderRetries;

		@PluginBuilderAttribute("applicationId")
		private @Nullable String applicationId;

		@PluginBuilderAttribute("routingKeyPattern")
		private @Nullable String routingKeyPattern;

		@PluginBuilderAttribute("generateId")
		private boolean generateId;

		@PluginBuilderAttribute("deliveryMode")
		private @Nullable String deliveryMode;

		@PluginBuilderAttribute("exchange")
		private @Nullable String exchange;

		@PluginBuilderAttribute("exchangeType")
		private @Nullable String exchangeType;

		@PluginBuilderAttribute("declareExchange")
		private boolean declareExchange;

		@PluginBuilderAttribute("durable")
		private boolean durable;

		@PluginBuilderAttribute("autoDelete")
		private boolean autoDelete;

		@PluginBuilderAttribute("contentType")
		private @Nullable String contentType;

		@PluginBuilderAttribute("contentEncoding")
		private @Nullable String contentEncoding;

		@PluginBuilderAttribute("connectionName")
		private @Nullable String connectionName;

		@PluginBuilderAttribute("clientConnectionProperties")
		private @Nullable String clientConnectionProperties;

		@PluginBuilderAttribute("async")
		private boolean async;

		@PluginBuilderAttribute("charset")
		private @Nullable String charset;

		@PluginBuilderAttribute("bufferSize")
		private int bufferSize = Integer.MAX_VALUE;

		@PluginElement(BlockingQueueFactory.ELEMENT_TYPE)
		private @Nullable BlockingQueueFactory<Event> blockingQueueFactory;

		@PluginBuilderAttribute("addMdcAsHeaders")
		private boolean addMdcAsHeaders = Boolean.TRUE;

		public Builder setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}

		public Builder setName(String name) {
			this.name = name;
			return this;
		}

		public Builder setLayout(Layout<? extends Serializable> layout) {
			this.layout = layout;
			return this;
		}

		public Builder setFilter(Filter filter) {
			this.filter = filter;
			return this;
		}

		public Builder setIgnoreExceptions(boolean ignoreExceptions) {
			this.ignoreExceptions = ignoreExceptions;
			return this;
		}

		public Builder setUri(URI uri) {
			this.uri = uri;
			return this;
		}

		public Builder setHost(String host) {
			this.host = host;
			return this;
		}

		public Builder setPort(String port) {
			this.port = port;
			return this;
		}

		public Builder setAddresses(String addresses) {
			this.addresses = addresses;
			return this;
		}

		public Builder setUser(String user) {
			this.user = user;
			return this;
		}

		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		public Builder setVirtualHost(String virtualHost) {
			this.virtualHost = virtualHost;
			return this;
		}

		public Builder setUseSsl(boolean useSsl) {
			this.useSsl = useSsl;
			return this;
		}

		public Builder setVerifyHostname(boolean verifyHostname) {
			this.verifyHostname = verifyHostname;
			return this;
		}

		public Builder setSslAlgorithm(String sslAlgorithm) {
			this.sslAlgorithm = sslAlgorithm;
			return this;
		}

		public Builder setSslPropertiesLocation(String sslPropertiesLocation) {
			this.sslPropertiesLocation = sslPropertiesLocation;
			return this;
		}

		public Builder setKeyStore(String keyStore) {
			this.keyStore = keyStore;
			return this;
		}

		public Builder setKeyStorePassphrase(String keyStorePassphrase) {
			this.keyStorePassphrase = keyStorePassphrase;
			return this;
		}

		public Builder setKeyStoreType(String keyStoreType) {
			this.keyStoreType = keyStoreType;
			return this;
		}

		public Builder setTrustStore(String trustStore) {
			this.trustStore = trustStore;
			return this;
		}

		public Builder setTrustStorePassphrase(String trustStorePassphrase) {
			this.trustStorePassphrase = trustStorePassphrase;
			return this;
		}

		public Builder setTrustStoreType(String trustStoreType) {
			this.trustStoreType = trustStoreType;
			return this;
		}

		public Builder setSaslConfig(String saslConfig) {
			this.saslConfig = saslConfig;
			return this;
		}

		public Builder setSenderPoolSize(int senderPoolSize) {
			this.senderPoolSize = senderPoolSize;
			return this;
		}

		public Builder setMaxSenderRetries(int maxSenderRetries) {
			this.maxSenderRetries = maxSenderRetries;
			return this;
		}

		public Builder setApplicationId(String applicationId) {
			this.applicationId = applicationId;
			return this;
		}

		public Builder setRoutingKeyPattern(String routingKeyPattern) {
			this.routingKeyPattern = routingKeyPattern;
			return this;
		}

		public Builder setGenerateId(boolean generateId) {
			this.generateId = generateId;
			return this;
		}

		public Builder setDeliveryMode(String deliveryMode) {
			this.deliveryMode = deliveryMode;
			return this;
		}

		public Builder setExchange(String exchange) {
			this.exchange = exchange;
			return this;
		}

		public Builder setExchangeType(String exchangeType) {
			this.exchangeType = exchangeType;
			return this;
		}

		public Builder setDeclareExchange(boolean declareExchange) {
			this.declareExchange = declareExchange;
			return this;
		}

		public Builder setDurable(boolean durable) {
			this.durable = durable;
			return this;
		}

		public Builder setAutoDelete(boolean autoDelete) {
			this.autoDelete = autoDelete;
			return this;
		}

		public Builder setContentType(String contentType) {
			this.contentType = contentType;
			return this;
		}

		public Builder setContentEncoding(String contentEncoding) {
			this.contentEncoding = contentEncoding;
			return this;
		}

		public Builder setConnectionName(String connectionName) {
			this.connectionName = connectionName;
			return this;
		}

		public Builder setClientConnectionProperties(String clientConnectionProperties) {
			this.clientConnectionProperties = clientConnectionProperties;
			return this;
		}

		public Builder setAsync(boolean async) {
			this.async = async;
			return this;
		}

		public Builder setCharset(String charset) {
			this.charset = charset;
			return this;
		}

		public Builder setBufferSize(int bufferSize) {
			this.bufferSize = bufferSize;
			return this;
		}

		public Builder setBlockingQueueFactory(BlockingQueueFactory<Event> blockingQueueFactory) {
			this.blockingQueueFactory = blockingQueueFactory;
			return this;
		}

		public Builder setAddMdcAsHeaders(boolean addMdcAsHeaders) {
			this.addMdcAsHeaders = addMdcAsHeaders;
			return this;
		}

		@Override
		@SuppressWarnings("NullAway") // Dataflow analysis limitation
		public @Nullable AmqpAppender build() {
			if (this.name == null) {
				LOGGER.error("No name for AmqpAppender");
			}
			Layout<? extends Serializable> theLayout = this.layout;
			if (theLayout == null) {
				theLayout = PatternLayout.createDefaultLayout();
			}
			AmqpManager manager = new AmqpManager(this.configuration.getLoggerContext(), this.name);
			JavaUtils.INSTANCE
				.acceptIfNotNull(this.uri, value -> manager.uri = value)
				.acceptIfNotNull(this.host, value -> manager.host = value)
				.acceptIfNotNull(this.port, value -> manager.port = Integers.parseInt(value))
				.acceptIfNotNull(this.addresses, value -> manager.addresses = value)
				.acceptIfNotNull(this.user, value -> manager.username = value)
				.acceptIfNotNull(this.password, value -> manager.password = value)
				.acceptIfNotNull(this.virtualHost, value -> manager.virtualHost = value)
				.acceptIfNotNull(this.useSsl, value -> manager.useSsl = value)
				.acceptIfNotNull(this.verifyHostname, value -> manager.verifyHostname = value)
				.acceptIfNotNull(this.sslAlgorithm, value -> manager.sslAlgorithm = value)
				.acceptIfNotNull(this.sslPropertiesLocation, value -> manager.sslPropertiesLocation = value)
				.acceptIfNotNull(this.keyStore, value -> manager.keyStore = value)
				.acceptIfNotNull(this.keyStorePassphrase, value -> manager.keyStorePassphrase = value)
				.acceptIfNotNull(this.keyStoreType, value -> manager.keyStoreType = value)
				.acceptIfNotNull(this.trustStore, value -> manager.trustStore = value)
				.acceptIfNotNull(this.trustStorePassphrase, value -> manager.trustStorePassphrase = value)
				.acceptIfNotNull(this.trustStoreType, value -> manager.trustStoreType = value)
				.acceptIfNotNull(this.saslConfig, value -> manager.saslConfig = value)
				.acceptIfNotNull(this.senderPoolSize, value -> manager.senderPoolSize = value)
				.acceptIfNotNull(this.maxSenderRetries, value -> manager.maxSenderRetries = value)
				.acceptIfNotNull(this.applicationId, value -> manager.applicationId = value)
				.acceptIfNotNull(this.routingKeyPattern, value -> manager.routingKeyPattern = value)
				.acceptIfNotNull(this.generateId, value -> manager.generateId = value)
				.acceptIfNotNull(this.deliveryMode,
						value -> manager.deliveryMode = MessageDeliveryMode.valueOf(this.deliveryMode))
				.acceptIfNotNull(this.exchange, value -> manager.exchangeName = value)
				.acceptIfNotNull(this.exchangeType, value -> manager.exchangeType = value)
				.acceptIfNotNull(this.declareExchange, value -> manager.declareExchange = value)
				.acceptIfNotNull(this.durable, value -> manager.durable = value)
				.acceptIfNotNull(this.autoDelete, value -> manager.autoDelete = value)
				.acceptIfNotNull(this.contentType, value -> manager.contentType = value)
				.acceptIfNotNull(this.contentEncoding, value -> manager.contentEncoding = value)
				.acceptIfNotNull(this.connectionName, value -> manager.connectionName = value)
				.acceptIfNotNull(this.clientConnectionProperties, value -> manager.clientConnectionProperties = value)
				.acceptIfNotNull(this.charset, value -> manager.charset = value)
				.acceptIfNotNull(this.async, value -> manager.async = value)
				.acceptIfNotNull(this.addMdcAsHeaders, value -> manager.addMdcAsHeaders = value);

			BlockingQueue<Event> eventQueue;
			if (this.blockingQueueFactory == null) {
				eventQueue = new LinkedBlockingQueue<>(this.bufferSize);
			}
			else {
				eventQueue = this.blockingQueueFactory.create(this.bufferSize);
			}

			AmqpAppender appender =
					buildInstance(this.name, this.filter, theLayout, this.ignoreExceptions, manager, eventQueue);
			if (manager.activateOptions()) {
				appender.startSenders();
				return appender;
			}
			return null;
		}

		/**
		 * Subclasses can extend Builder, use same logic but need to modify class instance.
		 *
		 * @param name The Appender name.
		 * @param filter The Filter to associate with the Appender.
		 * @param layout The layout to use to format the event.
		 * @param ignoreExceptions If true, exceptions will be logged and suppressed. If false errors will be logged and
		 *            then passed to the application.
		 * @param manager Manager class for the appender.
		 * @param eventQueue Where LoggingEvents are queued to send.
		 * @return {@link AmqpAppender}
		 */
		protected AmqpAppender buildInstance(String name, @Nullable Filter filter, Layout<? extends Serializable> layout,
				boolean ignoreExceptions, AmqpManager manager, BlockingQueue<Event> eventQueue) {

			return new AmqpAppender(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY, manager, eventQueue);
		}

	}

}
