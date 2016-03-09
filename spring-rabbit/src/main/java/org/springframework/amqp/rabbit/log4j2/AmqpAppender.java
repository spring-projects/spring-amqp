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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

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
import org.springframework.amqp.rabbit.core.DeclareExchangeConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * A Log4j 2 appender that publishes logging events to an AMQP Exchange.
 *
 * @author Gary Russell
 *
 * @since 1.6
 */
@Plugin(name = "RabbitMQ", category = "Core", elementType = "appender", printObject = true)
public class AmqpAppender extends AbstractAppender {

	private static final long serialVersionUID = 1L;

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
	private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<Event>();

	/**
	 * Used to synchronize access to pattern layouts.
	 */
	private final Object layoutMutex = new Object();


	public AmqpAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
			AmqpManager manager) {
		super(name, filter, layout, ignoreExceptions);
		this.manager = manager;
	}

	@PluginFactory
	public static AmqpAppender createAppender(
			@PluginAttribute("name") String name,
			@PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginElement("Filter") Filter filter,
			@PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
			@PluginAttribute("host") String host,
			@PluginAttribute("port") int port,
			@PluginAttribute("user") String user,
			@PluginAttribute("password") String password,
			@PluginAttribute("virtualHost") String virtualHost,
			@PluginAttribute("senderPoolSize") int senderPoolSize,
			@PluginAttribute("maxSenderRetries") int maxSenderRetries,
			@PluginAttribute("applicationId") String applicationId,
			@PluginAttribute("routingKeyPattern") String routingKeyPattern,
			@PluginAttribute("generateId") boolean generateId,
			@PluginAttribute("deliveryMode") String deliveryMode,
			@PluginAttribute("exchange") String exchange,
			@PluginAttribute("exchangeType") String exchangeType,
			@PluginAttribute("declareExchange") boolean declareExchange,
			@PluginAttribute("durable") boolean durable,
			@PluginAttribute("autoDelete") boolean autoDelete,
			@PluginAttribute("contentType") String contentType,
			@PluginAttribute("contentEncoding") String contentEncoding,
			@PluginAttribute("charset") String charset) {
		if (name == null) {
			LogFactory.getLog("log4j2AppenderErrors").error("No name for AmqpAppender");
		}
		Layout<? extends Serializable> theLayout = layout;
		if (theLayout == null) {
			theLayout = PatternLayout.createDefaultLayout();
		}
		AmqpManager manager = new AmqpManager(name);
		manager.host = host;
		manager.port = port;
		manager.username = user;
		manager.password = password;
		manager.virtualHost = virtualHost;
		manager.senderPoolSize = senderPoolSize;
		manager.maxSenderRetries = maxSenderRetries;
		manager.applicationId = applicationId;
		manager.routingKeyPattern = routingKeyPattern;
		manager.generateId = generateId;
		manager.deliveryMode = MessageDeliveryMode.valueOf(deliveryMode);
		manager.exchangeName = exchange;
		manager.exchangeType = exchangeType;
		manager.declareExchange = declareExchange;
		manager.durable = durable;
		manager.autoDelete = autoDelete;
		manager.contentType = contentType;
		manager.contentEncoding = contentEncoding;
		manager.charset = charset;
		AmqpAppender appender = new AmqpAppender(name, filter, theLayout, ignoreExceptions, manager);
		manager.activateOptions();
		appender.startSenders();
		return appender;
	}

	/**
	 * Submit the required number of senders into the pool.
	 */
	private void startSenders() {
		for (int i = 0; i < this.manager.senderPoolSize; i++) {
			this.manager.senderPool.submit(new EventSender());
		}
	}


	@Override
	public void append(LogEvent event) {
		this.events.add(new Event(event, event.getContextMap()));
	}

	/**
	 * Subclasses may modify the final message before sending.
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
				RabbitTemplate rabbitTemplate = AmqpAppender.this.rabbitTemplate;
				rabbitTemplate.setConnectionFactory(AmqpAppender.this.manager.connectionFactory);
				while (true) {
					final Event event = AmqpAppender.this.events.take();
					LogEvent logEvent = event.getEvent();

					String name = logEvent.getLoggerName();
					Level level = logEvent.getLevel();

					MessageProperties amqpProps = new MessageProperties();
					amqpProps.setDeliveryMode(AmqpAppender.this.manager.deliveryMode);
					amqpProps.setContentType(AmqpAppender.this.manager.contentType);
					if (null != AmqpAppender.this.manager.contentEncoding) {
						amqpProps.setContentEncoding(AmqpAppender.this.manager.contentEncoding);
					}
					amqpProps.setHeader(CATEGORY_NAME, name);
					amqpProps.setHeader(CATEGORY_LEVEL, level.toString());
					if (AmqpAppender.this.manager.generateId) {
						amqpProps.setMessageId(UUID.randomUUID().toString());
					}

					// Set applicationId, if we're using one
					if (null != AmqpAppender.this.manager.applicationId) {
						amqpProps.setAppId(AmqpAppender.this.manager.applicationId);
					}

					// Set timestamp
					Calendar tstamp = Calendar.getInstance();
					tstamp.setTimeInMillis(logEvent.getTimeMillis());
					amqpProps.setTimestamp(tstamp.getTime());

					// Copy properties in from MDC
					@SuppressWarnings("rawtypes")
					Map props = event.getProperties();
					@SuppressWarnings("unchecked")
					Set<Entry<?,?>> entrySet = props.entrySet();
					for (Entry<?, ?> entry : entrySet) {
						amqpProps.setHeader(entry.getKey().toString(), entry.getValue());
					}
					if (logEvent.getSource() != null) {
						amqpProps.setHeader(
								"location",
								String.format("%s.%s()[%s]", logEvent.getSource().getClassName(),
										logEvent.getSource().getMethodName(),
										logEvent.getSource().getLineNumber()));
					}

					StringBuilder msgBody;
					String routingKey;

					// Send a message
					try {
						synchronized (AmqpAppender.this.layoutMutex) {
							msgBody = new StringBuilder(new String(getLayout().toByteArray(logEvent), "UTF-8"));
							routingKey = new String(AmqpAppender.this.manager.routingKeyLayout.toByteArray(logEvent),
									"UTF-8");
						}
						Message message = null;
						if (AmqpAppender.this.manager.charset != null) {
							try {
								message = new Message(msgBody.toString().getBytes(AmqpAppender.this.manager.charset),
										amqpProps);
							}
							catch (UnsupportedEncodingException e) {/* fall back to default */}
						}
						if (message == null) {
							message = new Message(msgBody.toString().getBytes(), amqpProps);//NOSONAR (default charset)
						}
						message = postProcessMessageBeforeSend(message, event);
						rabbitTemplate.send(AmqpAppender.this.manager.exchangeName, routingKey, message);
					}
					catch (AmqpException e) {
						int retries = event.incrementRetries();
						if (retries < AmqpAppender.this.manager.maxSenderRetries) {
							// Schedule a retry based on the number of times I've tried to re-send this
							AmqpAppender.this.manager.retryTimer.schedule(new TimerTask() {
								@Override
								public void run() {
									AmqpAppender.this.events.add(event);
								}
							}, (long) (Math.pow(retries, Math.log(retries)) * 1000));
						}
						else {
							getHandler().error("Could not send log message " + logEvent.getMessage()
									+ " after " + AmqpAppender.this.manager.maxSenderRetries + " retries", logEvent, e);
						}
					}
					catch (Exception e) {
						getHandler().error("Could not send log message " + logEvent.getMessage(), logEvent, e);
					}
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
	@SuppressWarnings("rawtypes")
	protected static class Event {

		final LogEvent event;

		final Map properties;

		final AtomicInteger retries = new AtomicInteger(0);

		public Event(LogEvent event, Map properties) {
			this.event = event;
			this.properties = properties;
		}

		public LogEvent getEvent() {
			return this.event;
		}

		public Map getProperties() {
			return this.properties;
		}

		public int incrementRetries() {
			return this.retries.incrementAndGet();
		}

	}

	private static final class AmqpManager extends AbstractManager {

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
		private Layout<String> routingKeyLayout;

		/**
		 * Configuration arbitrary application ID.
		 */
		private String applicationId = null;

		/**
		 * How many senders to use at once. Use more senders if you have lots of log output going through this appender.
		 */
		private int senderPoolSize = 2;

		/**
		 * How many times to retry sending a message if the broker is unavailable or there is some other error.
		 */
		private int maxSenderRetries = 30;

		/**
		 * RabbitMQ ConnectionFactory.
		 */
		private AbstractConnectionFactory connectionFactory;

		/**
		 * RabbitMQ host to connect to.
		 */
		private String host = "localhost";

		/**
		 * RabbitMQ virtual host to connect to.
		 */
		private String virtualHost = "/";

		/**
		 * RabbitMQ port to connect to.
		 */
		private int port = 5672;

		/**
		 * RabbitMQ user to connect as.
		 */
		private String username = "guest";

		/**
		 * RabbitMQ password for this user.
		 */
		private String password = "guest";

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
		private String charset = Charset.defaultCharset().name();

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
		private ExecutorService senderPool = null;

		/**
		 * Retries are delayed like: N ^ log(N), where N is the retry number.
		 */
		private final Timer retryTimer = new Timer("log-event-retry-delay", true);


		protected AmqpManager(String name) {
			super(name);
		}

		private final void activateOptions() {
			this.routingKeyLayout = PatternLayout.createLayout(this.routingKeyPattern
					.replaceAll("%X\\{applicationId\\}", this.applicationId),
					null, null, null, Charset.forName(this.charset), false, true, null, null);
			this.connectionFactory = new CachingConnectionFactory();
			this.connectionFactory.setHost(this.host);
			this.connectionFactory.setPort(this.port);
			this.connectionFactory.setUsername(this.username);
			this.connectionFactory.setPassword(this.password);
			this.connectionFactory.setVirtualHost(this.virtualHost);
			setUpExchangeDeclaration();
			this.senderPool = Executors.newCachedThreadPool();
		}

		@Override
		protected void releaseSub() {
			this.retryTimer.cancel();
			this.senderPool.shutdownNow();
			this.connectionFactory.destroy();
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

	}

}
