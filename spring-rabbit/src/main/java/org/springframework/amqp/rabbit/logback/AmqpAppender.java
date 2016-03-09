/*
 * Copyright 2014-2016 the original author or authors.
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.pattern.TargetLengthBasedClassNameAbbreviator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;

/**
 * A Lockback appender that publishes logging events to an AMQP Exchange.
 * <p>
 * A fully-configured AmqpAppender, with every option set to their defaults, would look like this:
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

	/**
	 * Configuration arbitrary application ID.
	 */
	private String applicationId = null;

	/**
	 * Where LoggingEvents are queued to send.
	 */
	private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<Event>();

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
	private String charset;

	private boolean durable = true;

	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	private boolean autoDelete = false;

	/**
	 * Used to determine whether {@link MessageProperties#setMessageId(String)} is set.
	 */
	private boolean generateId = false;

	private Layout<ILoggingEvent> layout;

	private TargetLengthBasedClassNameAbbreviator abbreviator;

	public void setRoutingKeyPattern(String routingKeyPattern) {
		this.routingKeyLayout.setPattern("%nopex{}" + routingKeyPattern);
	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return this.port;
	}

	public void setPort(int port) {
		this.port = port;
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

	public void setAbbreviation(int len) {
		this.abbreviator = new TargetLengthBasedClassNameAbbreviator(len);
	}

	@Override
	public void start() {
		super.start();
		this.routingKeyLayout.setPattern(this.routingKeyLayout.getPattern()
				.replaceAll("%property\\{applicationId\\}", this.applicationId));
		this.routingKeyLayout.setContext(getContext());
		this.routingKeyLayout.start();
		this.locationLayout.setContext(getContext());
		this.locationLayout.start();
		this.connectionFactory = new CachingConnectionFactory();
		this.connectionFactory.setHost(this.host);
		this.connectionFactory.setPort(this.port);
		this.connectionFactory.setUsername(this.username);
		this.connectionFactory.setPassword(this.password);
		this.connectionFactory.setVirtualHost(this.virtualHost);
		setUpExchangeDeclaration();
		this.senderPool = Executors.newCachedThreadPool();
		for (int i = 0; i < this.senderPoolSize; i++) {
			this.senderPool.submit(new EventSender());
		}
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
		this.events.add(new Event(event));
	}

	/**
	 * @deprecated - use {@link #setUpExchangeDeclaration()}
	 */
	@Deprecated
	protected void maybeDeclareExchange() {
		setUpExchangeDeclaration();
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
					String msgBody;
					String routingKey = AmqpAppender.this.routingKeyLayout.doLayout(logEvent);
					// Set applicationId, if we're using one
					if (AmqpAppender.this.applicationId != null) {
						amqpProps.setAppId(AmqpAppender.this.applicationId);
					}

					if (AmqpAppender.this.abbreviator != null && logEvent instanceof LoggingEvent) {
						((LoggingEvent) logEvent).setLoggerName(AmqpAppender.this.abbreviator.abbreviate(name));
						msgBody = AmqpAppender.this.layout.doLayout(logEvent);
						((LoggingEvent) logEvent).setLoggerName(name);
					}
					else {
						msgBody = AmqpAppender.this.layout.doLayout(logEvent);
					}

					// Send a message
					try {
						Message message = null;
						if (AmqpAppender.this.charset != null) {
							try {
								message = new Message(msgBody.getBytes(AmqpAppender.this.charset), amqpProps);
							}
							catch (UnsupportedEncodingException e) {
								message = new Message(msgBody.getBytes(), amqpProps);//NOSONAR (default charset)
							}
						}

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
	}

	/**
	 * Small helper class to encapsulate a LoggingEvent, its MDC properties, and the number of retries.
	 */
	protected static class Event {

		final ILoggingEvent event;

		final Map<String, String> properties;

		final AtomicInteger retries = new AtomicInteger(0);

		public Event(ILoggingEvent event) {
			this.event = event;
			this.properties = this.event.getMDCPropertyMap();
			this.event.getCallerData();
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
