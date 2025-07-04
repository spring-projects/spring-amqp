/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.core;

import java.io.Serial;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * Message Properties for an AMQP message.
 *
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Gary Russell
 * @author Dmitry Chernyshov
 * @author Artem Bilan
 * @author Csaba Soti
 * @author Raylax Grey
 * @author Ngoc Nhan
 */
public class MessageProperties implements Serializable {

	@Serial
	private static final long serialVersionUID = 1619000546531112290L;

	public static final String CONTENT_TYPE_BYTES = "application/octet-stream";

	public static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";

	public static final String CONTENT_TYPE_SERIALIZED_OBJECT = "application/x-java-serialized-object";

	public static final String CONTENT_TYPE_JSON = "application/json";

	public static final String CONTENT_TYPE_JSON_ALT = "text/x-json";

	public static final String CONTENT_TYPE_XML = "application/xml";

	public static final String SPRING_BATCH_FORMAT = "springBatchFormat";

	public static final String BATCH_FORMAT_LENGTH_HEADER4 = "lengthHeader4";

	public static final String SPRING_AUTO_DECOMPRESS = "springAutoDecompress";

	public static final String X_DELAY = "x-delay";

	/**
	 * The custom header to represent a number of retries a message is republished.
	 * In case of server-side DLX, this header contains the value of {@code x-death.count} property.
	 * When republish is done manually, this header has to be incremented by the application.
	 */
	public static final String RETRY_COUNT = "retry-count";

	public static final String DEFAULT_CONTENT_TYPE = CONTENT_TYPE_BYTES;

	public static final MessageDeliveryMode DEFAULT_DELIVERY_MODE = MessageDeliveryMode.PERSISTENT;

	public static final Integer DEFAULT_PRIORITY = 0;

	/**
	 * The maximum value of x-delay header.
	 * @since 3.1.2
	 */
	public static final long X_DELAY_MAX = 0xffffffffL;

	private final HashMap<String, @Nullable Object> headers = new HashMap<>();

	private @Nullable Date timestamp;

	private @Nullable String messageId;

	private @Nullable String userId;

	private @Nullable String appId;

	private @Nullable String clusterId;

	private @Nullable String type;

	private @Nullable String correlationId;

	private @Nullable String replyTo;

	private String contentType = DEFAULT_CONTENT_TYPE;

	private @Nullable String contentEncoding;

	private long contentLength;

	private boolean contentLengthSet;

	private @Nullable MessageDeliveryMode deliveryMode = DEFAULT_DELIVERY_MODE;

	private @Nullable String expiration;

	private Integer priority = DEFAULT_PRIORITY;

	private @Nullable Boolean redelivered;

	private @Nullable String receivedExchange;

	private @Nullable String receivedRoutingKey;

	private @Nullable String receivedUserId;

	private long deliveryTag;

	private boolean deliveryTagSet;

	private @Nullable Integer messageCount;

	// Not included in hashCode()

	private @Nullable String consumerTag;

	private @Nullable String consumerQueue;

	private @Nullable Long receivedDelay;

	private @Nullable MessageDeliveryMode receivedDeliveryMode;

	private long retryCount;

	private boolean finalRetryForMessageWithNoId;

	private long publishSequenceNumber;

	private boolean lastInBatch;

	private boolean projectionUsed;

	private transient @Nullable Type inferredArgumentType;

	private transient @Nullable Method targetMethod;

	private transient @Nullable Object targetBean;

	private transient @Nullable AmqpAcknowledgment amqpAcknowledgment;

	public void setHeader(String key, Object value) {
		this.headers.put(key, value);
	}

	/**
	 * Set headers.
	 * @param headers the headers.
	 * @since 2.4.7
	 */
	public void setHeaders(Map<String, Object> headers) {
		this.headers.putAll(headers);
	}

	/**
	 * Typed getter for a header.
	 * @param headerName the header name.
	 * @param <T> the type.
	 * @return the header value
	 * @since 2.2
	 */
	@SuppressWarnings("unchecked")
	public <T> @Nullable T getHeader(String headerName) {
		return (T) this.headers.get(headerName);
	}

	public Map<String, @Nullable Object> getHeaders() {
		return this.headers;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp; //NOSONAR
	}

	public @Nullable Date getTimestamp() {
		return this.timestamp; //NOSONAR
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public @Nullable String getMessageId() {
		return this.messageId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public @Nullable String getUserId() {
		return this.userId;
	}

	/**
	 * Return the user id from an incoming message.
	 * @return the user id.
	 * @since 1.6
	 */
	public @Nullable String getReceivedUserId() {
		return this.receivedUserId;
	}

	public void setReceivedUserId(String receivedUserId) {
		this.receivedUserId = receivedUserId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public @Nullable String getAppId() {
		return this.appId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public @Nullable String getClusterId() {
		return this.clusterId;
	}

	public void setType(String type) {
		this.type = type;
	}

	public @Nullable String getType() {
		return this.type;
	}

	/**
	 * Set the correlation id.
	 * @param correlationId the id.
	 */
	public void setCorrelationId(@Nullable String correlationId) {
		this.correlationId = correlationId;
	}

	/**
	 * Get the correlation id.
	 * @return the id.
	 */
	public @Nullable String getCorrelationId() {
		return this.correlationId;
	}

	public void setReplyTo(@Nullable String replyTo) {
		this.replyTo = replyTo;
	}

	public @Nullable String getReplyTo() {
		return this.replyTo;
	}

	public void setReplyToAddress(@Nullable Address replyTo) {
		this.replyTo = (replyTo != null) ? replyTo.toString() : null;
	}

	public @Nullable Address getReplyToAddress() {
		return (this.replyTo != null) ? new Address(this.replyTo) : null;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentEncoding(@Nullable String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	public @Nullable String getContentEncoding() {
		return this.contentEncoding;
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
		this.contentLengthSet = true;
	}

	public long getContentLength() {
		return this.contentLength;
	}

	protected final boolean isContentLengthSet() {
		return this.contentLengthSet;
	}

	public void setDeliveryMode(@Nullable MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public @Nullable MessageDeliveryMode getDeliveryMode() {
		return this.deliveryMode;
	}

	public @Nullable MessageDeliveryMode getReceivedDeliveryMode() {
		return this.receivedDeliveryMode;
	}

	public void setReceivedDeliveryMode(MessageDeliveryMode receivedDeliveryMode) {
		this.receivedDeliveryMode = receivedDeliveryMode;
	}

	/**
	 * Set the message expiration. This is a String property per the AMQP 0.9.1 spec. For
	 * RabbitMQ, this is a String representation of the message time to live in
	 * milliseconds.
	 * @param expiration the expiration.
	 */
	public void setExpiration(String expiration) {
		this.expiration = expiration;
	}

	/**
	 * Get the message expiration. This is a String property per the AMQP 0.9.1 spec. For
	 * RabbitMQ, this is a String representation of the message time to live in
	 * milliseconds.
	 * @return the expiration.
	 */
	public @Nullable String getExpiration() {
		return this.expiration;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public Integer getPriority() {
		return this.priority;
	}

	public void setReceivedExchange(String receivedExchange) {
		this.receivedExchange = receivedExchange;
	}

	public @Nullable String getReceivedExchange() {
		return this.receivedExchange;
	}

	public void setReceivedRoutingKey(String receivedRoutingKey) {
		this.receivedRoutingKey = receivedRoutingKey;
	}

	public @Nullable String getReceivedRoutingKey() {
		return this.receivedRoutingKey;
	}

	/**
	 * When a delayed message exchange is used the x-delay header on a
	 * received message contains the delay.
	 * @return the received delay.
	 * @since 3.1.2
	 * @see #getDelayLong()
	 */
	public @Nullable Long getReceivedDelayLong() {
		return this.receivedDelay;
	}

	/**
	 * When a delayed message exchange is used the x-delay header on a
	 * received message contains the delay.
	 * @param receivedDelay the received delay.
	 * @since 3.1.2
	 * @see #setDelayLong(Long)
	 */
	public void setReceivedDelayLong(Long receivedDelay) {
		this.receivedDelay = receivedDelay;
	}

	public void setRedelivered(Boolean redelivered) {
		this.redelivered = redelivered;
	}

	public @Nullable Boolean isRedelivered() {
		return this.redelivered;
	}

	/*
	 * Additional accessor because is* is not standard for type Boolean
	 */
	public @Nullable Boolean getRedelivered() {
		return this.redelivered;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
		this.deliveryTagSet = true;
	}

	public long getDeliveryTag() {
		return this.deliveryTag;
	}

	protected final boolean isDeliveryTagSet() {
		return this.deliveryTagSet;
	}

	/**
	 * Set the message count.
	 * @param messageCount the count
	 * @see #getMessageCount()
	 */
	public void setMessageCount(Integer messageCount) {
		this.messageCount = messageCount;
	}

	/**
	 * Return the server's most recent estimate of the number of messages remaining on the queue.
	 * Only applies to messages retrieved via {@code basicGet}.
	 * @return the count.
	 */
	public @Nullable Integer getMessageCount() {
		return this.messageCount;
	}

	public @Nullable String getConsumerTag() {
		return this.consumerTag;
	}

	public void setConsumerTag(String consumerTag) {
		this.consumerTag = consumerTag;
	}

	public @Nullable String getConsumerQueue() {
		return this.consumerQueue;
	}

	public void setConsumerQueue(String consumerQueue) {
		this.consumerQueue = consumerQueue;
	}

	/**
	 * Get the x-delay header long value.
	 * @return the delay.
	 * @since 3.1.2
	 */
	public @Nullable Long getDelayLong() {
		Object delay = this.headers.get(X_DELAY);
		if (delay instanceof Long delayLong) {
			return delayLong;
		}
		return null;
	}

	/**
	 * Set the x-delay header to a long value.
	 * @param delay the delay.
	 * @since 3.1.2
	 */
	public void setDelayLong(@Nullable Long delay) {
		if (delay == null || delay < 0) {
			this.headers.remove(X_DELAY);
			return;
		}

		Assert.isTrue(delay <= X_DELAY_MAX, "Delay cannot exceed " + X_DELAY_MAX);
		this.headers.put(X_DELAY, delay);
	}

	/**
	 * The number of retries for this message over broker.
	 * @return the retry count
	 * @since 3.2
	 */
	public long getRetryCount() {
		return this.retryCount;
	}

	/**
	 * Set a number of retries for this message over broker.
	 * @param retryCount the retry count.
	 * @since 3.2
	 * @see #incrementRetryCount()
	 */
	public void setRetryCount(long retryCount) {
		this.retryCount = retryCount;
	}

	/**
	 * Increment a retry count for this message when it is re-published back to the broker.
	 * @since 3.2
	 */
	public void incrementRetryCount() {
		this.retryCount++;
	}

	public boolean isFinalRetryForMessageWithNoId() {
		return this.finalRetryForMessageWithNoId;
	}

	public void setFinalRetryForMessageWithNoId(boolean finalRetryForMessageWithNoId) {
		this.finalRetryForMessageWithNoId = finalRetryForMessageWithNoId;
	}

	/**
	 * Return the publishing sequence number if publisher confirms are enabled; set by the template.
	 * @return the sequence number.
	 * @since 2.1
	 */
	public long getPublishSequenceNumber() {
		return this.publishSequenceNumber;
	}

	/**
	 * Set the publishing sequence number, if publisher confirms are enabled; set by the template.
	 * @param publishSequenceNumber the sequence number.
	 * @since 2.1
	 */
	public void setPublishSequenceNumber(long publishSequenceNumber) {
		this.publishSequenceNumber = publishSequenceNumber;
	}

	/**
	 * The inferred target argument type when using a method-level
	 * {@code @RabbitListener}.
	 * @return the type.
	 * @since 1.6
	 */
	public @Nullable Type getInferredArgumentType() {
		return this.inferredArgumentType;
	}

	/**
	 * Set the inferred target argument type when using a method-level
	 * {@code @RabbitListener}.
	 * @param inferredArgumentType the type.
	 * @since 1.6
	 */
	public void setInferredArgumentType(Type inferredArgumentType) {
		this.inferredArgumentType = inferredArgumentType;
	}

	/**
	 * The target method when using a {@code @RabbitListener}.
	 * @return the method.
	 * @since 1.6
	 */
	public @Nullable Method getTargetMethod() {
		return this.targetMethod;
	}

	/**
	 * Set the target method when using a {@code @RabbitListener}.
	 * @param targetMethod the target method.
	 * @since 1.6
	 */
	public void setTargetMethod(@Nullable Method targetMethod) {
		this.targetMethod = targetMethod;
	}

	/**
	 * The target bean when using {@code @RabbitListener}.
	 * @return the bean.
	 * @since 1.6
	 */
	public @Nullable Object getTargetBean() {
		return this.targetBean;
	}

	/**
	 * Set the target bean when using {@code @RabbitListener}.
	 * @param targetBean the bean.
	 * @since 1.6
	 */
	public void setTargetBean(@Nullable Object targetBean) {
		this.targetBean = targetBean;
	}

	/**
	 * When true; the message having these properties is the last message from a batch.
	 * @return true for the last message.
	 * @since 2.2
	 */
	public boolean isLastInBatch() {
		return this.lastInBatch;
	}

	/**
	 * Set to true to indicate these properties are for the last message in a batch.
	 * @param lastInBatch true for the last.
	 * @since 2.2
	 */
	public void setLastInBatch(boolean lastInBatch) {
		this.lastInBatch = lastInBatch;
	}

	/**
	 * Get an internal flag used to communicate that conversion used projection; always
	 * false at the application level.
	 * @return true if projection was used.
	 * @since 2.2.20
	 */
	public boolean isProjectionUsed() {
		return this.projectionUsed;
	}

	/**
	 * Set an internal flag used to communicate that conversion used projection; always false
	 * at the application level.
	 * @param projectionUsed true for projection.
	 * @since 2.2.20
	 */
	public void setProjectionUsed(boolean projectionUsed) {
		this.projectionUsed = projectionUsed;
	}

	/**
	 * Return the x-death header.
	 * @return the header.
	 */
	@SuppressWarnings("unchecked")
	public @Nullable List<Map<String, ?>> getXDeathHeader() {
		try {
			return (List<Map<String, ?>>) this.headers.get("x-death");
		}
		catch (@SuppressWarnings("unused") Exception e) {
			return null;
		}
	}

	/**
	 * Return the {@link AmqpAcknowledgment} for consumer if any.
	 * @return the {@link AmqpAcknowledgment} for consumer if any.
	 * @since 4.0
	 */
	public @Nullable AmqpAcknowledgment getAmqpAcknowledgment() {
		return this.amqpAcknowledgment;
	}

	/**
	 * Set an {@link AmqpAcknowledgment} for manual acks in the target message processor.
	 * This is only in-application a consumer side logic.
	 * @param amqpAcknowledgment the {@link AmqpAcknowledgment} to use in the application.
	 * @since 4.0
	 */
	public void setAmqpAcknowledgment(AmqpAcknowledgment amqpAcknowledgment) {
		this.amqpAcknowledgment = amqpAcknowledgment;
	}

	@Override // NOSONAR complexity
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.appId == null) ? 0 : this.appId.hashCode());
		result = prime * result + ((this.clusterId == null) ? 0 : this.clusterId.hashCode());
		result = prime * result + ((this.contentEncoding == null) ? 0 : this.contentEncoding.hashCode());
		result = prime * result + Long.hashCode(this.contentLength);
		result = prime * result + this.contentType.hashCode();
		result = prime * result + ((this.correlationId == null) ? 0 : this.correlationId.hashCode());
		result = prime * result + ((this.deliveryMode == null) ? 0 : this.deliveryMode.hashCode());
		result = prime * result + Long.hashCode(this.deliveryTag);
		result = prime * result + ((this.expiration == null) ? 0 : this.expiration.hashCode());
		result = prime * result + this.headers.hashCode();
		result = prime * result + ((this.messageCount == null) ? 0 : this.messageCount.hashCode());
		result = prime * result + ((this.messageId == null) ? 0 : this.messageId.hashCode());
		result = prime * result + this.priority.hashCode();
		result = prime * result + ((this.receivedExchange == null) ? 0 : this.receivedExchange.hashCode());
		result = prime * result + ((this.receivedRoutingKey == null) ? 0 : this.receivedRoutingKey.hashCode());
		result = prime * result + ((this.redelivered == null) ? 0 : this.redelivered.hashCode());
		result = prime * result + ((this.replyTo == null) ? 0 : this.replyTo.hashCode());
		result = prime * result + ((this.timestamp == null) ? 0 : this.timestamp.hashCode());
		result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
		result = prime * result + ((this.userId == null) ? 0 : this.userId.hashCode());
		return result;
	}

	@Override // NOSONAR complexity
	public boolean equals(Object obj) { // NOSONAR line count
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		MessageProperties other = (MessageProperties) obj;
		if (this.appId == null) {
			if (other.appId != null) {
				return false;
			}
		}
		else if (!this.appId.equals(other.appId)) {
			return false;
		}
		if (this.clusterId == null) {
			if (other.clusterId != null) {
				return false;
			}
		}
		else if (!this.clusterId.equals(other.clusterId)) {
			return false;
		}
		if (this.contentEncoding == null) {
			if (other.contentEncoding != null) {
				return false;
			}
		}
		else if (!this.contentEncoding.equals(other.contentEncoding)) {
			return false;
		}
		if (this.contentLength != other.contentLength) {
			return false;
		}
		if (!this.contentType.equals(other.contentType)) {
			return false;
		}

		if (this.correlationId == null) {
			if (other.correlationId != null) {
				return false;
			}
		}
		else if (!this.correlationId.equals(other.correlationId)) {
			return false;
		}

		if (this.deliveryMode != other.deliveryMode) {
			return false;
		}
		if (this.deliveryTag != other.deliveryTag) {
			return false;
		}
		if (this.expiration == null) {
			if (other.expiration != null) {
				return false;
			}
		}
		else if (!this.expiration.equals(other.expiration)) {
			return false;
		}
		if (!this.headers.equals(other.headers)) {
			return false;
		}
		if (this.messageCount == null) {
			if (other.messageCount != null) {
				return false;
			}
		}
		else if (!this.messageCount.equals(other.messageCount)) {
			return false;
		}
		if (this.messageId == null) {
			if (other.messageId != null) {
				return false;
			}
		}
		else if (!this.messageId.equals(other.messageId)) {
			return false;
		}
		if (!this.priority.equals(other.priority)) {
			return false;
		}
		if (this.receivedExchange == null) {
			if (other.receivedExchange != null) {
				return false;
			}
		}
		else if (!this.receivedExchange.equals(other.receivedExchange)) {
			return false;
		}
		if (this.receivedRoutingKey == null) {
			if (other.receivedRoutingKey != null) {
				return false;
			}
		}
		else if (!this.receivedRoutingKey.equals(other.receivedRoutingKey)) {
			return false;
		}
		if (this.redelivered == null) {
			if (other.redelivered != null) {
				return false;
			}
		}
		else if (!this.redelivered.equals(other.redelivered)) {
			return false;
		}
		if (this.replyTo == null) {
			if (other.replyTo != null) {
				return false;
			}
		}
		else if (!this.replyTo.equals(other.replyTo)) {
			return false;
		}
		if (this.timestamp == null) {
			if (other.timestamp != null) {
				return false;
			}
		}
		else if (!this.timestamp.equals(other.timestamp)) {
			return false;
		}
		if (this.type == null) {
			if (other.type != null) {
				return false;
			}
		}
		else if (!this.type.equals(other.type)) {
			return false;
		}
		if (this.userId == null) {
			return other.userId == null;
		}
		return this.userId.equals(other.userId);
	}

	@Override // NOSONAR complexity
	public String toString() {
		return "MessageProperties [headers=" + this.headers
				+ (this.timestamp == null ? "" : ", timestamp=" + this.timestamp)
				+ (this.messageId == null ? "" : ", messageId=" + this.messageId)
				+ (this.userId == null ? "" : ", userId=" + this.userId)
				+ (this.receivedUserId == null ? "" : ", receivedUserId=" + this.receivedUserId)
				+ (this.appId == null ? "" : ", appId=" + this.appId)
				+ (this.clusterId == null ? "" : ", clusterId=" + this.clusterId)
				+ (this.type == null ? "" : ", type=" + this.type)
				+ (this.correlationId == null ? "" : ", correlationId=" + this.correlationId)
				+ (this.replyTo == null ? "" : ", replyTo=" + this.replyTo)
				+ ", contentType=" + this.contentType
				+ (this.contentEncoding == null ? "" : ", contentEncoding=" + this.contentEncoding)
				+ ", contentLength=" + this.contentLength
				+ (this.deliveryMode == null ? "" : ", deliveryMode=" + this.deliveryMode)
				+ (this.receivedDeliveryMode == null ? "" : ", receivedDeliveryMode=" + this.receivedDeliveryMode)
				+ (this.expiration == null ? "" : ", expiration=" + this.expiration)
				+ ", priority=" + this.priority
				+ (this.redelivered == null ? "" : ", redelivered=" + this.redelivered)
				+ (this.receivedExchange == null ? "" : ", receivedExchange=" + this.receivedExchange)
				+ (this.receivedRoutingKey == null ? "" : ", receivedRoutingKey=" + this.receivedRoutingKey)
				+ (this.receivedDelay == null ? "" : ", receivedDelay=" + this.receivedDelay)
				+ ", deliveryTag=" + this.deliveryTag
				+ (this.messageCount == null ? "" : ", messageCount=" + this.messageCount)
				+ (this.consumerTag == null ? "" : ", consumerTag=" + this.consumerTag)
				+ (this.consumerQueue == null ? "" : ", consumerQueue=" + this.consumerQueue)
				+ "]";
	}

}
