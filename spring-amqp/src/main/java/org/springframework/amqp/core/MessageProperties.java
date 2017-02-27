/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.core;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Message Properties for an AMQP message.
 *
 * @author Mark Fisher
 * @author Mark Pollack
 * @author Gary Russell
 * @author Dmitry Chernyshov
 * @author Artem Bilan
 */
public class MessageProperties implements Serializable {

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

	public static final String DEFAULT_CONTENT_TYPE = CONTENT_TYPE_BYTES;

	public static final MessageDeliveryMode DEFAULT_DELIVERY_MODE = MessageDeliveryMode.PERSISTENT;

	public static final Integer DEFAULT_PRIORITY = 0;

	private final Map<String, Object> headers = new HashMap<String, Object>();

	private volatile Date timestamp;

	private volatile String messageId;

	private volatile String userId;

	private volatile String appId;

	private volatile String clusterId;

	private volatile String type;

	private volatile String correlationId;

	private volatile String replyTo;

	private volatile String contentType = DEFAULT_CONTENT_TYPE;

	private volatile String contentEncoding;

	private volatile long contentLength;

	private volatile boolean contentLengthSet;

	private volatile MessageDeliveryMode deliveryMode = DEFAULT_DELIVERY_MODE;

	private volatile String expiration;

	private volatile Integer priority = DEFAULT_PRIORITY;

	private volatile Boolean redelivered;

	private volatile String receivedExchange;

	private volatile String receivedRoutingKey;

	private volatile String receivedUserId;

	private volatile long deliveryTag;

	private volatile boolean deliveryTagSet;

	private volatile Integer messageCount;

	// Not included in hashCode()

	private volatile String consumerTag;

	private volatile String consumerQueue;

	private volatile Integer receivedDelay;

	private volatile MessageDeliveryMode receivedDeliveryMode;

	private volatile boolean finalRetryForMessageWithNoId;

	private volatile transient Type inferredArgumentType;

	private volatile transient Method targetMethod;

	private volatile transient Object targetBean;

	public void setHeader(String key, Object value) {
		this.headers.put(key, value);
	}

	public Map<String, Object> getHeaders() {
		return this.headers;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp; //NOSONAR
	}

	// NOTE qpid java timestamp is long, presumably can convert to Date.
	public Date getTimestamp() {
		return this.timestamp; //NOSONAR
	}

	// NOTE Not forward compatible with qpid 1.0 .NET
	// qpid 0.8 .NET/Java: is a string
	// qpid 1.0 .NET: MessageId property on class MessageProperties and is UUID
	// There is an 'ID' stored IMessage class and is an int.
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getMessageId() {
		return this.messageId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	// NOTE Note forward compatible with qpid 1.0 .NET
	// qpid 0.8 .NET/java: is a string
	// qpid 1.0 .NET: getUserId is byte[]
	public String getUserId() {
		return this.userId;
	}

	/**
	 * Return the user id from an incoming message.
	 * @return the user id.
	 * @since 1.6
	 */
	public String getReceivedUserId() {
		return this.receivedUserId;
	}

	public void setReceivedUserId(String receivedUserId) {
		this.receivedUserId = receivedUserId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAppId() {
		return this.appId;
	}

	// NOTE not forward compatible with qpid 1.0 .NET
	// qpid 0.8 .NET/Java: is a string
	// qpid 1.0 .NET: is not present
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getClusterId() {
		return this.clusterId;
	}

	public void setType(String type) {
		this.type = type;
	}

	// NOTE structureType is int in Qpid
	public String getType() {
		return this.type;
	}

	/**
	 * Set the correlation id.
	 * @param correlationId the id.
	 */
	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	/**
	 * Get the correlation id.
	 * @return the id.
	 */
	public String getCorrelationId() {
		return this.correlationId;
	}

	/**
	 * Get the correlation id.
	 * @return the correlation id
	 * @deprecated use {@link #getCorrelationId()}.
	 */
	@Deprecated
	public String getCorrelationIdString() {
		return this.correlationId;
	}

	/**
	 * Set the correlation id.
	 * @param correlationId the id.
	 * @deprecated - use {@link #setCorrelationId(String)}.
	 */
	@Deprecated
	public void setCorrelationIdString(String correlationId) {
		this.correlationId = correlationId;
	}

	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}

	public String getReplyTo() {
		return this.replyTo;
	}

	public void setReplyToAddress(Address replyTo) {
		this.replyTo = (replyTo != null) ? replyTo.toString() : null;
	}

	public Address getReplyToAddress() {
		return (this.replyTo != null) ? new Address(this.replyTo) : null;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	public String getContentEncoding() {
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

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return this.deliveryMode;
	}

	public MessageDeliveryMode getReceivedDeliveryMode() {
		return this.receivedDeliveryMode;
	}

	public void setReceivedDeliveryMode(MessageDeliveryMode receivedDeliveryMode) {
		this.receivedDeliveryMode = receivedDeliveryMode;
	}

	// why not a Date or long?
	public void setExpiration(String expiration) {
		this.expiration = expiration;
	}

	// NOTE qpid Java broker qpid 0.8/1.0 .NET: is a long.
	// 0.8 Spec has: expiration (shortstr)
	public String getExpiration() {
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

	public String getReceivedExchange() {
		return this.receivedExchange;
	}

	public void setReceivedRoutingKey(String receivedRoutingKey) {
		this.receivedRoutingKey = receivedRoutingKey;
	}

	public String getReceivedRoutingKey() {
		return this.receivedRoutingKey;
	}

	/**
	 * When a delayed message exchange is used the x-delay header on a
	 * received message contains the delay.
	 * @return the received delay.
	 * @since 1.6
	 * @see #getDelay()
	 */
	public Integer getReceivedDelay() {
		return this.receivedDelay;
	}

	/**
	 * When a delayed message exchange is used the x-delay header on a
	 * received message contains the delay.
	 * @param receivedDelay the received delay.
	 * @since 1.6
	 */
	public void setReceivedDelay(Integer receivedDelay) {
		this.receivedDelay = receivedDelay;
	}

	public void setRedelivered(Boolean redelivered) {
		this.redelivered = redelivered;
	}

	public Boolean isRedelivered() {
		return this.redelivered;
	}

	/*
	 * Additional accessor because is* is not standard for type Boolean
	 */
	public Boolean getRedelivered() {
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
	public Integer getMessageCount() {
		return this.messageCount;
	}

	public String getConsumerTag() {
		return this.consumerTag;
	}

	public void setConsumerTag(String consumerTag) {
		this.consumerTag = consumerTag;
	}

	public String getConsumerQueue() {
		return this.consumerQueue;
	}

	public void setConsumerQueue(String consumerQueue) {
		this.consumerQueue = consumerQueue;
	}

	/**
	 * The x-delay header (outbound).
	 * @return the delay.
	 * @since 1.6
	 * @see #getReceivedDelay()
	 */
	public Integer getDelay() {
		Object delay = this.headers.get(X_DELAY);
		if (delay instanceof Integer) {
			return (Integer) delay;
		}
		else {
			return null;
		}
	}

	/**
	 * Set the x-delay header.
	 * @param delay the delay.
	 * @since 1.6
	 */
	public void setDelay(Integer delay) {
		if (delay == null || delay < 0) {
			this.headers.remove(X_DELAY);
		}
		else {
			this.headers.put(X_DELAY, delay);
		}
	}

	public boolean isFinalRetryForMessageWithNoId() {
		return this.finalRetryForMessageWithNoId;
	}

	public void setFinalRetryForMessageWithNoId(boolean finalRetryForMessageWithNoId) {
		this.finalRetryForMessageWithNoId = finalRetryForMessageWithNoId;
	}

	/**
	 * The inferred target argument type when using a method-level
	 * {@code @RabbitListener}.
	 * @return the type.
	 * @since 1.6
	 */
	public Type getInferredArgumentType() {
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
	 * The target method when using a method-level {@code @RabbitListener}.
	 * @return the method.
	 * @since 1.6
	 */
	public Method getTargetMethod() {
		return this.targetMethod;
	}

	/**
	 * Set the target method when using a method-level {@code @RabbitListener}.
	 * @param targetMethod the target method.
	 * @since 1.6
	 */
	public void setTargetMethod(Method targetMethod) {
		this.targetMethod = targetMethod;
	}

	/**
	 * The target bean when using {@code @RabbitListener}.
	 * @return the bean.
	 * @since 1.6
	 */
	public Object getTargetBean() {
		return this.targetBean;
	}

	/**
	 * Set the target bean when using {@code @RabbitListener}.
	 * @param targetBean the bean.
	 * @since 1.6
	 */
	public void setTargetBean(Object targetBean) {
		this.targetBean = targetBean;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.appId == null) ? 0 : this.appId.hashCode());
		result = prime * result + ((this.clusterId == null) ? 0 : this.clusterId.hashCode());
		result = prime * result + ((this.contentEncoding == null) ? 0 : this.contentEncoding.hashCode());
		result = prime * result + (int) (this.contentLength ^ (this.contentLength >>> 32));
		result = prime * result + ((this.contentType == null) ? 0 : this.contentType.hashCode());
		result = prime * result + this.correlationId.hashCode();
		result = prime * result + ((this.deliveryMode == null) ? 0 : this.deliveryMode.hashCode());
		result = prime * result + (int) (this.deliveryTag ^ (this.deliveryTag >>> 32));
		result = prime * result + ((this.expiration == null) ? 0 : this.expiration.hashCode());
		result = prime * result + this.headers.hashCode();
		result = prime * result + ((this.messageCount == null) ? 0 : this.messageCount.hashCode());
		result = prime * result + ((this.messageId == null) ? 0 : this.messageId.hashCode());
		result = prime * result + ((this.priority == null) ? 0 : this.priority.hashCode());
		result = prime * result + ((this.receivedExchange == null) ? 0 : this.receivedExchange.hashCode());
		result = prime * result + ((this.receivedRoutingKey == null) ? 0 : this.receivedRoutingKey.hashCode());
		result = prime * result + ((this.redelivered == null) ? 0 : this.redelivered.hashCode());
		result = prime * result + ((this.replyTo == null) ? 0 : this.replyTo.hashCode());
		result = prime * result + ((this.timestamp == null) ? 0 : this.timestamp.hashCode());
		result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
		result = prime * result + ((this.userId == null) ? 0 : this.userId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
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
		if (this.contentType == null) {
			if (other.contentType != null) {
				return false;
			}
		}
		else if (!this.contentType.equals(other.contentType)) {
			return false;
		}
		if (!this.correlationId.equals(other.correlationId)) {
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
		if (this.priority == null) {
			if (other.priority != null) {
				return false;
			}
		}
		else if (!this.priority.equals(other.priority)) {
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
			if (other.userId != null) {
				return false;
			}
		}
		else if (!this.userId.equals(other.userId)) {
			return false;
		}
		return true;
	}

	@Override
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
				+ (this.contentType == null ? "" : ", contentType=" + this.contentType)
				+ (this.contentEncoding == null ? "" : ", contentEncoding=" + this.contentEncoding)
				+ ", contentLength=" + this.contentLength
				+ (this.deliveryMode == null ? "" : ", deliveryMode=" + this.deliveryMode)
				+ (this.receivedDeliveryMode == null ? "" : ", receivedDeliveryMode=" + this.receivedDeliveryMode)
				+ (this.expiration == null ? "" : ", expiration=" + this.expiration)
				+ (this.priority == null ? "" : ", priority=" + this.priority)
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
