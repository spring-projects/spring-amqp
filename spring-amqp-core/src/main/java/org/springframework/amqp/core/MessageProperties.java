/*
 * Copyright 2002-2010 the original author or authors.
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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Message Properties for an AMQP message.
 * 
 * @author Mark Fisher
 * @author Mark Pollack
 */
public class MessageProperties {

	public static final String CONTENT_TYPE_BYTES = "application/octet-stream";

	public static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";

	public static final String CONTENT_TYPE_SERIALIZED_OBJECT = "application/x-java-serialized-object";

	public static final String CONTENT_TYPE_JSON = "application/json";


	private static final String DEFAULT_CONTENT_TYPE = CONTENT_TYPE_BYTES;

	private static final MessageDeliveryMode DEFAULT_DELIVERY_MODE = MessageDeliveryMode.PERSISTENT;

	private static final Integer DEFAULT_PRIORITY = new Integer(0);


	private final Map<String, Object> headers = new HashMap<String, Object>();

	private volatile Date timestamp;

	private volatile String messageId;

	private volatile String userId;

	private volatile String appId;

	private volatile String clusterId;

	private volatile String type;

	private volatile byte[] correlationId;

	private volatile Address replyTo;

	private volatile String contentType = DEFAULT_CONTENT_TYPE;

	private volatile String contentEncoding;

	private volatile long contentLength;

	private volatile MessageDeliveryMode deliveryMode = DEFAULT_DELIVERY_MODE;

	private volatile String expiration;

	private volatile Integer priority = DEFAULT_PRIORITY;

	private volatile Boolean redelivered;

	private volatile String receivedExchange;

	private volatile String receivedRoutingKey;

	private volatile long deliveryTag;

	private volatile Integer messageCount;


	public void setHeader(String key, Object value) {
		this.headers.put(key, value);
	}

	public Map<String, Object> getHeaders() {
		return this.headers;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	//NOTE qpid java timestamp is long, presumably can convert to Date.
	public Date getTimestamp() {
		return this.timestamp;
	}

	//NOTE Not forward compatible with qpid 1.0 .NET
	//     qpid 0.8 .NET/Java: is a string
	//     qpid 1.0 .NET: MessageId property on class MessageProperties and is UUID 
	//                    There is an 'ID' stored IMessage class and is an int.
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getMessageId() {
		return this.messageId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	//NOTE Note forward compatible with qpid 1.0 .NET
	//     qpid 0.8 .NET/java: is a string
	//     qpid 1.0 .NET: getUserId is byte[]
	public String getUserId() {
		return this.userId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAppId() {
		return this.appId;
	}

	//NOTE not forward compatible with qpid 1.0 .NET
	//     qpid 0.8 .NET/Java: is a string
	//     qpid 1.0 .NET: is not present
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;;
	}

	public String getClusterId() {
		return this.clusterId;
	}

	public void setType(String type) {
		this.type = type;
	}

	//TODO what is this?  is it stuctureType - int in qpid
	public String getType() {
		return this.type;
	}

	public void setCorrelationId(byte[] correlationId) {
		this.correlationId = correlationId;
	}

	public byte[] getCorrelationId() {
		return this.correlationId;
	}

	public void setReplyTo(Address replyTo) {
		this.replyTo = replyTo;
	}

	//TODO - create Address/ReplyTo class to encapsulate exchangeType/exchange/routingkey ? 
	//       qpid 0.8/1.0 .NET don't use a single string, but a pair.  
	//       qpid 0.8 Java uses a single string
	//
	//       See RabbitMQ .NET developer guide for more details on conventions for this string
	public Address getReplyTo() {
		return this.replyTo;
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
	}

	public long getContentLength() {
		return this.contentLength;
	}

	//public void setDefaultCharset(String charSet) {
	//}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return this.deliveryMode;
	}

	// why not a Date or long?
	public void setExpiration(String expiration) {
		this.expiration = expiration;
	}

	//NOTE qpid Java broker qpid 0.8/1.0 .NET: is a long.  
	//     0.8 Spec has:  expiration (shortstr) 
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

	public void setRedelivered(Boolean redelivered) {
		this.redelivered = redelivered;
	}

	public Boolean isRedelivered() {
		return this.redelivered;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	public long getDeliveryTag() {
		return this.deliveryTag;
	}

	public void setMessageCount(Integer messageCount) {
		this.messageCount = messageCount;
	}

	public Integer getMessageCount() {
		return this.messageCount;
	}

}
