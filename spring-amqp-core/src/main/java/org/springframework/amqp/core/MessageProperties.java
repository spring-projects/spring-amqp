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
import java.util.Map;

/**
 * Message Properties for an AMQP message.
 * 
 * @author Mark Fisher
 * @author Mark Pollack
 *
 */
public interface MessageProperties {

	public static final String CONTENT_TYPE_BYTES = "application/octet-stream";
	public static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
	public static final String CONTENT_TYPE_SERIALIZED_OBJECT = "application/x-java-serialized-object";
	public static final String CONTENT_TYPE_JSON = "application/json";

	public abstract Map<String, Object> getHeaders();

	public abstract void setHeader(String key, Object value);

	//NOTE qpid java timestamp is long, presumably can convert to Date.
	public abstract Date getTimestamp();

	public abstract void setAppId(String appId);

	public abstract String getAppId();

	public abstract void setUserId(String userId);

	//NOTE Note forward compatible with qpid 1.0 .NET
	//     qpid 0.8 .NET/java: is a string
	//     qpid 1.0 .NET: getUserId is byte[]
	public abstract String getUserId();

	public abstract void setType(String type);

	//TODO what is this?  is it stuctureType - int in qpid
	public abstract String getType();

	//NOTE Not forward compatible with qpid 1.0 .NET
	//     qpid 0.8 .NET/Java: is a string
	//     qpid 1.0 .NET: MessageId property on class MessageProperties and is UUID 
	//                    There is an 'ID' stored IMessage class and is an int.
	public abstract void setMessageId(String id);

	public abstract String getMessageId();

	
	//NOTE not foward compatible with qpid 1.0 .NET
	//     qpid 0.8 .NET/Java: is a string
	//     qpid 1.0 .NET: is not present
	public abstract void setClusterId(String id);

	public abstract String getClusterId();

	public abstract void setCorrelationId(byte[] correlationId);

	public abstract byte[] getCorrelationId();

	public abstract void setReplyTo(Address replyTo);

	//TODO - create Address/ReplyTo class to encapsulate exchangType/exchange/routingkey ? 
	//       qpid 0.8/1.0 .NET don't use a single string, but a pair.  
	//       qpid 0.8 Java uses a single string
	//
	//       See RabbitMQ .NET developer guide for more details on conventions for this string
	public abstract Address getReplyTo();

	public abstract void setContentType(String contentType);

	public abstract String getContentType();

	public abstract void setContentEncoding(String contentEncoding);

	public abstract String getContentEncoding();

	public abstract void setContentLength(long contentLength);

	public abstract long getContentLength();

	public abstract void setDefaultCharset(String charSet);

	public abstract void setDeliveryMode(MessageDeliveryMode deliveryMode);

	public abstract MessageDeliveryMode getDeliveryMode();

	public abstract void setExpiration(String expiration);

	//NOTE qpid Java broker qpid 0.8/1.0 .NET: is a long.  
	//     0.8 Spec has:  expiration (shortstr) 
	public abstract String getExpiration();

	public abstract void setPriority(Integer priority);

	public abstract Integer getPriority();

	public abstract String getReceivedExchange();

	public abstract String getReceivedRoutingKey();

	public abstract Boolean isRedelivered();

	public abstract long getDeliveryTag();

	public abstract Integer getMessageCount();

}