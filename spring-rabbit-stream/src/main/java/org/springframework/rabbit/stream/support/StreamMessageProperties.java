/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.rabbit.stream.support;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.lang.Nullable;

import com.rabbitmq.stream.MessageBuilder.PropertiesBuilder;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.Properties;

/**
 * {@link MessageProperties} extension for stream messages.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamMessageProperties extends MessageProperties {

	private static final long serialVersionUID = 1L;

	private transient Context context;

	private String to;

	private String subject;

	private long creationTime;

	private String groupId;

	private long groupSequence;

	private String replyToGroupId;

	/**
	 * Create a new instance with the provided context.
	 * @param context the context.
	 */
	public StreamMessageProperties(@Nullable Context context) {
		this.context = context;
	}

	/**
	 * Return the stream {@link Context} for the message.
	 * @return the context.
	 */
	@Nullable
	public Context getContext() {
		return this.context;
	}

	/**
	 * See {@link Properties#getTo()}.
	 * @return the to address.
	 */
	public String getTo() {
		return this.to;
	}

	/**
	 * See {@link PropertiesBuilder#to(String)}.
	 * @param address the address.
	 */
	public void setTo(String address) {
		this.to = address;
	}

	/**
	 * See {@link Properties#getSubject()}.
	 * @return the subject.
	 */
	public String getSubject() {
		return this.subject;
	}

	/**
	 * See {@link PropertiesBuilder#subject(String)}.
	 * @param subject the subject.
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}

	/**
	 * See {@link Properties#getCreationTime()}.
	 * @return the creation time.
	 */
	public long getCreationTime() {
		return this.creationTime;
	}

	/**
	 * See {@link PropertiesBuilder#creationTime(long)}.
	 * @param creationTime the creation time.
	 */
	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	/**
	 * See {@link Properties#getGroupId()}.
	 * @return the group id.
	 */
	public String getGroupId() {
		return this.groupId;
	}

	/**
	 * See {@link PropertiesBuilder#groupId(String)}.
	 * @param groupId the group id.
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	/**
	 * See {@link Properties#getGroupSequence()}.
	 * @return the group sequence.
	 */
	public long getGroupSequence() {
		return this.groupSequence;
	}

	/**
	 * See {@link PropertiesBuilder#groupSequence(long)}.
	 * @param groupSequence the group sequence.
	 */
	public void setGroupSequence(long groupSequence) {
		this.groupSequence = groupSequence;
	}

	/**
	 * See {@link Properties#getReplyToGroupId()}.
	 * @return the reply to group id.
	 */
	public String getReplyToGroupId() {
		return this.replyToGroupId;
	}

	/**
	 * See {@link PropertiesBuilder#replyToGroupId(String)}.
	 * @param replyToGroupId the reply to group id.
	 */
	public void setReplyToGroupId(String replyToGroupId) {
		this.replyToGroupId = replyToGroupId;
	}

}
