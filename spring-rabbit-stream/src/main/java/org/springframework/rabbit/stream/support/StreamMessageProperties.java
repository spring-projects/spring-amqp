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

import java.util.Objects;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.lang.Nullable;

import com.rabbitmq.stream.MessageHandler.Context;

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
	 * See {@link com.rabbitmq.stream.Properties#getTo()}.
	 * @return the to address.
	 */
	public String getTo() {
		return this.to;
	}

	/**
	 * See {@link com.rabbitmq.stream.MessageBuilder.PropertiesBuilder#to(String)}.
	 * @param address the address.
	 */
	public void setTo(String address) {
		this.to = address;
	}

	/**
	 * See {@link com.rabbitmq.stream.Properties#getSubject()}.
	 * @return the subject.
	 */
	public String getSubject() {
		return this.subject;
	}

	/**
	 * See {@link com.rabbitmq.stream.MessageBuilder.PropertiesBuilder#subject(String)}.
	 * @param subject the subject.
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}

	/**
	 * See {@link com.rabbitmq.stream.Properties#getCreationTime()}.
	 * @return the creation time.
	 */
	public long getCreationTime() {
		return this.creationTime;
	}

	/**
	 * See
	 * {@link com.rabbitmq.stream.MessageBuilder.PropertiesBuilder#creationTime(long)}.
	 * @param creationTime the creation time.
	 */
	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	/**
	 * See {@link com.rabbitmq.stream.Properties#getGroupId()}.
	 * @return the group id.
	 */
	public String getGroupId() {
		return this.groupId;
	}

	/**
	 * See {@link com.rabbitmq.stream.MessageBuilder.PropertiesBuilder#groupId(String)}.
	 * @param groupId the group id.
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	/**
	 * See {@link com.rabbitmq.stream.Properties#getGroupSequence()}.
	 * @return the group sequence.
	 */
	public long getGroupSequence() {
		return this.groupSequence;
	}

	/**
	 * See
	 * {@link com.rabbitmq.stream.MessageBuilder.PropertiesBuilder#groupSequence(long)}.
	 * @param groupSequence the group sequence.
	 */
	public void setGroupSequence(long groupSequence) {
		this.groupSequence = groupSequence;
	}

	/**
	 * See {@link com.rabbitmq.stream.Properties#getReplyToGroupId()}.
	 * @return the reply to group id.
	 */
	public String getReplyToGroupId() {
		return this.replyToGroupId;
	}

	/**
	 * See
	 * {@link com.rabbitmq.stream.MessageBuilder.PropertiesBuilder#replyToGroupId(String)}.
	 * @param replyToGroupId the reply to group id.
	 */
	public void setReplyToGroupId(String replyToGroupId) {
		this.replyToGroupId = replyToGroupId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(this.creationTime, this.groupId, this.groupSequence, this.replyToGroupId,
				this.subject, this.to);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		StreamMessageProperties other = (StreamMessageProperties) obj;
		return this.creationTime == other.creationTime && Objects.equals(this.groupId, other.groupId)
				&& this.groupSequence == other.groupSequence
				&& Objects.equals(this.replyToGroupId, other.replyToGroupId)
				&& Objects.equals(this.subject, other.subject) && Objects.equals(this.to, other.to);
	}

}
