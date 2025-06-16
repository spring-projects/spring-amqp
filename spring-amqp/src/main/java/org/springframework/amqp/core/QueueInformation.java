/*
 * Copyright 2019-present the original author or authors.
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

/**
 * Information about a queue, resulting from a passive declaration.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
public class QueueInformation {

	private final String name;

	private final long messageCount;

	private final int consumerCount;

	private String type = "classic";

	public QueueInformation(String name, long messageCount, int consumerCount) {
		this.name = name;
		this.messageCount = messageCount;
		this.consumerCount = consumerCount;
	}

	public String getName() {
		return this.name;
	}

	public long getMessageCount() {
		return this.messageCount;
	}

	public int getConsumerCount() {
		return this.consumerCount;
	}

	/**
	 * Return a queue type.
	 * {@code classic} by default since AMQP 0.9.1 protocol does not return this info in {@code DeclareOk} reply.
	 * @return a queue type
	 * @since 4.0
	 */
	public String getType() {
		return this.type;
	}

	/**
	 * Set a queue type.
	 * @param type the queue type: {@code quorum}, {@code classic} or {@code stream}
	 * @since 4.0
	 */
	public void setType(String type) {
		this.type = type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.name.hashCode();
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
		QueueInformation other = (QueueInformation) obj;
		return this.name.equals(other.name);
	}

	@Override
	public String toString() {
		return "QueueInformation [name=" + this.name + ", messageCount=" + this.messageCount + ", consumerCount="
				+ this.consumerCount + "]";
	}

}
