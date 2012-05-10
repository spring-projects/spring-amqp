/*
 * Copyright 2002-2012 the original author or authors.
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
package org.springframework.amqp.rabbit.support;

/**
 * Instances of this object track pending publisher confirms.
 * The timestamp allows the pending confirmation to be
 * expired. It also holds {@link CorrelationData} for
 * the client to correlate a confirm with a sent message.
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class PendingConfirm {

	private final CorrelationData correlationData;

	private final long timestamp;

	/**
	 * @param correlationId
	 * @param timestamp
	 */
	public PendingConfirm(CorrelationData correlationData, long timestamp) {
		this.correlationData = correlationData;
		this.timestamp = timestamp;
	}

	public CorrelationData getCorrelationData() {
		return correlationData;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "PendingConfirm [correlationData=" + correlationData + "]";
	}

}
