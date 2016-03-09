/*
 * Copyright 2002-2016 the original author or authors.
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

	private String cause;

	/**
	 * @param correlationData The correlation data.
	 * @param timestamp The timestamp.
	 */
	public PendingConfirm(CorrelationData correlationData, long timestamp) {
		this.correlationData = correlationData;
		this.timestamp = timestamp;
	}

	/**
	 * The correlation data supplied by the client when sending the message
	 * corresponding to this confirmation.
	 * @return The correlation data.
	 */
	public CorrelationData getCorrelationData() {
		return this.correlationData;
	}

	/**
	 * @return The time the message was sent.
	 */
	public long getTimestamp() {
		return this.timestamp;
	}

	/**
	 * When the confirmation is nacked, set the cause when available.
	 * @param cause The cause.
	 * @since 1.4
	 */
	public void setCause(String cause) {
		this.cause = cause;
	}

	/**
	 * @return the cause.
	 * @since 1.4
	 */
	public String getCause() {
		return this.cause;
	}

	@Override
	public String toString() {
		return "PendingConfirm [correlationData=" + this.correlationData + (this.cause == null ? "" : " cause=" + this.cause) + "]";
	}

}
