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

package org.springframework.amqp.rabbit.connection;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

/**
 * Instances of this object track pending publisher confirms.
 * The timestamp allows the pending confirmation to be
 * expired. It also holds {@link CorrelationData} for
 * the client to correlate a confirm with a sent message.
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.0.1
 *
 */
public class PendingConfirm {

	static final long RETURN_CALLBACK_TIMEOUT = 60;

	private final @Nullable CorrelationData correlationData;

	private final long timestamp;

	private final CountDownLatch latch = new CountDownLatch(1);

	private @Nullable String cause;

	private boolean returned;

	/**
	 * @param correlationData The correlation data.
	 * @param timestamp The timestamp.
	 */
	public PendingConfirm(@Nullable CorrelationData correlationData, long timestamp) {
		this.correlationData = correlationData;
		this.timestamp = timestamp;
	}

	/**
	 * The correlation data supplied by the client when sending the message
	 * corresponding to this confirmation.
	 * @return The correlation data.
	 */
	@Nullable
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
	public @Nullable String getCause() {
		return this.cause;
	}

	/**
	 * True if a returned message has been received.
	 * @return true if there is a return.
	 * @since 2.2.10
	 */
	public boolean isReturned() {
		return this.returned;
	}

	/**
	 * Indicate that a returned message has been received.
	 * @param isReturned true if there is a return.
	 * @since 2.2.10
	 */
	public void setReturned(boolean isReturned) {
		this.returned = isReturned;
	}

	/**
	 * Return true if a return has been passed to the listener or if no return has been
	 * received.
	 * @return false if an expected returned message has not been passed to the listener.
	 * @throws InterruptedException if interrupted.
	 * @since 2.2.10
	 */
	public boolean waitForReturnIfNeeded() throws InterruptedException {
		return !this.returned || this.latch.await(RETURN_CALLBACK_TIMEOUT, TimeUnit.SECONDS);
	}

	/**
	 * Count down the returned message latch; call after the listener has been called.
	 * @since 2.2.10
	 */
	public void countDown() {
		this.latch.countDown();
	}

	@Override
	public String toString() {
		return "PendingConfirm [correlationData=" + this.correlationData +
				(this.cause == null ? "" : " cause=" + this.cause) + "]";
	}

}
