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

package org.springframework.amqp.core;

/**
 * Acknowledgment modes supported by the listener container.
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public enum AcknowledgeMode {

	/**
	 * No acks - {@code autoAck=true} in {@code Channel.basicConsume()}.
	 */
	NONE,

	/**
	 * Manual acks - user must ack/nack via a channel aware listener.
	 */
	MANUAL,

	/**
	 * Auto - the container will issue the ack/nack based on whether
	 * the listener returns normally, or throws an exception.
	 * <p><em>Do not confuse with RabbitMQ {@code autoAck} which is
	 * represented by {@link #NONE} here</em>.
	 */
	AUTO;

	/**
	 * Return if transactions are allowed - if the mode is {@link #AUTO} or
	 * {@link #MANUAL}.
	 * @return true if transactions are allowed.
	 */
	public boolean isTransactionAllowed() {
		return this == AUTO || this == MANUAL;
	}

	/**
	 * Return if the mode is {@link #NONE} (which is called {@code autoAck}
	 * in RabbitMQ).
	 * @return true if the mode is {@link #NONE}.
	 */
	public boolean isAutoAck() {
		return this == NONE;
	}

	/**
	 * Return true if the mode is {@link #MANUAL}.
	 * @return true if manual.
	 */
	public boolean isManual() {
		return this == MANUAL;
	}

}
