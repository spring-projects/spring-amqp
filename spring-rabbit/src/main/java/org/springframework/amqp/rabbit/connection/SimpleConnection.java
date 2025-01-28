/*
 * Copyright 2002-2025 the original author or authors.
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

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.Nullable;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.NetworkConnection;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;

import org.springframework.amqp.AmqpResourceNotAvailableException;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.util.ObjectUtils;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Simply a Connection.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Salk Lee
 *
 * @since 1.0
 */
public class SimpleConnection implements Connection, NetworkConnection {

	private final com.rabbitmq.client.Connection delegate;

	private final int closeTimeout;

	private volatile boolean explicitlyClosed;

	@Nullable
	private final BackOffExecution backOffExecution;

    public SimpleConnection(com.rabbitmq.client.Connection delegate, int closeTimeout) {
        this(delegate, closeTimeout, null);
    }

	/**
	 * Construct an instance with the {@link org.springframework.util.backoff.BackOffExecution} arguments.
	 * @param delegate delegate connection
	 * @param closeTimeout the time of physical close time out
	 * @param backOffExecution backOffExecution is nullable
	 * @since 3.1.3
	 */
	public SimpleConnection(com.rabbitmq.client.Connection delegate, int closeTimeout,
	@Nullable BackOffExecution backOffExecution) {
		this.delegate = delegate;
		this.closeTimeout = closeTimeout;
		this.backOffExecution = backOffExecution;
	}

	@Override
	public Channel createChannel(boolean transactional) {
		try {
			Channel channel = this.delegate.createChannel();
			while (channel == null && this.backOffExecution != null) {
				long interval = this.backOffExecution.nextBackOff();
				if (interval == BackOffExecution.STOP) {
					break;
				}
				Thread.sleep(interval);
				channel = this.delegate.createChannel();
			}
			if (channel == null) {
				throw new AmqpResourceNotAvailableException("The channelMax limit is reached. Try later.");
			}
			if (transactional) {
				// Just created so we want to start the transaction
				channel.txSelect();
			}
			return channel;
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AmqpTimeoutException("Interrupted while creating a new channel", e);
		}
		catch (IOException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	public void close() {
		try {
			this.explicitlyClosed = true;
			// let the physical close time out if necessary
			this.delegate.close(this.closeTimeout);
		}
		catch (AlreadyClosedException e) {
			// Ignore
		}
		catch (IOException e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	/**
	 * True if the connection is open.
	 * @return true if the connection is open
	 * @throws AutoRecoverConnectionNotCurrentlyOpenException if the connection is an
	 * {@link AutorecoveringConnection} and is currently closed; this is required to
	 * prevent the {@link CachingConnectionFactory} from discarding this connection
	 * and opening a new one, in which case the "old" connection would eventually be recovered
	 * and orphaned - also any consumers belonging to it might be recovered too
	 * and the broker will deliver messages to them when there is no code actually running
	 * to deal with those messages (when using the {@code SimpleMessageListenerContainer}).
	 * If we have actually closed the connection
	 * (e.g. via {@link CachingConnectionFactory#resetConnection()}) this will return false.
	 */
	@Override
	public boolean isOpen() {
		if (!this.explicitlyClosed && this.delegate instanceof AutorecoveringConnection && !this.delegate.isOpen()) {
			throw new AutoRecoverConnectionNotCurrentlyOpenException("Auto recovery connection is not currently open");
		}
		return this.delegate != null && (this.delegate.isOpen());
	}


	@Override
	public int getLocalPort() {
		if (this.delegate instanceof NetworkConnection networkConn) {
			return networkConn.getLocalPort();
		}
		return 0;
	}

	@Override
	public void addBlockedListener(BlockedListener listener) {
		this.delegate.addBlockedListener(listener);
	}

	@Override
	public boolean removeBlockedListener(BlockedListener listener) {
		return this.delegate.removeBlockedListener(listener);
	}

	@Override
	public InetAddress getLocalAddress() {
		if (this.delegate instanceof NetworkConnection networkConn) {
			return networkConn.getLocalAddress();
		}
		return null;
	}

	@Override
	public InetAddress getAddress() {
		return this.delegate.getAddress();
	}

	@Override
	public int getPort() {
		return this.delegate.getPort();
	}

	@Override
	public com.rabbitmq.client.Connection getDelegate() {
		return this.delegate;
	}

	@Override
	public String toString() {
		return "SimpleConnection@"
				+ ObjectUtils.getIdentityHexString(this)
				+ " [delegate=" + this.delegate + ", localPort=" + getLocalPort() + "]";
	}

}
