/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.NextReceiverPolicy;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.jspecify.annotations.Nullable;

/**
 * The {@link Connection} wrapper to open links on a session which is replaced when it is found closed.
 * Stream links, the default sender and {@code nextReceiver()} calls are not covered:
 * they use the sessions managed by the wrapped connection.
 *
 * @author Tanishq Choudhary
 *
 * @since 4.2
 *
 * @see SpringAmqpConnectionFactory
 */
final class SessionRecoveringConnection implements Connection {

	private final Lock sessionLock = new ReentrantLock();

	private final Connection delegate;

	private volatile @Nullable Session session;

	SessionRecoveringConnection(Connection delegate) {
		this.delegate = delegate;
	}

	@Override
	public Receiver openReceiver(String address) throws ClientException {
		return execute((session) -> session.openReceiver(address));
	}

	@Override
	public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
		return execute((session) -> session.openReceiver(address, receiverOptions));
	}

	@Override
	public Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException {
		return execute((session) -> session.openDurableReceiver(address, subscriptionName));
	}

	@Override
	public Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions)
			throws ClientException {

		return execute((session) -> session.openDurableReceiver(address, subscriptionName, receiverOptions));
	}

	@Override
	public Receiver openDynamicReceiver() throws ClientException {
		return execute(Session::openDynamicReceiver);
	}

	@Override
	public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException {
		return execute((session) -> session.openDynamicReceiver(dynamicNodeProperties));
	}

	@Override
	public Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException {
		return execute((session) -> session.openDynamicReceiver(receiverOptions));
	}

	@Override
	public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions)
			throws ClientException {

		return execute((session) -> session.openDynamicReceiver(dynamicNodeProperties, receiverOptions));
	}

	@Override
	public Sender openSender(String address) throws ClientException {
		return execute((session) -> session.openSender(address));
	}

	@Override
	public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
		return execute((session) -> session.openSender(address, senderOptions));
	}

	@Override
	public Sender openAnonymousSender() throws ClientException {
		return execute(Session::openAnonymousSender);
	}

	@Override
	public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
		return execute((session) -> session.openAnonymousSender(senderOptions));
	}

	/**
	 * Return the session this connection opens links on.
	 * @return the session, which may be closed if the peer has ended it since the last opened link.
	 */
	@Override
	public Session defaultSession() throws ClientException {
		return session();
	}

	@Override
	public Client client() {
		return this.delegate.client();
	}

	@Override
	public Future<Connection> openFuture() {
		return this.delegate.openFuture();
	}

	@Override
	public void close() {
		this.delegate.close();
	}

	@Override
	public void close(ErrorCondition error) {
		this.delegate.close(error);
	}

	@Override
	public Future<Connection> closeAsync() {
		return this.delegate.closeAsync();
	}

	@Override
	public Future<Connection> closeAsync(ErrorCondition error) {
		return this.delegate.closeAsync(error);
	}

	@Override
	public StreamReceiver openStreamReceiver(String address) throws ClientException {
		return this.delegate.openStreamReceiver(address);
	}

	@Override
	public StreamReceiver openStreamReceiver(String address, StreamReceiverOptions receiverOptions)
			throws ClientException {

		return this.delegate.openStreamReceiver(address, receiverOptions);
	}

	@Override
	public Sender defaultSender() throws ClientException {
		return this.delegate.defaultSender();
	}

	@Override
	public StreamSender openStreamSender(String address) throws ClientException {
		return this.delegate.openStreamSender(address);
	}

	@Override
	public StreamSender openStreamSender(String address, StreamSenderOptions senderOptions) throws ClientException {
		return this.delegate.openStreamSender(address, senderOptions);
	}

	@Override
	public Session openSession() throws ClientException {
		return this.delegate.openSession();
	}

	@Override
	public Session openSession(SessionOptions sessionOptions) throws ClientException {
		return this.delegate.openSession(sessionOptions);
	}

	@Override
	public Tracker send(Message<?> message) throws ClientException {
		return this.delegate.send(message);
	}

	@Override
	public Receiver nextReceiver() throws ClientException {
		return this.delegate.nextReceiver();
	}

	@Override
	public Receiver nextReceiver(NextReceiverPolicy policy) throws ClientException {
		return this.delegate.nextReceiver(policy);
	}

	@Override
	public Receiver nextReceiver(long timeout, TimeUnit unit) throws ClientException {
		return this.delegate.nextReceiver(timeout, unit);
	}

	@Override
	public Receiver nextReceiver(NextReceiverPolicy policy, long timeout, TimeUnit unit) throws ClientException {
		return this.delegate.nextReceiver(policy, timeout, unit);
	}

	@Override
	public Map<String, Object> properties() throws ClientException {
		return this.delegate.properties();
	}

	@Override
	public String[] offeredCapabilities() throws ClientException {
		return this.delegate.offeredCapabilities();
	}

	@Override
	public String[] desiredCapabilities() throws ClientException {
		return this.delegate.desiredCapabilities();
	}

	private <T> T execute(SessionCallback<T> callback) throws ClientException {
		Session sessionToUse = session();
		try {
			return callback.doInSession(sessionToUse);
		}
		catch (ClientIllegalStateException ex) {
			return callback.doInSession(replaceSession(sessionToUse));
		}
	}

	private Session session() throws ClientException {
		Session sessionToReturn = this.session;
		if (sessionToReturn == null) {
			this.sessionLock.lock();
			try {
				sessionToReturn = this.session;
				if (sessionToReturn == null) {
					sessionToReturn = this.delegate.openSession();
					this.session = sessionToReturn;
				}
			}
			finally {
				this.sessionLock.unlock();
			}
		}
		return sessionToReturn;
	}

	private Session replaceSession(Session closedSession) throws ClientException {
		this.sessionLock.lock();
		try {
			Session currentSession = this.session;
			if (currentSession != null && currentSession != closedSession) {
				return currentSession;
			}
			Session newSession = this.delegate.openSession();
			this.session = newSession;
			closedSession.closeAsync();
			return newSession;
		}
		finally {
			this.sessionLock.unlock();
		}
	}

	@FunctionalInterface
	private interface SessionCallback<T> {

		T doInSession(Session session) throws ClientException;

	}

}
