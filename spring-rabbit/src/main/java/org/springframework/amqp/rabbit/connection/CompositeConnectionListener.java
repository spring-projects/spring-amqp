/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.rabbitmq.client.ShutdownSignalException;
import org.jspecify.annotations.Nullable;

/**
 * A composite listener that invokes its delegates in turn.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Ngoc Nhan
 *
 */
public class CompositeConnectionListener implements ConnectionListener {

	private List<ConnectionListener> delegates = new CopyOnWriteArrayList<>();

	@Override
	public void onCreate(@Nullable Connection connection) {
		this.delegates.forEach(delegate -> delegate.onCreate(connection));
	}

	@Override
	public void onClose(Connection connection) {
		this.delegates.forEach(delegate -> delegate.onClose(connection));
	}

	@Override
	public void onShutDown(ShutdownSignalException signal) {
		this.delegates.forEach(delegate -> delegate.onShutDown(signal));
	}

	@Override
	public void onFailed(Exception exception) {
		this.delegates.forEach(delegate -> delegate.onFailed(exception));
	}

	public void setDelegates(List<? extends ConnectionListener> delegates) {
		this.delegates = new ArrayList<>(delegates);
	}

	public void addDelegate(ConnectionListener delegate) {
		this.delegates.add(delegate);
	}

	public boolean removeDelegate(ConnectionListener delegate) {
		return this.delegates.remove(delegate);
	}

	public void clearDelegates() {
		this.delegates.clear();
	}

}
