/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Ngoc Nhan
 *
 */
public class CompositeChannelListener implements ChannelListener {

	private List<ChannelListener> delegates = new ArrayList<>();

	@Override
	public void onCreate(Channel channel, boolean transactional) {
		for (ChannelListener delegate : this.delegates) {
			delegate.onCreate(channel, transactional);
		}
	}

	@Override
	public void onShutDown(ShutdownSignalException signal) {
		for (ChannelListener delegate : this.delegates) {
			delegate.onShutDown(signal);
		}
	}

	public void setDelegates(List<? extends ChannelListener> delegates) {
		this.delegates = new ArrayList<>(delegates);
	}

	public void addDelegate(ChannelListener delegate) {
		this.delegates.add(delegate);
	}

}
