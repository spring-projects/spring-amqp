/*
 * Copyright 2002-2024 the original author or authors.
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
