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

package org.springframework.amqp.rabbit.connection;

import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 *
 */
public class CompositeChannelListener implements ChannelListener {

	private List<ChannelListener> delegates = new ArrayList<ChannelListener>();

	public void onCreate(Channel channel, boolean transactional) {
		for (ChannelListener delegate : this.delegates) {
			delegate.onCreate(channel, transactional);
		}
	}

	public void setDelegates(List<? extends ChannelListener> delegates) {
		this.delegates = new ArrayList<ChannelListener>(delegates);
	}

	public void addDelegate(ChannelListener delegate) {
		this.delegates.add(delegate);
	}

}
