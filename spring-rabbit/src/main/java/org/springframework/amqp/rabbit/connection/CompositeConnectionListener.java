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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Dave Syer
 *
 */
public class CompositeConnectionListener implements ConnectionListener {

	private List<ConnectionListener> delegates = new CopyOnWriteArrayList<ConnectionListener>();

	public void onCreate(Connection connection) {
		for (ConnectionListener delegate : this.delegates) {
			delegate.onCreate(connection);
		}
	}

	public void onClose(Connection connection) {
		for (ConnectionListener delegate : this.delegates) {
			delegate.onClose(connection);
		}
	}

	public void setDelegates(List<? extends ConnectionListener> delegates) {
		this.delegates = new ArrayList<ConnectionListener>(delegates);
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
