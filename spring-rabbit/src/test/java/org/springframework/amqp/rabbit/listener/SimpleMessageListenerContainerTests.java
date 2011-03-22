/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.listener;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * @author David Syer
 * 
 */
public class SimpleMessageListenerContainerTests {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testInconsistentTransactionConfiguration() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(new SingleConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(false);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setTransactionManager(new TestTransactionManager());
		expectedException.expect(IllegalStateException.class);
		container.afterPropertiesSet();
	}

	@Test
	public void testInconsistentAcknowledgeConfiguration() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(new SingleConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(true);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		expectedException.expect(IllegalStateException.class);
		container.afterPropertiesSet();
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}
}
