/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.transaction.interceptor.RuleBasedTransactionAttribute;

/**
 * Subclass of {@link RuleBasedTransactionAttribute} that is aware that
 * listener exceptions are wrapped in {@link ListenerExecutionFailedException}s.
 * Allows users to control rollback based on the actual cause.
 *
 * @author Gary Russell
 * @since 1.6.6
 *
 */
@SuppressWarnings("serial")
public class ListenerFailedRuleBasedTransactionAttribute extends RuleBasedTransactionAttribute {

	@Override
	public boolean rollbackOn(Throwable ex) {
		if (ex instanceof ListenerExecutionFailedException) {
			return super.rollbackOn(ex.getCause());
		}
		else {
			return super.rollbackOn(ex);
		}
	}

}
