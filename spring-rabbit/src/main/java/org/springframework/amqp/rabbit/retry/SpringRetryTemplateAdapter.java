/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import org.jspecify.annotations.Nullable;

import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.Retryable;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 * The {@link org.springframework.core.retry.RetryTemplate} adapter for
 * the {@link org.springframework.retry.support.RetryTemplate} instance.
 * The goal of this class is to simplify a migration path for existing retry infrastructure.
 * <p>
 * The super {@link org.springframework.core.retry.RetryTemplate} is configured with "no retry" policy
 * since all the retrying logic is delegated to the provided
 * {@link org.springframework.retry.support.RetryTemplate}.
 * <p>
 * NOTE: only stateless operations are delegated with implementation at the moment.
 *
 * @author Artem Bilan
 */
public class SpringRetryTemplateAdapter extends org.springframework.core.retry.RetryTemplate {

	private final RetryTemplate retryTemplate;

	private @Nullable RecoveryCallback<?> recoveryCallback;

	public SpringRetryTemplateAdapter(RetryTemplate retryTemplate) {
		super(throwable -> false);
		this.retryTemplate = retryTemplate;
	}

	@Override
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		throw new UnsupportedOperationException(
				"The 'org.springframework.retry.RetryPolicy' has to be used instead " +
						"on the provided 'org.springframework.retry.support.RetryTemplate'");
	}

	/**
	 * Set a {@link RecoveryCallback} to be used in the
	 * {@link org.springframework.retry.support.RetryTemplate#execute(RetryCallback, RecoveryCallback)}
	 * delegating operation.
	 * This is exactly a {@link RecoveryCallback} used previously in the target service alongside with a
	 * {@link org.springframework.retry.support.RetryTemplate}.
	 * @param recoveryCallback the {@link RecoveryCallback} to use.
	 */
	public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <R> @Nullable R execute(Retryable<? extends @Nullable R> retryable) throws RetryException {
		SpringRetryTemplateRetryableDelegate<R> retryableDelegate =
				new SpringRetryTemplateRetryableDelegate<>(retryable, this.retryTemplate,
						(RecoveryCallback<R>) this.recoveryCallback);

		return super.execute(retryableDelegate);
	}

	private record SpringRetryTemplateRetryableDelegate<R>(Retryable<? extends @Nullable R> retryable,
														   RetryTemplate retryTemplate,
														   @Nullable RecoveryCallback<R> recoveryCallback)
			implements Retryable<R> {

		@Override
		@SuppressWarnings("NullAway")
		public @Nullable R execute() throws Throwable {
			return this.retryTemplate.execute((ctx) -> this.retryable.execute(), this.recoveryCallback);
		}

	}

}
