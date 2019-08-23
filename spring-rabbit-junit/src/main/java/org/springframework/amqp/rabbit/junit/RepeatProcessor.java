/*
 * Copyright 2002-2019 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.test.annotation.Repeat;

/**
 * A JUnit method &#064;Rule that looks at Spring repeat annotations on methods and executes the test multiple times
 * (without re-initializing the test case if necessary). To avoid re-initializing use the {@link #isInitialized()}
 * method to protect the &#64;Before and &#64;After methods.
 * @deprecated in favor of JUnit 5 {@link org.junit.jupiter.api.RepeatedTest}.
 *
 * @author Dave Syer
 *
 */
@Deprecated
public class RepeatProcessor implements MethodRule {

	private static final Log LOGGER = LogFactory.getLog(RepeatProcessor.class);

	private final int concurrency;

	private volatile boolean initialized = false;

	private volatile boolean finalizing = false;

	public RepeatProcessor() {
		this(0);
	}

	public RepeatProcessor(int concurrency) {
		this.concurrency = concurrency < 0 ? 0 : concurrency;
	}

	@Override
	public Statement apply(final Statement base, FrameworkMethod method, final Object target) {

		Repeat repeat = AnnotationUtils.findAnnotation(method.getMethod(), Repeat.class);
		if (repeat == null) {
			return base;
		}

		final int repeats = repeat.value();
		if (repeats <= 1) {
			return base;
		}

		initializeIfNecessary(target);

		if (this.concurrency <= 0) {
			return new Statement() {
				@Override
				public void evaluate() throws Throwable {
					try {
						for (int i = 0; i < repeats; i++) {
							try {
								base.evaluate();
							}
							catch (Throwable t) { // NOSONAR
								throw new IllegalStateException(
										"Failed on iteration: " + i + " of " + repeats + " (started at 0)", t);
							}
						}
					}
					finally {
						finalizeIfNecessary(target);
					}
				}
			};
		}
		return new Statement() { // NOSONAR
			@Override
			public void evaluate() throws Throwable {
				List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
				ExecutorService executor = Executors.newFixedThreadPool(RepeatProcessor.this.concurrency);
				CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(executor);
				try {
					for (int i = 0; i < repeats; i++) {
						final int count = i;
						results.add(completionService.submit(new Callable<Boolean>() {
							@Override
							public Boolean call() {
								try {
									base.evaluate();
								}
								catch (Throwable t) { // NOSONAR
									throw new IllegalStateException("Failed on iteration: " + count, t);
								}
								return true;
							}
						}));
					}
					for (int i = 0; i < repeats; i++) {
						Future<Boolean> future = completionService.take();
						assertThat(future.get()).as("Null result from completer").isTrue();
					}
				}
				finally {
					executor.shutdownNow();
					finalizeIfNecessary(target);
				}
			}
		};
	}

	private void finalizeIfNecessary(Object target) {
		this.finalizing = true;
		List<FrameworkMethod> afters = new TestClass(target.getClass()).getAnnotatedMethods(After.class);
		try {
			if (!afters.isEmpty()) {
				LOGGER.debug("Running @After methods");
				try {
					new RunAfters(new Statement() {
						@Override
						public void evaluate() {
						}
					}, afters, target).evaluate();
				}
				catch (Throwable e) { // NOSONAR
					fail("Unexpected throwable " + e);
				}
			}
		}
		finally {
			this.finalizing = false;
		}
	}

	private void initializeIfNecessary(Object target) {
		TestClass testClass = new TestClass(target.getClass());
		List<FrameworkMethod> befores = testClass.getAnnotatedMethods(Before.class);
		if (!befores.isEmpty()) {
			LOGGER.debug("Running @Before methods");
			try {
				new RunBefores(new Statement() {
					@Override
					public void evaluate() {
					}
				}, befores, target).evaluate();
			}
			catch (Throwable e) { // NOSONAR
				fail("Unexpected throwable " + e);
			}
			this.initialized = true;
		}
		if (!testClass.getAnnotatedMethods(After.class).isEmpty()) {
			this.initialized = true;
		}
	}

	public boolean isInitialized() {
		return this.initialized;
	}

	public boolean isFinalizing() {
		return this.finalizing;
	}

	public int getConcurrency() {
		return this.concurrency > 0 ? this.concurrency : 1;
	}

}
