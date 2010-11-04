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

package org.springframework.amqp.rabbit.test;

import static org.junit.Assert.assertTrue;

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
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
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
 * A JUnit method &#064;Rule that looks at Spring repeat annotations on methods and
 * executes the test multiple times (without re-initializing the test case).
 * 
 * @author Dave Syer
 * @since 2.0
 * 
 */
public class RepeatProcessor implements MethodRule {

	private static final Log logger = LogFactory.getLog(RepeatProcessor.class);

	private final int concurrency;

	private volatile boolean initialized = false;

	public RepeatProcessor() {
		this(0);
	}

	public RepeatProcessor(int concurrency) {
		this.concurrency = concurrency < 0 ? 0 : concurrency;
	}

	public Statement apply(final Statement base, FrameworkMethod method, Object target) {

		Repeat repeat = AnnotationUtils.findAnnotation(method.getMethod(), Repeat.class);
		if (repeat == null) {
			return base;
		}

		final int repeats = repeat.value();
		if (repeats <= 1) {
			return base;
		}

		initializeIfNecessary(target);

		try {
			if (concurrency <= 0) {
				return new Statement() {
					@Override
					public void evaluate() throws Throwable {
						for (int i = 0; i < repeats; i++) {
							try {
								base.evaluate();
							}
							catch (Throwable t) {
								throw new IllegalStateException("Failed on iteration: " + i, t);
							}
						}
					}
				};
			}
			return new Statement() {
				@Override
				public void evaluate() throws Throwable {
					List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
					ExecutorService executor = Executors.newFixedThreadPool(concurrency);
					CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(executor);
					try {
						for (int i = 0; i < repeats; i++) {
							final int count = i;
							results.add(completionService.submit(new Callable<Boolean>() {
								public Boolean call() {
									try {
										base.evaluate();
									}
									catch (Throwable t) {
										throw new IllegalStateException("Failed on iteration: " + count, t);
									}
									return true;
								}
							}));
						}
						for (int i = 0; i < repeats; i++) {
							Future<Boolean> future = completionService.take();
							assertTrue("Null result from completer", future.get());
						}
					}
					finally {
						executor.shutdownNow();
					}
				}
			};
		}
		finally {
			finalizeIfNecessary(target);
		}
	}

	private void finalizeIfNecessary(Object target) {
		List<FrameworkMethod> afters = new TestClass(target.getClass()).getAnnotatedMethods(After.class);
		if (!afters.isEmpty()) {
			logger.debug("Running @After methods");
			try {
				new RunAfters(new Statement() {
					public void evaluate() {
					}
				}, afters, target).evaluate();
			}
			catch (Throwable e) {
				Assert.assertThat(e, CoreMatchers.not(CoreMatchers.anything()));
			}
		}
	}

	private void initializeIfNecessary(Object target) {
		TestClass testClass = new TestClass(target.getClass());
		List<FrameworkMethod> befores = testClass.getAnnotatedMethods(Before.class);
		if (!befores.isEmpty()) {
			logger.debug("Running @Before methods");
			try {
				new RunBefores(new Statement() {
					public void evaluate() {
					}
				}, befores, target).evaluate();
			}
			catch (Throwable e) {
				Assert.assertThat(e, CoreMatchers.not(CoreMatchers.anything()));
			}
			initialized = true;
		}
		if (!testClass.getAnnotatedMethods(After.class).isEmpty()) {
			initialized = true;
		}
	}

	public boolean isInitialized() {
		return initialized;
	}

	public int getConcurrency() {
		return concurrency > 0 ? concurrency : 1;
	}
}