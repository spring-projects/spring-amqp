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

package org.springframework.amqp.rabbit.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.rabbit.junit.BrokerRunning;

/**
 * A JUnit method &#064;Rule that changes the logger level for a set of classes
 * while a test method is running. Useful for performance or scalability tests
 * where we don't want to generate a large log in a tight inner loop.
 *
 * As well as adjusting Log4j, this also adjusts loggers for logback. The amqp-client
 * library now uses slf4j. Since we have logback on the CP (for the appender)
 * we can't add the slf4j-log4j bridge as well.
 *
 * @author Dave Syer
 * @author Artem Bilan
 * @author Gary Russell
 *
 */
public class LogLevelAdjuster implements MethodRule {

	private static final Log logger = LogFactory.getLog(LogLevelAdjuster.class);

	private final List<Class<?>> classes;

	private List<String> categories;

	private final Level level;

	public LogLevelAdjuster(Level level, Class<?>... classes) {
		this.level = level;
		this.classes = new ArrayList<>(Arrays.asList(classes));
		this.classes.add(getClass());
		this.categories = Collections.emptyList();
	}

	public LogLevelAdjuster categories(String... categories) {
		this.categories = new ArrayList<>(Arrays.asList(categories));
		return this;
	}

	@Override
	public Statement apply(final Statement base, FrameworkMethod method, Object target) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				Map<Class<?>, Level> oldLevels = new HashMap<Class<?>, Level>();
				Map<String, Level> oldCatLevels = new HashMap<String, Level>();
				LogLevelAdjuster.this.classes.stream()
					.forEach(cls -> {
						oldLevels.put(cls, LogManager.getLogger(cls).getLevel());
						((Logger) LogManager.getLogger(cls)).setLevel(LogLevelAdjuster.this.level);
					});
				LogLevelAdjuster.this.categories.stream()
					.forEach(cat -> {
						oldCatLevels.put(cat, LogManager.getLogger(cat).getLevel());
						((Logger) LogManager.getLogger(cat)).setLevel(LogLevelAdjuster.this.level);
					});
				Map<String, ch.qos.logback.classic.Level> oldLbLevels = applyLogBack();
				logger.debug("++++++++++++++++++++++++++++ "
						+ "Overridden log level setting for: "
						+ LogLevelAdjuster.this.classes.stream()
							.map(Class::getSimpleName)
							.collect(Collectors.toList())
						+ " and " + LogLevelAdjuster.this.categories
						+ " for test " + method.getName());
				try {
					base.evaluate();
				}
				finally {
					logger.debug("++++++++++++++++++++++++++++ "
							+ "Restoring log level setting for test " + method.getName());
					LogLevelAdjuster.this.categories.stream()
						.forEach(cat -> {
							if (!cat.contains("BrokerRunning")) {
								((Logger) LogManager.getLogger(cat)).setLevel(oldCatLevels.get(cat));
							}
						});
					LogLevelAdjuster.this.classes.stream()
						.forEach(cls -> {
							if (!cls.equals(BrokerRunning.class)) {
								((Logger) LogManager.getLogger(cls)).setLevel(oldLevels.get(cls));
							}
						});
					revertLogBack(oldLbLevels);
				}
			}

		};
	}

	private Map<String, ch.qos.logback.classic.Level> applyLogBack() {
		Map<String, ch.qos.logback.classic.Level> oldCatLevels = new HashMap<>();
		this.categories.stream().forEach(cat -> {
			ch.qos.logback.classic.Logger lbLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(cat);
			oldCatLevels.put(cat, lbLogger.getLevel());
			lbLogger.setLevel(ch.qos.logback.classic.Level.toLevel(this.level.name()));
		});
		return oldCatLevels;
	}

	private void revertLogBack(Map<String, ch.qos.logback.classic.Level> oldLevels) {
		this.categories.stream().forEach(cat -> {
			((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(cat)).setLevel(oldLevels.get(cat));
		});
	}

}
