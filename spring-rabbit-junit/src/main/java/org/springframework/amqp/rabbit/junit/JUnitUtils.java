/*
 * Copyright 2019 the original author or authors.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for JUnit rules and conditions.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public final class JUnitUtils {

	private static final Log LOGGER = LogFactory.getLog(JUnitUtils.class);

	private JUnitUtils() {
		super();
	}

	/**
	 * Return the parsed value if the provided property exists in the environment or system properties.
	 * @param property the property name.
	 * @return the parsed property value if it exists, false otherwise.
	 */
	public static boolean parseBooleanProperty(String property) {
		for (String value: new String[] { System.getenv(property), System.getProperty(property) }) {
			if (Boolean.parseBoolean(value)) {
				return true;
			}
		}
		return false;
	}

	public static LevelsContainer adjustLogLevels(String methodName, List<Class<?>> classes, List<String> categories,
			Level level) {

		Map<Class<?>, Level> oldLevels = new HashMap<Class<?>, Level>();
		Map<String, Level> oldCatLevels = new HashMap<String, Level>();
		Map<String, ch.qos.logback.classic.Level> oldLbLevels = new HashMap<>();
		classes.stream()
			.forEach(cls -> {
				oldLevels.put(cls, LogManager.getLogger(cls).getLevel());
				((Logger) LogManager.getLogger(cls)).setLevel(level);
			});
		categories.stream()
			.forEach(cat -> {
				oldCatLevels.put(cat, LogManager.getLogger(cat).getLevel());
				((Logger) LogManager.getLogger(cat)).setLevel(level);
				ch.qos.logback.classic.Logger lbLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(cat);
				oldLbLevels.put(cat, lbLogger.getLevel());
				lbLogger.setLevel(ch.qos.logback.classic.Level.toLevel(level.name()));
			});
		LOGGER.info("++++++++++++++++++++++++++++ "
				+ "Overridden log level setting for: "
				+ classes.stream()
					.map(Class::getSimpleName)
					.collect(Collectors.toList())
				+ " and " + categories.toString()
				+ " for test " + methodName);
		return new LevelsContainer(oldLevels, oldCatLevels, oldLbLevels);
	}

	public static void revertLevels(String methodName, LevelsContainer container) {
		LOGGER.info("++++++++++++++++++++++++++++ "
				+ "Restoring log level setting for test " + methodName);
		container.oldCatLevels.entrySet()
				.stream()
				.forEach(entry -> {
					if (!entry.getKey().contains("BrokerRunning")) {
						((Logger) LogManager.getLogger(entry.getKey())).setLevel(entry.getValue());
					}
				});
		container.oldLevels.entrySet()
				.stream()
				.forEach(entry -> {
					if (!entry.getKey().equals(BrokerRunning.class)) {
						((Logger) LogManager.getLogger(entry.getKey())).setLevel(entry.getValue());
					}
				});
		container.oldLbLevels.entrySet()
				.stream()
				.forEach(entry -> {
					((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(entry.getKey()))
							.setLevel(entry.getValue());
				});
	}

	public static class LevelsContainer {

		final Map<Class<?>, Level> oldLevels;

		final Map<String, Level> oldCatLevels;

		final Map<String, ch.qos.logback.classic.Level> oldLbLevels;

		public LevelsContainer(Map<Class<?>, Level> oldLevels, Map<String, Level> oldCatLevels,
				Map<String, ch.qos.logback.classic.Level> oldLbLevels) {

			this.oldLevels = oldLevels;
			this.oldCatLevels = oldCatLevels;
			this.oldLbLevels = oldLbLevels;
		}

	}

}
