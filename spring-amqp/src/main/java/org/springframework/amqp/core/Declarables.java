/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.amqp.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A collection of {@link Declarable} objects; used to declare multiple objects on the
 * broker using a single bean declaration for the collection.
 *
 * @author Gary Russell
 * @author Björn Michael
 * @since 2.1
 */
public class Declarables {

	private final Collection<Declarable> declarables = new ArrayList<>();

	public Declarables(Declarable... declarables) {
		if (!ObjectUtils.isEmpty(declarables)) {
			this.declarables.addAll(Arrays.asList(declarables));
		}
	}

	public Declarables(Collection<? extends Declarable> declarables) {
		Assert.notNull(declarables, "declarables cannot be null");
		this.declarables.addAll(declarables);
	}

	public Collection<Declarable> getDeclarables() {
		return this.declarables;
	}

	/**
	 * Return the elements that are instances of the provided class.
	 * @param <T> The type.
	 * @param type the type's class.
	 * @return the filtered list.
	 * @since 2.2
	 */
	public <T extends Declarable> List<T> getDeclarablesByType(Class<? extends T> type) {
		return this.declarables.stream()
				.filter(type::isInstance)
				.map(type::cast)
				.collect(Collectors.toList());
	}

	@Override
	public String toString() {
		return "Declarables [declarables=" + this.declarables + "]";
	}

}
