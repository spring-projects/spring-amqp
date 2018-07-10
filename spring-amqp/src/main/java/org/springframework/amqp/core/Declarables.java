/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.amqp.core;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A collection of {@link Declarable} objects; used to declare multiple objects on the
 * broker using a single bean declaration for the collection.
 *
 * @author Gary Russell
 * @since 2.1
 */
public class Declarables {

	private final Collection<Declarable> declarables = new ArrayList<>();

	public Declarables(Collection<Declarable> declarables) {
		this.declarables.addAll(declarables);
	}

	public Collection<Declarable> getDeclarables() {
		return this.declarables;
	}

	@Override
	public String toString() {
		return "Declarables [declarables=" + this.declarables + "]";
	}

}
