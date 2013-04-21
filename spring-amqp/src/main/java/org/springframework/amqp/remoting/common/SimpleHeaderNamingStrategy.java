/*
 * Copyright 2002-2012 the original author or authors.
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

package org.springframework.amqp.remoting.common;

import java.lang.reflect.Method;

/**
 * Uses the parameters suggested in the {@link Method#equals(Object)} method
 * with the omission of the declaring class: That should be unnecessary in the
 * given context.
 * 
 * <p>
 * Will create a String of the form
 * <code>returnType + " " + name + "(" + parameterTypes + ")"</code> which has
 * the side-effect of being human-readable.
 * 
 * @author David
 * 
 */
public class SimpleHeaderNamingStrategy implements MethodHeaderNamingStrategy {

	@Override
	public String generateMethodName(Method method) {
		String name = method.getName();
		String parameterTypes = serializeParameterTypes(method.getParameterTypes());
		String returnType = method.getReturnType().getCanonicalName();

		return returnType + " " + name + "(" + parameterTypes + ")";
	}

	private String serializeParameterTypes(Class<?>[] parameterTypes) {
		StringBuilder sb = new StringBuilder();

		for (Class<?> parameterType : parameterTypes) {
			sb.append(parameterType.getCanonicalName() + ",");
		}

		return sb.toString();
	}

}
