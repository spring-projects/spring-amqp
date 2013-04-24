/*
 * Copyright 2002-2013 the original author or authors.
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

package org.springframework.amqp.remoting.common;

import java.lang.reflect.Method;

import org.springframework.amqp.remoting.client.AmqpClientInterceptor;
import org.springframework.amqp.remoting.service.AmqpInvokerServiceExporter;

/**
 * A strategy to create a unique method identifier by which client and service can determine which method is to be
 * called. Used in both the {@link AmqpClientInterceptor} and the {@link AmqpInvokerServiceExporter} and has to match in
 * a pair of those.
 * 
 * <p>
 * The method identifier is put in the message header and passed along with the serialized arguments.
 * 
 * @author David Bilge
 * @since 1.2
 * 
 */
public interface MethodHeaderNamingStrategy {
	String generateMethodName(Method method);
}
