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
package org.springframework.erlang.core;

import org.springframework.erlang.support.converter.ErlangConverter;

/**
 * Encapsulates an Erlang control action with the appropriate ErlangConverter to use.
 * @author Helena Edelson
 */
public interface ControlAction {

    /**
     * Returns the key.
     * @return key as class type to allow the caller to know the implementation class
     * versus the changing function names in a Broker's internal API per version.
     */
    Class getKey();

    /**
     * Returns the Rabbit control module name.
     * @return the module name
     */
    String getModule();

    /**
     * Returns the Rabbit control function name.
     * @return the function name
     */
    String getFunction();

    /**
     * Returns the Spring Erlang converter to use
     * for the particular module:function pair.
     * @return org.springframework.erlang.support.converter.ErlangConverter
     */
    ErlangConverter getConverter();
    
}
