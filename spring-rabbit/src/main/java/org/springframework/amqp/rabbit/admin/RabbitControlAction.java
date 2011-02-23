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
package org.springframework.amqp.rabbit.admin;

import org.springframework.erlang.core.ControlAction;
import org.springframework.erlang.support.converter.ErlangConverter;

/**
 * Encapsulates a Rabbit Erlang control action with the appropriate ErlangConverter to use.
 * @author Helena Edelson
 */
public class RabbitControlAction implements ControlAction {

    private ErlangConverter converter;

    private String module;

    private String function;

    private Class key;

    public RabbitControlAction() {
    }

    protected RabbitControlAction(String module, String function, ErlangConverter converter) {
        this(null, module, function, converter);
    }

    protected RabbitControlAction(Class key, String module, String function, ErlangConverter converter) {
        this.module = module;
        this.function = function;
        this.converter = converter;
        this.key = key != null ? key : this.getClass();
    } 

    public Class getKey() {
        return key;
    }

    public String getModule() {
        return module;
    }

    public String getFunction() {
        return function;
    }

    public ErlangConverter getConverter() {
        return converter;
    }
}
