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
