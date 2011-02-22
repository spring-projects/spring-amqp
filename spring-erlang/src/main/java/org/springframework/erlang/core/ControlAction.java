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
