/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.support.postprocessor;

import java.util.zip.Deflater;

/**
 * Base class for post processors based on {@link Deflater}.
 * @author Gary Russell
 *
 * @since 1.4.2
 *
 */
public abstract class AbstractDeflaterPostProcessor extends AbstractCompressingPostProcessor {

	private int level = Deflater.BEST_SPEED;

	public AbstractDeflaterPostProcessor() {
	}

	public AbstractDeflaterPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	/**
	 * Set the deflater compression level.
	 * @param level the level (default {@link Deflater#BEST_SPEED}
	 * @see Deflater
	 */
	public void setLevel(int level) {
		this.level = level;
	}

	/**
	 * Get the deflater compression level.
	 * @return the level.
	 */
	public int getLevel() {
		return this.level;
	}

}
