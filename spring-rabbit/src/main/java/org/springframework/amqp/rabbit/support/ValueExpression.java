/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.TypedValue;
import org.springframework.expression.common.ExpressionUtils;

/**
 * A very simple hardcoded implementation of the {@link Expression}
 * interface that represents an immutable value.
 * It is used as value holder in the context of expression evaluation.
 *
 * @param <V> - The expected value type.
 *
 * @author Artem Bilan
 *
 * @since 1.4
 */
public class ValueExpression<V> implements Expression {

	/** Fixed value of this expression. */
	private final @Nullable V value;

	private final @Nullable Class<V> aClass;

	private final TypedValue typedResultValue;

	private final @Nullable TypeDescriptor typeDescriptor;

	@SuppressWarnings("unchecked")
	public ValueExpression(@Nullable V value) {
		this.value = value;
		this.aClass = (Class<V>) (this.value != null ? this.value.getClass() : null);
		this.typedResultValue = new TypedValue(this.value);
		this.typeDescriptor = this.typedResultValue.getTypeDescriptor();
	}

	@Override
	public @Nullable V getValue() throws EvaluationException {
		return this.value;
	}

	@Override
	public @Nullable V getValue(@Nullable Object rootObject) throws EvaluationException {
		return this.value;
	}

	@Override
	public @Nullable V getValue(EvaluationContext context) throws EvaluationException {
		return this.value;
	}

	@Override
	public @Nullable V getValue(EvaluationContext context, @Nullable Object rootObject) throws EvaluationException {
		return this.value;
	}

	@Override
	public <T> @Nullable T getValue(@Nullable Object rootObject, @Nullable Class<T> desiredResultType)
			throws EvaluationException {

		return getValue(desiredResultType);
	}

	@Override
	public <T> @Nullable T getValue(@Nullable Class<T> desiredResultType) throws EvaluationException {
		return ExpressionUtils.convertTypedValue(null, this.typedResultValue, desiredResultType);
	}

	@Override
	public <T> @Nullable T getValue(EvaluationContext context, @Nullable Object rootObject,
			@Nullable Class<T> desiredResultType)
			throws EvaluationException {

		return getValue(context, desiredResultType);
	}

	@Override
	public <T> @Nullable T getValue(EvaluationContext context, @Nullable Class<T> desiredResultType)
			throws EvaluationException {

		return ExpressionUtils.convertTypedValue(context, this.typedResultValue, desiredResultType);
	}

	@Override
	public @Nullable Class<V> getValueType() throws EvaluationException {
		return this.aClass;
	}

	@Override
	public @Nullable Class<V> getValueType(@Nullable Object rootObject) throws EvaluationException {
		return this.aClass;
	}

	@Override
	public @Nullable Class<V> getValueType(EvaluationContext context) throws EvaluationException {
		return this.aClass;
	}

	@Override
	public @Nullable Class<V> getValueType(EvaluationContext context, @Nullable Object rootObject)
			throws EvaluationException {

		return this.aClass;
	}

	@Override
	public @Nullable TypeDescriptor getValueTypeDescriptor() throws EvaluationException {
		return this.typeDescriptor;
	}

	@Override
	public @Nullable TypeDescriptor getValueTypeDescriptor(@Nullable Object rootObject) throws EvaluationException {
		return this.typeDescriptor;
	}

	@Override
	public @Nullable TypeDescriptor getValueTypeDescriptor(EvaluationContext context) throws EvaluationException {
		return this.typeDescriptor;
	}

	@Override
	public @Nullable TypeDescriptor getValueTypeDescriptor(EvaluationContext context, @Nullable Object rootObject)
			throws EvaluationException {

		return this.typeDescriptor;
	}

	@Override
	public boolean isWritable(EvaluationContext context) throws EvaluationException {
		return false;
	}

	@Override
	public boolean isWritable(EvaluationContext context, @Nullable Object rootObject) throws EvaluationException {
		return false;
	}

	@Override
	public boolean isWritable(@Nullable Object rootObject) throws EvaluationException {
		return false;
	}

	@Override
	public void setValue(EvaluationContext context, @Nullable Object value) throws EvaluationException {
		setValue(context, null, value);
	}

	@Override
	public void setValue(@Nullable Object rootObject, @Nullable Object value) throws EvaluationException {
		setValue(null, rootObject, value);
	}

	@Override
	public void setValue(@Nullable EvaluationContext context, @Nullable Object rootObject, @Nullable Object value)
			throws EvaluationException {

		throw new EvaluationException(getExpressionString(), "Cannot call setValue() on a ValueExpression");
	}

	@Override
	public String getExpressionString() {
		return this.value != null ? this.value.toString() : "null";
	}

}
