/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import parquet.filter2.predicate.FilterApi;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.io.api.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterPredicateSerializer {
    private static final Map<Class<?>, String> classToTypeMap = new HashMap<Class<?>, String>();

    static {
        classToTypeMap.put(Binary.class, "binary");
        classToTypeMap.put(Boolean.class, "boolean");
        classToTypeMap.put(Float.class, "float");
        classToTypeMap.put(Double.class, "double");
        classToTypeMap.put(Long.class, "long");
        classToTypeMap.put(Integer.class, "int");
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "op")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = AndOperator.class, name = "and"),
            @JsonSubTypes.Type(value = OrOperator.class, name = "or"),
            @JsonSubTypes.Type(value = NotOperator.class, name = "not"),
            @JsonSubTypes.Type(value = EqOperator.class, name = "eq"),
            @JsonSubTypes.Type(value = NotEqOperator.class, name = "notEq"),
            @JsonSubTypes.Type(value = LtOperator.class, name = "lt"),
            @JsonSubTypes.Type(value = LtEqOperator.class, name = "ltEq"),
            @JsonSubTypes.Type(value = GtOperator.class, name = "gt"),
            @JsonSubTypes.Type(value = GtEqOperator.class, name = "gtEq") })
    private static interface SerializedOperator {
        <R> R accept(Visitor<R> operator);

        public interface Visitor<R> {
            R visit(AndOperator op);
            R visit(OrOperator op);
            R visit(NotOperator op);
            R visit(EqOperator op);
            R visit(NotEqOperator op);
            R visit(LtOperator op);
            R visit(LtEqOperator op);
            R visit(GtOperator op);
            R visit(GtEqOperator op);
        }
    }

    private abstract static class BinaryOperator implements SerializedOperator {
        private final List<SerializedOperator> operators;

        public BinaryOperator(
                @JsonProperty("ops") List<SerializedOperator> operators)
        {
            this.operators = operators;
        }

        @JsonProperty("ops")
        public List<SerializedOperator> getOperators() {
            return operators;
        }

        public abstract <R> R accept(Visitor<R> operator);
    }

    private static class AndOperator extends BinaryOperator {
        public AndOperator(
                @JsonProperty("ops") List<SerializedOperator> operators)
        {
            super(operators);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class OrOperator extends BinaryOperator {
        public OrOperator(
                @JsonProperty("ops") List<SerializedOperator> operators)
        {
            super(operators);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class NotOperator implements SerializedOperator {
        private final SerializedOperator predicate;

        public NotOperator(
                @JsonProperty("predicate") SerializedOperator predicate)
        {
            this.predicate = predicate;
        }

        @JsonProperty("predicate")
        public SerializedOperator getPredicate() {
            return predicate;
        }

        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private abstract static class ComparisonOperator implements SerializedOperator {
        private final String columnPath;
        private final String columnType;
        private final Object value;

        public ComparisonOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            this.columnPath = columnPath;
            this.columnType = columnType;
            this.value = value;
        }

        @JsonProperty("path")
        public String getColumnPath() {
            return columnPath;
        }

        @JsonProperty("type")
        public String getColumnType() {
            return columnType;
        }

        @JsonProperty("value")
        public Object getValue() {
            return value;
        }

        public abstract <R> R accept(Visitor<R> operator);
    }

    private static class EqOperator extends ComparisonOperator {
        public EqOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            super(columnPath, columnType, value);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class NotEqOperator extends ComparisonOperator {
        public NotEqOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            super(columnPath, columnType, value);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class LtOperator extends ComparisonOperator {
        public LtOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            super(columnPath, columnType, value);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class LtEqOperator extends ComparisonOperator {
        public LtEqOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            super(columnPath, columnType, value);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class GtOperator extends ComparisonOperator {
        public GtOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            super(columnPath, columnType, value);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    private static class GtEqOperator extends ComparisonOperator {
        public GtEqOperator(
                @JsonProperty("path") String columnPath,
                @JsonProperty("type") String columnType,
                @JsonProperty("value") Object value)
        {
            super(columnPath, columnType, value);
        }

        @Override
        public <R> R accept(Visitor<R> operator) {
            return operator.visit(this);
        }
    }

    public static class Se extends StdSerializer<FilterPredicate> implements FilterPredicate.Visitor<SerializedOperator> {

        public Se() {
            super(FilterPredicate.class);
        }

        @Override
        public void serialize(FilterPredicate filterPredicate, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            Object entry = filterPredicate.accept(this);
            jsonGenerator.writeObject(entry);
        }

        @Override
        public <T extends Comparable<T>> SerializedOperator visit(Operators.Eq<T> eq) {
            return new EqOperator(eq.getColumn().getColumnPath().toDotString(), classToTypeMap.get(eq.getColumn().getColumnType()), eq.getValue());
        }

        @Override
        public <T extends Comparable<T>> SerializedOperator visit(Operators.NotEq<T> notEq) {
            return new NotEqOperator(notEq.getColumn().getColumnPath().toDotString(), classToTypeMap.get(notEq.getColumn().getColumnType()), notEq.getValue());
        }

        @Override
        public <T extends Comparable<T>> SerializedOperator visit(Operators.Lt<T> lt) {
            return new LtOperator(lt.getColumn().getColumnPath().toDotString(), classToTypeMap.get(lt.getColumn().getColumnType()), lt.getValue());
        }

        @Override
        public <T extends Comparable<T>> SerializedOperator visit(Operators.LtEq<T> ltEq) {
            return new LtEqOperator(ltEq.getColumn().getColumnPath().toDotString(), classToTypeMap.get(ltEq.getColumn().getColumnType()), ltEq.getValue());
        }

        @Override
        public <T extends Comparable<T>> SerializedOperator visit(Operators.Gt<T> gt) {
            return new GtOperator(gt.getColumn().getColumnPath().toDotString(), classToTypeMap.get(gt.getColumn().getColumnType()), gt.getValue());
        }

        @Override
        public <T extends Comparable<T>> SerializedOperator visit(Operators.GtEq<T> gtEq) {
            return new GtEqOperator(gtEq.getColumn().getColumnPath().toDotString(), classToTypeMap.get(gtEq.getColumn().getColumnType()), gtEq.getValue());
        }

        private BinaryOperator getBinaryOperator(Class<?> binaryClass, FilterPredicate leftPredicate, FilterPredicate rightPredicate) {
            SerializedOperator left = leftPredicate.accept(this);
            SerializedOperator right = rightPredicate.accept(this);

            ArrayList<SerializedOperator> operators = new ArrayList<SerializedOperator>();

            if (binaryClass.isInstance(left)) {
                operators.addAll(((BinaryOperator) left).getOperators());
            }
            else {
                operators.add(left);
            }

            if (binaryClass.isInstance(right)) {
                operators.addAll(((BinaryOperator) right).getOperators());
            }
            else {
                operators.add(right);
            }

            if (binaryClass == AndOperator.class) {
                return new AndOperator(operators);
            }
            if (binaryClass == OrOperator.class) {
                return new OrOperator(operators);
            }

            return null;
        }

        @Override
        public SerializedOperator visit(Operators.And and) {
            return getBinaryOperator(AndOperator.class, and.getLeft(), and.getRight());
        }

        @Override
        public SerializedOperator visit(Operators.Or or) {
            return getBinaryOperator(OrOperator.class, or.getLeft(), or.getRight());
        }

        @Override
        public SerializedOperator visit(Operators.Not not) {
            return new NotOperator(not.getPredicate().accept(this));
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> SerializedOperator visit(Operators.UserDefined<T, U> userDefined) {
            throw new IllegalArgumentException("User-defined predicates are not supported for serialization.");
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> SerializedOperator visit(Operators.LogicalNotUserDefined<T, U> logicalNotUserDefined) {
            throw new IllegalArgumentException("Logical not user-defined predicates are not supported for serialization.");
        }
    }

    public static class De extends StdDeserializer<FilterPredicate> implements SerializedOperator.Visitor<FilterPredicate> {
        public De() {
            super(FilterPredicate.class);
        }

        @Override
        public FilterPredicate deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            return jsonParser.readValueAs(SerializedOperator.class).accept(this);
        }

        @Override
        public FilterPredicate visit(AndOperator op) {
            FilterPredicate left = op.getOperators().get(0).accept(this);
            FilterPredicate right;
            if (op.getOperators().size() > 2) {
                right = new AndOperator(op.getOperators().subList(1, op.getOperators().size())).accept(this);
            }
            else {
                right = op.getOperators().get(1).accept(this);
            }

            return FilterApi.and(left, right);
        }

        @Override
        public FilterPredicate visit(OrOperator op) {
            FilterPredicate left = op.getOperators().get(0).accept(this);
            FilterPredicate right;
            if (op.getOperators().size() > 2) {
                right = new OrOperator(op.getOperators().subList(1, op.getOperators().size() - 1)).accept(this);
            }
            else {
                right = op.getOperators().get(1).accept(this);
            }

            return FilterApi.or(left, right);
        }

        @Override
        public FilterPredicate visit(NotOperator op) {
            return FilterApi.not(op.getPredicate().accept(this));
        }

        private Double getDouble(Object value) {
            if (value instanceof Double) {
                return (Double) value;
            }
            if (value instanceof Integer) {
                return ((Integer) value).doubleValue();
            }
            if (value instanceof Long) {
                return ((Long) value).doubleValue();
            }

            return null;
        }

        private Float getFloat(Object value) {
            if (value instanceof Float) {
                return (Float) value;
            }
            if (value instanceof Double) {
                return ((Double) value).floatValue();
            }

            return null;
        }

        private Long getLong(Object value) {
            if (value instanceof Long) {
                return (Long) value;
            }
            if (value instanceof Integer) {
                return ((Integer) value).longValue();
            }

            return null;
        }

        private Integer getInteger(Object value) {
            if (value instanceof Integer) {
                return (Integer) value;
            }
            if (value instanceof Long) {
                return ((Long) value).intValue();
            }

            return null;
        }

        @Override
        public FilterPredicate visit(EqOperator op) {
            String columnPath = op.getColumnPath();
            Object value = op.getValue();
            switch (op.getColumnType()) {
                case "binary":
                    return FilterApi.eq(FilterApi.binaryColumn(columnPath), (Binary) value);
                case "boolean":
                    return FilterApi.eq(FilterApi.booleanColumn(columnPath), (Boolean) value);
                case "float":
                    return FilterApi.eq(FilterApi.floatColumn(columnPath), getFloat(value));
                case "double":
                    return FilterApi.eq(FilterApi.doubleColumn(columnPath), getDouble(value));
                case "long":
                    return FilterApi.eq(FilterApi.longColumn(columnPath), getLong(value));
                case "int":
                    return FilterApi.eq(FilterApi.intColumn(columnPath), getInteger(value));
            }
            return null;
        }

        @Override
        public FilterPredicate visit(NotEqOperator op) {
            String columnPath = op.getColumnPath();
            Object value = op.getValue();
            switch (op.getColumnType()) {
                case "binary":
                    return FilterApi.notEq(FilterApi.binaryColumn(columnPath), (Binary) value);
                case "boolean":
                    return FilterApi.notEq(FilterApi.booleanColumn(columnPath), (Boolean) value);
                case "float":
                    return FilterApi.notEq(FilterApi.floatColumn(columnPath), getFloat(value));
                case "double":
                    return FilterApi.notEq(FilterApi.doubleColumn(columnPath), getDouble(value));
                case "long":
                    return FilterApi.notEq(FilterApi.longColumn(columnPath), getLong(value));
                case "int":
                    return FilterApi.notEq(FilterApi.intColumn(columnPath), getInteger(value));
            }
            return null;
        }

        @Override
        public FilterPredicate visit(LtOperator op) {
            String columnPath = op.getColumnPath();
            Object value = op.getValue();
            switch (op.getColumnType()) {
                case "binary":
                    return FilterApi.lt(FilterApi.binaryColumn(columnPath), (Binary) value);
                case "float":
                    return FilterApi.lt(FilterApi.floatColumn(columnPath), getFloat(value));
                case "double":
                    return FilterApi.lt(FilterApi.doubleColumn(columnPath), getDouble(value));
                case "long":
                    return FilterApi.lt(FilterApi.longColumn(columnPath), getLong(value));
                case "int":
                    return FilterApi.lt(FilterApi.intColumn(columnPath), getInteger(value));
            }
            return null;
        }

        @Override
        public FilterPredicate visit(LtEqOperator op) {
            String columnPath = op.getColumnPath();
            Object value = op.getValue();
            switch (op.getColumnType()) {
                case "binary":
                    return FilterApi.ltEq(FilterApi.binaryColumn(columnPath), (Binary) value);
                case "float":
                    return FilterApi.ltEq(FilterApi.floatColumn(columnPath), getFloat(value));
                case "double":
                    return FilterApi.ltEq(FilterApi.doubleColumn(columnPath), getDouble(value));
                case "long":
                    return FilterApi.ltEq(FilterApi.longColumn(columnPath), getLong(value));
                case "int":
                    return FilterApi.ltEq(FilterApi.intColumn(columnPath), getInteger(value));
            }
            return null;
        }

        @Override
        public FilterPredicate visit(GtOperator op) {
            String columnPath = op.getColumnPath();
            Object value = op.getValue();
            switch (op.getColumnType()) {
                case "binary":
                    return FilterApi.gt(FilterApi.binaryColumn(columnPath), (Binary) value);
                case "float":
                    return FilterApi.gt(FilterApi.floatColumn(columnPath), getFloat(value));
                case "double":
                    return FilterApi.gt(FilterApi.doubleColumn(columnPath), getDouble(value));
                case "long":
                    return FilterApi.gt(FilterApi.longColumn(columnPath), getLong(value));
                case "int":
                    return FilterApi.gt(FilterApi.intColumn(columnPath), getInteger(value));
            }
            return null;
        }

        @Override
        public FilterPredicate visit(GtEqOperator op) {
            String columnPath = op.getColumnPath();
            Object value = op.getValue();
            switch (op.getColumnType()) {
                case "binary":
                    return FilterApi.gtEq(FilterApi.binaryColumn(columnPath), (Binary) value);
                case "float":
                    return FilterApi.gtEq(FilterApi.floatColumn(columnPath), getFloat(value));
                case "double":
                    return FilterApi.gtEq(FilterApi.doubleColumn(columnPath), getDouble(value));
                case "long":
                    return FilterApi.gtEq(FilterApi.longColumn(columnPath), getLong(value));
                case "int":
                    return FilterApi.gtEq(FilterApi.intColumn(columnPath), getInteger(value));
            }
            return null;
        }
    }
}
