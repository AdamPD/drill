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
package org.apache.drill.exec.record;


import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;


public class BatchSchema implements Iterable<MaterializedField> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchSchema.class);
  final SelectionVectorMode selectionVectorMode;
  private final List<MaterializedField> fields;

  BatchSchema(SelectionVectorMode selectionVector, List<MaterializedField> fields) {
    this.fields = fields;
    this.selectionVectorMode = selectionVector;
  }

  public static SchemaBuilder newBuilder() {
    return new SchemaBuilder();
  }

  public int getFieldCount() {
    return fields.size();
  }

  public MaterializedField getColumn(int index) {
    if (index < 0 || index >= fields.size()) {
      return null;
    }
    return fields.get(index);
  }

  @Override
  public Iterator<MaterializedField> iterator() {
    return fields.iterator();
  }

  public SelectionVectorMode getSelectionVectorMode() {
    return selectionVectorMode;
  }

  @Override
  public BatchSchema clone() {
    List<MaterializedField> newFields = Lists.newArrayList();
    newFields.addAll(fields);
    return new BatchSchema(selectionVectorMode, newFields);
  }

  @Override
  public String toString() {
    return "BatchSchema [fields=" + fields + ", selectionVector=" + selectionVectorMode + "]";
  }

  public static enum SelectionVectorMode {
    NONE(-1, false), TWO_BYTE(2, true), FOUR_BYTE(4, true);

    public boolean hasSelectionVector;
    public final int size;
    SelectionVectorMode(int size, boolean hasSelectionVector) {
      this.size = size;
    }

    public static SelectionVectorMode[] DEFAULT = {NONE};
    public static SelectionVectorMode[] NONE_AND_TWO = {NONE, TWO_BYTE};
    public static SelectionVectorMode[] NONE_AND_FOUR = {NONE, FOUR_BYTE};
    public static SelectionVectorMode[] ALL = {NONE, TWO_BYTE, FOUR_BYTE};
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    result = prime * result + ((selectionVectorMode == null) ? 0 : selectionVectorMode.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BatchSchema other = (BatchSchema) obj;
    if (fields == null) {
      if (other.fields != null) {
        return false;
      }
    } else if (!fields.equals(other.fields)) {
      return false;
    }
    if (selectionVectorMode != other.selectionVectorMode) {
      return false;
    }
    return true;
  }

  private static boolean compareFields(MaterializedField[] fields1, MaterializedField[] fields2) {
    for (int i = 0; i < fields1.length; i++) {
      if (!fields1[i].getPath().equals(fields2[i].getPath()) ||
          !fields1[i].getType().equals(fields2[i].getType()) ||
          fields1[i].getChildren().size() != fields2[i].getChildren().size()) {
        return false;
      }
      if (fields1[i].getChildren().size() > 0 &&
          !compareFields(fields1[i].getChildren().toArray(new MaterializedField[0]),
                  fields2[i].getChildren().toArray(new MaterializedField[0]))) {
        return false;
      }
    }
    return true;
  }

  public boolean deepEquals(BatchSchema other) {
    if (this == other) {
      return true;
    }
    if (this == null || other == null) {
      return false;
    }
    if (this.getFieldCount() != other.getFieldCount()) {
      return false;
    }
    if (this.getSelectionVectorMode() != other.getSelectionVectorMode()) {
      return false;
    }
    return compareFields(this.fields.toArray(new MaterializedField[0]),
            other.fields.toArray(new MaterializedField[0]));
  }

  public BatchSchema deepClone() {
    List<MaterializedField> newFields = Lists.newArrayList();
    for (MaterializedField field : fields) {
      newFields.add(field.clone());
    }
    return new BatchSchema(selectionVectorMode, newFields);
  }
}
