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
package org.apache.drill.exec.planner.logical.partition;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.prepare.RelOptTableImpl;
import net.hydromatic.optiq.util.BitSets;

import net.hydromatic.optiq.rules.java.JavaRules.EnumerableTableAccessRel;

import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.FileSystemPartitionDescriptor;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PruneScanRule extends RelOptRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PruneScanRule.class);

  public static final RelOptRule getFilter(QueryContext context) {
    return new PruneScanRule(context);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final EnumerableTableAccessRel scan = (EnumerableTableAccessRel) call.rel(1);
    return scan.getTable().unwrap(DrillTable.class).getSelection() instanceof FormatSelection;
  }

  final QueryContext context;

  private PruneScanRule(QueryContext context) {
    super(RelOptHelper.some(FilterRel.class, RelOptHelper.any(EnumerableTableAccessRel.class)), "PruneScanRule");
    this.context = context;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterRel filterRel = (FilterRel) call.rel(0);
    final EnumerableTableAccessRel scanRel = (EnumerableTableAccessRel) call.rel(1);
    final DrillTable table = scanRel.getTable().unwrap(DrillTable.class);

    PlannerSettings settings = context.getPlannerSettings();
    FileSystemPartitionDescriptor descriptor = new FileSystemPartitionDescriptor(settings.getFsPartitionColumnLabel());
    final BufferAllocator allocator = context.getAllocator();

    RexNode condition = filterRel.getCondition();

    Map<Integer, String> dirNames = Maps.newHashMap();
    List<String> fieldNames = scanRel.getRowType().getFieldNames();
    BitSet columnBitset = new BitSet();
    BitSet dirBitset = new BitSet();
    {
      int colIndex = 0;
      for(String field : fieldNames){
        final Integer dirIndex = descriptor.getIdIfValid(field);
        if(dirIndex != null){
          dirNames.put(dirIndex, field);
          dirBitset.set(dirIndex);
          columnBitset.set(colIndex);
        }
        colIndex++;
      }
    }

    if(dirBitset.isEmpty()){
      return;
    }

    FindPartitionConditions c = new FindPartitionConditions(columnBitset, filterRel.getCluster().getRexBuilder());
    c.analyze(condition);
    RexNode pruneCondition = c.getFinalCondition();

    if(pruneCondition == null){
      return;
    }

    // set up the partitions
    final FormatSelection origSelection = (FormatSelection)table.getSelection();
    final List<String> files = origSelection.getAsFiles();
    final String selectionRoot = origSelection.getSelection().selectionRoot;
    List<PathPartition> partitions = Lists.newLinkedList();

    // let's only deal with one batch of files for now.
    if(files.size() > Character.MAX_VALUE){
      return;
    }

    for(String f : files){
      partitions.add(new PathPartition(descriptor.getMaxHierarchyLevel(), selectionRoot, f));
    }

    final NullableBitVector output = new NullableBitVector(MaterializedField.create("", Types.optional(MinorType.BIT)), allocator);
    final VectorContainer container = new VectorContainer();

    try{
      final NullableVarCharVector[] vectors = new NullableVarCharVector[descriptor.getMaxHierarchyLevel()];
      for(int dirIndex : BitSets.toIter(dirBitset)){
        NullableVarCharVector vector = new NullableVarCharVector(MaterializedField.create(dirNames.get(dirIndex), Types.optional(MinorType.VARCHAR)), allocator);
        vector.allocateNew(5000, partitions.size());
        vectors[dirIndex] = vector;
        container.add(vector);
      }

      // populate partition vectors.
      int record = 0;
      for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
        final PathPartition partition = iter.next();
        for(int dirIndex : BitSets.toIter(dirBitset)){
          if(partition.dirs[dirIndex] == null){
            vectors[dirIndex].getMutator().setNull(record);
          }else{
            byte[] bytes = partition.dirs[dirIndex].getBytes(Charsets.UTF_8);
            vectors[dirIndex].getMutator().setSafe(record, bytes, 0, bytes.length);
          }
        }
      }

      for(NullableVarCharVector v : vectors){
        if(v == null){
          continue;
        }
        v.getMutator().setValueCount(partitions.size());
      }


      // materialize the expression
      logger.debug("Attempting to prune {}", pruneCondition);
      LogicalExpression expr = DrillOptiq.toDrill(new DrillParseContext(), scanRel, pruneCondition);
      ErrorCollectorImpl errors = new ErrorCollectorImpl();
      LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, container, errors, context.getFunctionRegistry());
      if (errors.getErrorCount() != 0) {
        logger.warn("Failure while materializing expression [{}].  Errors: {}", expr, errors);
      }

      output.allocateNew(partitions.size());
      InterpreterEvaluator.evaluate(partitions.size(), context, container, output, materializedExpr);
      record = 0;

      List<String> newFiles = Lists.newArrayList();
      for(Iterator<PathPartition> iter = partitions.iterator(); iter.hasNext(); record++){
        PathPartition part = iter.next();
        if(!output.getAccessor().isNull(record) && output.getAccessor().get(record) == 1){
          newFiles.add(part.file);
        }
      }

      if(newFiles.isEmpty()){
        newFiles.add(files.get(0));
      }

      if(newFiles.size() == files.size()){
        return;
      }

      logger.debug("Pruned {} => {}", files, newFiles);


      final FileSelection newFileSelection = new FileSelection(newFiles, origSelection.getSelection().selectionRoot, true);
      table.modifySelection(new FormatSelection(((FormatSelection)table.getSelection()).getFormat(), newFileSelection));

    }catch(Exception e){
      logger.warn("Exception while trying to prune partition.", e);
    }finally{
      container.clear();
      if(output !=null){
        output.clear();
      }
    }
  }

  private static class PathPartition {
    final String[] dirs;
    final String file;

    public PathPartition(int max, String selectionRoot, String file){
      this.file = file;
      int start = file.indexOf(selectionRoot) + selectionRoot.length();
      String postPath = file.substring(start);
      if(postPath.charAt(0) == '/'){
        postPath = postPath.substring(1);
      }
      String[] mostDirs = postPath.split("/");
      this.dirs = new String[max];
      int maxLoop = Math.min(max, mostDirs.length - 1);
      for(int i =0; i < maxLoop; i++){
        this.dirs[i] = mostDirs[i];
      }
    }


  }

}
