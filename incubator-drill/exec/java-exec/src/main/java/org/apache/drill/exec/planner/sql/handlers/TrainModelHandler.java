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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.List;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.parser.SqlTrainModel;
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNode;

public class TrainModelHandler extends DefaultSqlHandler {

  public TrainModelHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    SqlTrainModel sqlTrainModel = unwrap(sqlNode, SqlTrainModel.class);

    try {
      // Convert the query in TMAS statement into a RelNode
      SqlNode validatedQuery = validateNode(sqlTrainModel.getQuery());
      RelNode relQuery = convertToRel(validatedQuery);

      List<String> mdlFiledNames = sqlTrainModel.getFieldNames();
      RelDataType queryRowType = relQuery.getRowType();

      if (mdlFiledNames.size() > 0) {
        // Field count should match.
        if (mdlFiledNames.size() != queryRowType.getFieldCount()) {
          return DirectPlan.createDirectPlan(context, false,
              "Model's field list and the model's query field list have different counts.");
        }

        // TMAS's query field list shouldn't have "*" when table's field list is specified.
        for (String field : queryRowType.getFieldNames()) {
          if (field.equals("*")) {
            return DirectPlan.createDirectPlan(context, false,
                "Model's query field list has a '*', which is invalid when model's field list is specified.");
          }
        }
      }

      // if the TMAS statement has model fields lists (ex. below), add a project rel to rename the query fields.
      // Ex. TRAIN MODEL mdlname(col1, medianOfCol2, avgOfCol3) AS
      //        SELECT col1, median(col2), avg(col3) FROM sourcetbl GROUP BY col1 ;
      if (mdlFiledNames.size() > 0) {
        // create rowtype to which the select rel needs to be casted.
        RelDataType rowType = new DrillFixedRelDataTypeImpl(planner.getTypeFactory(), mdlFiledNames);

        relQuery = RelOptUtil.createCastRel(relQuery, rowType, true);
      }

      SchemaPlus schema = findSchema(context.getRootSchema(), context.getNewDefaultSchema(),
          sqlTrainModel.getSchemaPath());

      AbstractSchema drillSchema = getDrillSchema(schema);

      if (!drillSchema.isMutable()) {
        return DirectPlan.createDirectPlan(context, false, String.format("Current schema '%s' is not a mutable schema. " +
            "Can't create models in this schema.", drillSchema.getFullSchemaName()));
      }

      String newMdlName = sqlTrainModel.getName();
      if (schema.getTable(newMdlName) != null) {
        return DirectPlan.createDirectPlan(context, false, String.format("Model '%s' already exists.", newMdlName));
      }

      log("Optiq Logical", relQuery);

      // Convert the query to Drill Logical plan and insert a writer operator on top.
      DrillRel drel = convertToDrel(relQuery, drillSchema, newMdlName);
      log("Drill Logical", drel);
      Prel prel = convertToPrel(drel);
      log("Drill Physical", prel);
      PhysicalOperator pop = convertToPop(prel);
      PhysicalPlan plan = convertToPlan(pop);
      log("Drill Plan", plan);

      return plan;
    } catch(Exception e) {
      logger.error("Failed to create model '{}'", sqlTrainModel.getName(), e);
      return DirectPlan.createDirectPlan(context, false, String.format("Error: %s", e.getMessage()));
    }
  }

  private DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String modelName) throws RelConversionException {
    RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_RULES,
        relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);

    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    }

    //TODO: Shadi create a schema.createNewModel(modelName)
    DrillWriterRel writerRel = new DrillWriterRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(),
        convertedRelNode, schema.trainNewModel(modelName));
    return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
  }

}
