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
package org.apache.drill.exec.planner.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.tools.FrameworkConfig;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.RuleSet;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.physical.DrillDistributionTraitDef;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.ExplainHandler;
import org.apache.drill.exec.planner.sql.handlers.SetOptionHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.parser.DrillSqlCall;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.rules.ReduceExpressionsRule;
import org.eigenbase.rel.rules.WindowedAggSplitterRule;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptCostFactory;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.relopt.hep.HepProgramBuilder;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;

public class DrillSqlWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);

  private final Planner planner;
  private final HepPlanner hepPlanner;
  public final static int LOGICAL_RULES = 0;
  public final static int PHYSICAL_MEM_RULES = 1;
  private final QueryContext context;

  public DrillSqlWorker(QueryContext context) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(DrillDistributionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    this.context = context;
    RelOptCostFactory costFactory = (context.getPlannerSettings().useDefaultCosting()) ?
        null : new DrillCostBase.DrillCostFactory() ;
    int idMaxLength = (int)context.getPlannerSettings().getIdentifierMaxLength();

    FrameworkConfig config = Frameworks.newConfigBuilder() //
        .parserConfig(new SqlParser.ParserConfigImpl(Lex.MYSQL, idMaxLength)) //
        .parserFactory(DrillParserWithCompoundIdConverter.FACTORY) //
        .defaultSchema(context.getNewDefaultSchema()) //
        .operatorTable(context.getDrillOperatorTable()) //
        .traitDefs(traitDefs) //
        .convertletTable(new DrillConvertletTable()) //
        .context(context.getPlannerSettings()) //
        .ruleSets(getRules(context)) //
        .costFactory(costFactory) //
        .build();
    this.planner = Frameworks.getPlanner(config);
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    builder.addRuleClass(WindowedAggSplitterRule.class);
    this.hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
    hepPlanner.addRule(WindowedAggSplitterRule.PROJECT);
  }

  private RuleSet[] getRules(QueryContext context) {
    StoragePluginRegistry storagePluginRegistry = context.getStorage();
    RuleSet drillPhysicalMem = DrillRuleSets.mergedRuleSets(
        DrillRuleSets.getPhysicalRules(context),
        storagePluginRegistry.getStoragePluginRuleSet());
    RuleSet[] allRules = new RuleSet[] {DrillRuleSets.getDrillBasicRules(context), drillPhysicalMem};
    return allRules;
  }

  public PhysicalPlan getPlan(String sql) throws SqlParseException, ValidationException, ForemanSetupException{
	  logger.info("SQL string = "+sql);
    return getPlan(sql, null);
  }

  public PhysicalPlan getPlan(String sql, Pointer<String> textPlan) throws ForemanSetupException {
    SqlNode sqlNode;
    logger.info("SQL textPlan= "+textPlan.value); 
    try {
    	
    	String originalSql = sql;
    	logger.info("SQL string with pointer= "+sql);
    	if(sql.toLowerCase().contains("qdm_train")){
    		String newSqlBefore = sql.substring(0, sql.toLowerCase().indexOf("qdm_"));
    		logger.info("before qdmFunction= "+newSqlBefore);
    		
    		int brackets=0;
    		int i=sql.toLowerCase().indexOf("qdm_")+4;
    		for(;i<sql.length();i++){
    			if(sql.charAt(i)=='(')
    				brackets++;
    			else if(sql.charAt(i)==')'){
    				brackets--;
    			}
    			
    			if(brackets>1 && sql.charAt(i)==','){
    				sql = sql.substring(0,i)+';'+sql.substring(i+1);
    			}
    			
    			if(sql.charAt(i)==')' && brackets==0)
    				break;
    		}
    		
    		logger.info("Transition with ; insterted= "+sql);
    		String newSqlAfter = sql.substring(i+1);
    		logger.info("after qdmFunction= "+newSqlAfter);
    		String qdmFunction = sql.substring(sql.toLowerCase().indexOf("qdm_"), i+1);
    		logger.info("qdmFunction= "+qdmFunction);
    		String functionName = qdmFunction.substring(0, qdmFunction.indexOf("("));
    		logger.info("functionName= "+functionName);
    		//TODO: Fix bug when the string has commas inside a function like concat
    		StringTokenizer st = new StringTokenizer(qdmFunction.substring(qdmFunction.indexOf("(")+1), ",");
    		if(st.countTokens()<4){
    			sql = originalSql;
    		} else {
    			String operation = st.nextToken();
	    		String args = st.nextToken();
//	    		String modelName = st.nextToken();
	    		logger.info("operation= "+operation);
	    		logger.info("args= "+args);
//	    		logger.info("modelName= "+modelName);
	    		String firstAttribute = st.nextToken();
	    		boolean hasMoreAttribtues = false;
	    		String concatString = "concat("+firstAttribute;
	    		while (st.hasMoreTokens()) {
	    			concatString+=","+"','"+","+st.nextToken();
	    			hasMoreAttribtues = true;
	    		}
	    		concatString+=")";
//	    		sql=newSqlBefore+functionName+"("+operation+","+args+","+modelName+","+(hasMoreAttribtues?concatString:firstAttribute)+newSqlAfter;
	    		sql=newSqlBefore+functionName+"("+operation+","+args+","+(hasMoreAttribtues?concatString:firstAttribute)+newSqlAfter;
	    		sql=sql.replace(';', ',');
    		}
    		logger.info("Final new train SQL statement= "+sql);
    	} else 	if(sql.toLowerCase().contains("qdm_test")){
    		String newSqlBefore = sql.substring(0, sql.toLowerCase().indexOf("qdm_"));
    		logger.info("before qdmFunction= "+newSqlBefore);
    		
    		int brackets=0;
    		int i=sql.toLowerCase().indexOf("qdm_")+4;
    		for(;i<sql.length();i++){
    			if(sql.charAt(i)=='(')
    				brackets++;
    			else if(sql.charAt(i)==')'){
    				brackets--;
    			}
    			
    			if(brackets>1 && sql.charAt(i)==','){
    				sql = sql.substring(0,i)+';'+sql.substring(i+1);
    			}
    			
    			if(sql.charAt(i)==')' && brackets==0)
    				break;
    		}
    		
    		logger.info("Transition with ; insterted= "+sql);
    		String newSqlAfter = sql.substring(i+1);
    		logger.info("after qdmFunction= "+newSqlAfter);
    		String qdmFunction = sql.substring(sql.toLowerCase().indexOf("qdm_"), i+1);
    		logger.info("qdmFunction= "+qdmFunction);
    		String functionName = qdmFunction.substring(0, qdmFunction.indexOf("("));
    		logger.info("functionName= "+functionName);
    		//TODO: Fix bug when the string has commas inside a function like concat
    		StringTokenizer st = new StringTokenizer(qdmFunction.substring(qdmFunction.indexOf("(")+1), ",");
    		if(st.countTokens()<4){
    			sql = originalSql;
    		} else {
    			String operation = st.nextToken();
	    		String args = st.nextToken();
//	    		String modelName = st.nextToken();
	    		logger.info("operation= "+operation);
	    		logger.info("args= "+args);
//	    		logger.info("modelName= "+modelName);
	    		String firstAttribute = st.nextToken();
	    		boolean hasMoreAttribtues = false;
	    		String concatString = "concat("+firstAttribute;
	    		while (st.hasMoreTokens()) {
	    			concatString+=","+"','"+","+st.nextToken();
	    			hasMoreAttribtues = true;
	    		}
	    		concatString+=")";
//	    		sql=newSqlBefore+functionName+"("+operation+","+args+","+modelName+","+(hasMoreAttribtues?concatString:firstAttribute)+newSqlAfter;
	    		sql=newSqlBefore+functionName+"("+operation+","+args+","+(hasMoreAttribtues?concatString:firstAttribute)+newSqlAfter;
	    		sql=sql.replace(';', ',');
    		}
    		logger.info("Final new test SQL statement= "+sql);
    	}
    	
    	if(sql.toLowerCase().contains("applying")){
    		Pattern pattern = Pattern.compile("from(.*?)applying");
    		Matcher matcher = pattern.matcher(sql.toLowerCase());
    		
    		String match="";
    		while(matcher.find()){
    			match = matcher.group(1);
    			logger.info("Loop Found applying statement:" + match);
    			matcher = pattern.matcher(match+"applying");
    			
    		}
    		
    		if (match.length()>0)
    		{
    			logger.info("Found applying statement:" + match);
    			
    			
    			String newSqlBefore = sql.toLowerCase().substring(0, sql.toLowerCase().indexOf(match));
    			
        		logger.info("before:" + newSqlBefore);
        		
        		String toParseString = sql.toLowerCase().substring(sql.toLowerCase().indexOf(match));
        		logger.info("parse string:" + toParseString);
        		
        		String firstArg = toParseString.substring(0,toParseString.indexOf("applying"));
        		logger.info("firstArg:" + firstArg);
        		
        		StringTokenizer st = new StringTokenizer(firstArg," ");
        		String a1="";
        		int brackets=0;
        		String token = st.nextToken();
        		logger.info("token:" + token);
        		if(!st.hasMoreTokens()){
        			a1=token;
    				logger.info("a1:" + a1);
        		}else{
	        		while(st.hasMoreTokens()){
	        			token = st.nextToken();	        			
	        			logger.info("token:" + token);
	        			if(token.equalsIgnoreCase("("))
	        				brackets++;
	        			else if(token.equalsIgnoreCase(")"))
	        				brackets--;
	        			if(brackets==0 && token.equalsIgnoreCase("as")){
	        				a1=st.nextToken();
	        				a1=a1.replace(")","");
	        				logger.info("a1:" + a1);
	        				break;
	        			} 
	        		}
        		}
        		
        		firstArg = "(SELECT 1 as d1a381f3g74, * FROM "+firstArg+" ) AS "+a1;
        		logger.info("firstArg modified:" + firstArg);
        		
        		
        		String newSqlAfter =sql.toLowerCase().substring(sql.toLowerCase().indexOf("applying")+9);
        		logger.info("newSqlAfter:" + newSqlAfter);
        		
        		st = new StringTokenizer(newSqlAfter," ");
        		String secondArg="";
        		String a2="";
        		brackets=0;
        		token = st.nextToken();
        		logger.info("token:" + token);
        		secondArg+=token+" ";
        		boolean hasBracket=false;
        		if(!st.hasMoreTokens()){
        			a2=token;
    				logger.info("a2:" + a2);
        		}else{
	        		while(st.hasMoreTokens()){
	        			token = st.nextToken();
	        			logger.info("token:" + token);
	        			secondArg+=token+" ";
	        			if(token.equalsIgnoreCase("("))
	        				brackets++;
	        			else if(token.equalsIgnoreCase(")"))
	        				brackets--;
	        			if(brackets==0 && token.equalsIgnoreCase("as")){
	        				a2=st.nextToken();
	        				if(a2.contains(")")){
	        					a2=a2.replace(")","");
		        				hasBracket=true;
	        				} 

	        				secondArg+=a2+" ";
	        				
	        				logger.info("a2:" + a2);
	        				break;
	        			} 
	        		}
        		}
        		logger.info("secondArg:" + secondArg);
        		
        		
        		secondArg = "(SELECT 1 as d1a381f3g73, * FROM "+secondArg+" ) AS "+a2;
        		logger.info("secondArg modified:" + secondArg);

    			sql=newSqlBefore+" "+firstArg+
        				" JOIN "+secondArg+ 
        				" ON ("+a1+".d1a381f3g74="+a2+".d1a381f3g73)"+(hasBracket?")":"");
    			while(st.hasMoreTokens()){
    				sql+=" "+st.nextToken(); 
    			}
    		
    		logger.info("final sql:" + sql);
        		
//        		
//        		StringTokenizer st = new StringTokenizer(toParseString," ");
//        	
//        		
//        			String ds1,ds2,alias1,alias2="";
//            		String token = st.nextToken();
//            		logger.info("token:" + token);
//                	ds1=token;
//                	logger.info("ds1:" + ds1);
//                	token = st.nextToken();
//                	logger.info("token:" + token);
//                	if(token.equalsIgnoreCase("as")){
//                		token = st.nextToken();
//                		logger.info("token:" + token);
//                		alias1 = token;
//                		logger.info("alias1:" + alias1);
//                		//skip allying token
//                		token = st.nextToken();
//                		logger.info("skip token:" + token);
//                	} else {
//                		alias1=ds1;
//                		logger.info("alias1:" + alias1);
//                	}
//                	
//                	token = st.nextToken();
//            		logger.info("token:" + token);
//            		ds2=token;
//                	logger.info("ds2:" + ds2);
//                	if(token.equalsIgnoreCase("as")){
//                		token = st.nextToken();
//                		logger.info("token:" + token);
//                		alias2 = token;
//                		logger.info("alias2:" + alias2);
//                	} else {
//                		alias2=ds2;
//                		logger.info("alias2:" + alias2);
//                	}
//            		
//            		
//        			sql=newSqlBefore+" FROM "+"(SELECT 1 as d1, * FROM "+ds1+")"+" AS "+alias1+
//            				" JOIN "+"(SELECT 1 as d1, * FROM "+ds2+")"+" AS "+alias2+ 
//            				" ON ("+alias1+".d1="+alias2+".d1)";
//        			while(st.hasMoreTokens()){
//        				sql+=" "+st.nextToken(); 
//        			}
//        		
//        		logger.info("final sql:" + sql);

    		}
    	
    	}
    	
      sqlNode = planner.parse(sql);
    } catch (SqlParseException e) {
      throw new QueryInputException("Failure parsing SQL. " + e.getMessage(), e);
    }

    AbstractSqlHandler handler;
    SqlHandlerConfig config = new SqlHandlerConfig(hepPlanner, planner, context);

    // TODO: make this use path scanning or something similar.
    switch(sqlNode.getKind()){
    case EXPLAIN:
      handler = new ExplainHandler(config);
      break;
    case SET_OPTION:
      handler = new SetOptionHandler(context);
      break;
    case OTHER:
      if (sqlNode instanceof DrillSqlCall) {
        handler = ((DrillSqlCall)sqlNode).getSqlHandler(config);
        break;
      }
      // fallthrough
    default:
      handler = new DefaultSqlHandler(config, textPlan);
    }

    try{
      return handler.getPlan(sqlNode);
    }catch(ValidationException e){
      throw new QueryInputException("Failure validating SQL. " + e.getMessage(), e);
    } catch (IOException | RelConversionException e) {
      throw new QueryInputException("Failure handling SQL.", e);
    }

  }

}
