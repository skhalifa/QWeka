package ca.queensu.cs.aggregate;


import io.netty.buffer.DrillBuf;

import javax.inject.Inject;










import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.RepeatedVarCharHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

import com.jcraft.jsch.Logger;



public class Weka {

	/**
	 * @author shadi
	 * 
	 * select qdm_train_weka('?') 
	 * from `output100M.csv` as mydata;
	 * 
	 * OR
	 * 
	 * Select qdm_train_weka('nb') 
	 * from `output100M.csv` as mydata;
	 *
	 */
	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainSupportedArgs implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Workspace  VarCharHolder operationHolder;
		@Workspace  IntHolder operationStringLength;
		@Inject DrillBuf tempBuff;
		@Output VarCharHolder out;

		@Override
		public void setup() {
			operationHolder = new VarCharHolder();
			operationHolder.start = operationHolder.end = 0; 
			operationHolder.buffer = tempBuff;

			operationStringLength = new IntHolder();
			operationStringLength.value=0;
		}

		@Override
		public void add() {
			byte[] operationBuf = new byte[operation.end - operation.start];
			operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
			operationHolder.buffer.setBytes(0, operationBuf);
			operationStringLength.value = (operation.end - operation.start);

		}

		@Override
		public void output() {
			out.buffer = tempBuff;
			byte[] operationBuf = new byte[operationStringLength.value];
			operationHolder.buffer.getBytes(0, operationBuf, 0, operationStringLength.value);
			String function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();

			if ("?".equals(function)){
				function = "qdm_weka_train(\'IBk\',arguments,comma-separated features (label is the last column))\n\r";
				function += "Type qdm_weka_train(\'IBk\') for IBk help\n\r\n\r";
				function += "qdm_weka_train(\'nb\',arguments,comma-separated features (label is the last column))\n\r";
				function += "Type qdm_weka_train(\'nb\') for Naive Bayes help";
			} else if ("ibk".equals(function)){
				weka.classifiers.lazy.IBk temp = new weka.classifiers.lazy.IBk();
				function+=":"+temp.globalInfo()+"\n\r";

				for ( java.util.Enumeration<weka.core.Option> e = (java.util.Enumeration<weka.core.Option>)temp.listOptions(); e.hasMoreElements();){
					weka.core.Option tmp = ((weka.core.Option)e.nextElement());
					function+=tmp.name()+" : "+tmp.description()+"\n\r";
				}
				function+="-classes {c1,c2,c3}"+" : "+"List possible classes for the dataset. If not specified class becomes NUMERIC"+"\n\r";

			} else if ("nb".equals(function)){
				weka.classifiers.bayes.NaiveBayesUpdateable temp = new weka.classifiers.bayes.NaiveBayesUpdateable();
				function+=":"+temp.globalInfo()+"\n\r";

				for ( java.util.Enumeration<weka.core.Option> e = (java.util.Enumeration<weka.core.Option>)temp.listOptions(); e.hasMoreElements();){
					weka.core.Option tmp = ((weka.core.Option)e.nextElement());
					function+=tmp.name()+" : "+tmp.description()+"\n\r";
				}
				function+="-classes {c1,c2,c3}"+" : "+"List possible classes for the dataset. If not specified class becomes NUMERIC"+"\n\r";

			} else{
				function+=":"+"No Args";
			}
			out.buffer = out.buffer.reallocIfNeeded(function.length()+100);
			out.buffer.setBytes(0,function.getBytes(com.google.common.base.Charsets.UTF_8));
			out.start=0;
			out.end=function.length();
		}

		@Override
		public void reset() {
			operationHolder.start = operationHolder.end = 0; 

		}

	}
	
	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_train_weka('nb','-classes {1,2}', mydata.columns[1], mydata.columns[2], mydata.columns[3], mydata.columns[4], mydata.columns[5]) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainUpdateable implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
		}

		@Override
		public void add() {
			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
			String [] options = null;
			if(firstRun.value==0){
				firstRun.value = 1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
				String classType = "numeric";
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
							classType = options[i+1];
							options[i]="";
							options[i+1]="";
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));
				instances.setClassIndex(instances.numAttributes() - 1);
				if ("ibk".equals(function.value)){
					try{
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();
							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function.value)){
					try{
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} 

			} catch (Exception e) {
				e.printStackTrace();
			}


		}
		@Override
		public void output() {
			try {
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifier.classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
		}
	}

	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_train_weka('nb','-classes {1,2}', columns) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainRepeatedUpdateable implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  RepeatedVarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
			currVal = new VarCharHolder();
		}

		@Override
		public void add() {
			byte[] temp = new byte[features.end - features.start];
			java.lang.StringBuilder rowBuilder = new java.lang.StringBuilder();
			for (int i = features.start; i < features.end; i++) {
			    features.vector.getAccessor().get(i, currVal);
			    rowBuilder.append(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(currVal.start, currVal.end, currVal.buffer)+",");
			}
			String rowData = rowBuilder.substring(0, rowBuilder.length()-1);
			String [] options = null;
			if(firstRun.value==0){
				firstRun.value = 1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
				String classType = "numeric";
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
							classType = options[i+1];
							options[i]="";
							options[i+1]="";
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));
				instances.setClassIndex(instances.numAttributes() - 1);
				if ("ibk".equals(function.value)){
					try{
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();
							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function.value)){
					try{
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} 

			} catch (Exception e) {
				e.printStackTrace();
			}


		}
		@Override
		public void output() {
			try {
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifier.classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
		}
	}
	
	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_train_weka('nb','-classes {1,2}', columns) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "qdm_train_weka_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainAgg1Updateable implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  RepeatedVarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
//		@Workspace LinkedListHolder dataHolder;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
//			dataHolder = new LinkedListHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
//			dataHolder.list = new java.util.LinkedList<String>();
			firstRun.value=0;
			currVal = new VarCharHolder();
		}

		@Override
		public void add() {
			java.lang.StringBuilder rowBuilder = new java.lang.StringBuilder();
			for (int i = features.start; i < features.end; i++) {
			    features.vector.getAccessor().get(i, currVal);
			    rowBuilder.append(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(currVal.start, currVal.end, currVal.buffer)+",");
			}
			String rowData = rowBuilder.substring(0, rowBuilder.length()-1);
			String [] options = null;
			if(firstRun.value==0){
				firstRun.value = 1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();

				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
				String classType = "numeric";
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
							classType = options[i+1];
							options[i]="";
							options[i+1]="";
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
//				stBuilder.append(function.value+"||"+options+"\n");
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
//				dataHolder.list.add(stBuilder.toString());
//				dataHolder.value = stBuilder.toString();
//				System.out.println("phase1-add: end first run"+arffHeader.value);
			}

//			long timeBefore = System.currentTimeMillis();
//			dataHolder.list.add(rowData);
//			dataHolder.value+=rowData;

//			
//			System.out.println("phase1-add:"+arffHeader.value);
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));
				instances.setClassIndex(instances.numAttributes() - 1);
				if ("ibk".equals(function.value)){
					try{
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();
							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function.value)){
					try{
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} 

			} catch (Exception e) {
				e.printStackTrace();
			}
//			long timeAfter = System.currentTimeMillis();
//			System.out.println("training time:"+(timeAfter-timeBefore));


		}
		@Override
		public void output() {
			try {
				System.out.println("Starting out");
				long timeBefore = System.currentTimeMillis();
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifier.classifier);
				byte[] data = os.toByteArray();
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(data.length);
				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
				out.start=0;
				out.end=data.length;
				long timeAfter = System.currentTimeMillis();
				System.out.println("out time:"+(timeAfter-timeBefore));
				
//				long timeBefore = System.currentTimeMillis();
//				System.out.println("Starting out");
//				byte[] data = dataHolder.toString().getBytes();
//				byte[] data = dataHolder.value.getBytes();
//				long timeAfter = System.currentTimeMillis();
//				System.out.println("out time:"+(timeAfter-timeBefore));
//				out.buffer = tempBuff;
//				long timeBefore2 = System.currentTimeMillis();
//				out.buffer = out.buffer.reallocIfNeeded(data.length);
//				long timeAfter2 = System.currentTimeMillis();
//				System.out.println("out time:"+(timeAfter2-timeBefore2));
//				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
//				long timeAfter3 = System.currentTimeMillis();
//				System.out.println("out time2:"+(timeAfter3-timeAfter2));
//				out.start=0;
//				out.end=data.length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
		}
	}
	
	
	/**
	 * @author shadi
	 * 
	 * AGGREGATOR FOR
	 * Train model xzy as 
	 * select qdm_train_weka('nb','-classes {1,2}', columns) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "qdm_train_weka_agg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainAgg2Updateable implements DrillAggFunc{

		@Param  VarCharHolder model;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifierAgg;
		@Workspace StringHolder function;
//		@Workspace  BitHolder firstRun;
//		@Workspace VarCharHolder currVal;

		public void setup() {
			classifierAgg = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
//			firstRun = new BitHolder();
			classifierAgg.classifier=null;
			function.value=null;
//			firstRun.value=0;
//			currVal = new VarCharHolder();
		}

		@Override
		public void add() {
		System.out.println("in agg add");
			byte[] classifierBuf = new byte[model.end - model.start];
			model.buffer.getBytes(model.start, classifierBuf, 0, model.end - model.start);
//			String arfftmp = new String(tmp, com.google.common.base.Charsets.UTF_8);
			
//			System.out.println("phase2-add:"+arfftmp);
			
			function.value= "nb";			
			try{
				java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
				try {
					weka.classifiers.Classifier classifier = null;
					if ("ibk".equals(function.value)){
//						classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
//						if(classifierAgg.classifier==null){
//							classifierAgg.classifier = classifier;
//						} else {
//							// aggregate classifiers
//							((weka.classifiers.lazy.IBk) classifierAgg.classifier).
//						}
					} else if ("nb".equals(function.value)){
						classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
						if(classifierAgg.classifier==null){
							System.out.println("first clasifier");
							classifierAgg.classifier = classifier;
						} else {
							// aggregate classifiers
							System.out.println("aggregating classifier");
							long timeBefore = System.currentTimeMillis();
							((weka.classifiers.bayes.NaiveBayesUpdateable) classifierAgg.classifier).aggregate((weka.classifiers.bayes.NaiveBayesUpdateable)classifier);
							long timeAfter = System.currentTimeMillis();
							System.out.println("aggregation time:"+(timeAfter-timeBefore));
						}
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			

//			try {
//				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arfftmp));
//				instances.setClassIndex(instances.numAttributes() - 1);
//				if ("ibk".equals(function.value)){
//					try{
//						for(int i=0;i<instances.numInstances();i++){
//							((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(i));
//						}
//					}catch(Exception ex){
//						try{
//							classifier.classifier = new weka.classifiers.lazy.IBk();
//							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
//							classifier.classifier.buildClassifier(instances);
//						}catch(Exception e){
//							e.printStackTrace();
//						}
//					}
//				} else if ("nb".equals(function.value)){
//					try{
//						System.out.println("phase2: old classifier");
//						for(int i=0;i<instances.numInstances();i++){
//							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(i));
//						}
//						System.out.println("phase2: old classifier updated with "+instances.numInstances()+" instances");
//					}catch(Exception ex){
//						try{
//							System.out.println("phase2: new classifier");
//							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
//							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
//							classifier.classifier.buildClassifier(instances);
//							System.out.println("phase2: new classifier Trained");
//						}catch(Exception e){
//							e.printStackTrace();
//						}
//					}
//				} 
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}


		}
		@Override
		public void output() {
			try {
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifierAgg.classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
		}

	}
	
	
	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_train_weka_old('nb','-classes {1,2}', mydata.columns[1], mydata.columns[2], mydata.columns[3], mydata.columns[4], mydata.columns[5]) 
	 * from `output100M.csv` as mydata;
	 *
	 */
	
	@FunctionTemplate(name = "qdm_train_weka_old", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainUpdateableOld implements DrillAggFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			classifier.classifier=null;
		}

		@Override
		public void add() {
			byte[] operationBuf = new byte[operation.end - operation.start];
			operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
			String function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
			java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
			int attributesCount = st.countTokens();
			String arffHeader = "@"+"RELATION Drill\n";
			for(int i=0; i< attributesCount-1;i++)
			{
				arffHeader+="@"+"ATTRIBUTE att"+i+" numeric\n";
			}
			
			byte[] argsBuf = new byte[arguments.end - arguments.start];
			arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
			
			String classType = "numeric";
			String[] options = null;
			try {
				options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
				for(int i=0;i<options.length;i++){
					if(options[i].indexOf("classes")>0){
						classType = options[i+1];
						options[i]="";
						options[i+1]="";
					}
				}
				
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";
			arffHeader+="@"+"DATA\n";
			try {
				// convert String into InputStream
				java.io.InputStream is = new java.io.ByteArrayInputStream((arffHeader+rowData).getBytes("UTF-8"));
				// read it with BufferedReader
				java.io.BufferedReader datafile = new java.io.BufferedReader(new java.io.InputStreamReader(is));
				weka.core.Instances instances = new weka.core.Instances(datafile);
				instances.setClassIndex(instances.numAttributes() - 1);
				if ("ibk".equals(function)){
					try{
						// train KNN
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();
							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function)){
					try{
						// train NaiveBayes
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} 

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		@Override
		public void output() {
			try {
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifier.classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {

		}
	}
	
	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select maxx(columns[0]) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "maxx", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainRepeatedUpdateableParallel implements DrillAggFunc{
		

		@Param  VarCharHolder operation;
//		@Param  VarCharHolder arguments;
//		@Param  RepeatedVarCharHolder features;

		  @Workspace ObjectHolder value;
		  @Workspace UInt1Holder init; 
		  @Inject DrillBuf buf;
		  @Output VarCharHolder out;

		  public void setup() {
		    init = new UInt1Holder();
		    init.value = 0;
		    value = new ObjectHolder();
		    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();
		    value.obj = tmp;

		  }

		  @Override
		  public void add() {
		    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
		    int cmp = 0;
		    boolean swap = false;

		    // if buffer is null then swap
		    if (init.value == 0) {
		      init.value = 1;
		      swap = true;
		    } else {
		      // Compare the bytes
//		      cmp = org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(in.buffer, in.start, in.end, tmp.getBytes(), 0, tmp.getLength());


		      swap = (cmp == 1);
		    }
//		    if (swap) {
//		      int inputLength = in.end - in.start;
//		      if (tmp.getLength() >= inputLength) {
//		        in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
//		        tmp.setLength(inputLength);
//		      } else {
//		        byte[] tempArray = new byte[in.end - in.start];
//		        in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
//		        tmp.setBytes(tempArray);
//		      }
//		    }
		  }

		  @Override
		  public void output() {
		    org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
		    buf = buf.reallocIfNeeded(tmp.getLength());
		    buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
		    out.start  = 0;
		    out.end    = tmp.getLength();
		    out.buffer = buf;
		  }

		  @Override
		  public void reset() {
		    value = new ObjectHolder();
		    init.value = 0;
		  }

	}

	
	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_train_weka_new('nb','-classes {1,2}', mydata.columns[1], mydata.columns[2], mydata.columns[3], mydata.columns[4], mydata.columns[5]) 
	 * from `output100M.csv` as mydata;
	 *
	 */
	@FunctionTemplate(name = "qdm_train_weka_new", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainUpdateableNew implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
//		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringArrayHolder options;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;

		public void setup() {
//			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			options = new StringArrayHolder();
			firstRun = new BitHolder();
//			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			options.value=null;
			firstRun.value=0;
		}

		@Override
		public void add() {
			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
//			String [] options = null;
			if(firstRun.value==0){
				firstRun.value = 1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
				String classType = "numeric";
				try {
					options.value = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.value.length;i++){
						if(options.value[i].indexOf("classes")>0){
							classType = options.value[i+1];
							options.value[i]="";
							options.value[i+1]="";
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}
			arffHeader.value+=rowData+"\n";


		}
		@Override
		public void output() {
			try {
				weka.classifiers.Classifier classifier = null;
				try {
					weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value));
					instances.setClassIndex(instances.numAttributes() - 1);
					if ("ibk".equals(function.value)){
						try{
							classifier = new weka.classifiers.lazy.IBk();
							((weka.classifiers.lazy.IBk)classifier).setOptions(options.value);
							((weka.classifiers.lazy.IBk)classifier).buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
						
					} else if ("nb".equals(function.value)){
						try{
							classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier).setOptions(options.value);
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier).buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					} 

				} catch (Exception e) {
					e.printStackTrace();
				}
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
		}
	}

	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_update_weka('nb','-classes {1,2}', mymodel.columns[0], mydata.columns[1], mydata.columns[2], mydata.columns[3], mydata.columns[4], mydata.columns[5]) 
	 * from `output100M.csv` as mydata applying nb100M_3 as mymodel;
	 *
	 */
	@FunctionTemplate(name = "qdm_update_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaUpdateTrain implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param 	NullableVarCharHolder classifierTxt;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;
		
		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
		}

		@Override
		public void add() {
			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
			String [] options = null;
			if(firstRun.value==0){
				firstRun.value = 1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				
				try{
					byte[] classifierBuf = new byte[classifierTxt.end - classifierTxt.start];
					classifierTxt.buffer.getBytes(classifierTxt.start, classifierBuf, 0, classifierTxt.end - classifierTxt.start);
					java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
					try {
						if ("ibk".equals(function)){
							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
						} else if ("nb".equals(function)){
							classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}catch(Exception e){
					e.printStackTrace();
				}
				
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
				String classType = "numeric";
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
							classType = options[i+1];
							options[i]="";
							options[i+1]="";
						}
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));
				instances.setClassIndex(instances.numAttributes() - 1);
				if ("ibk".equals(function.value)){
					try{
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();
							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function.value)){
					try{
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
					}catch(Exception ex){
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
							classifier.classifier.buildClassifier(instances);
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} 

			} catch (Exception e) {
				e.printStackTrace();
			}


		}
		@Override
		public void output() {
			try {
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifier.classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
		}
	}
	
	

	/**
	 * @author shadi
	 * 
	 * Create table xzy as 
	 * select qdm_test_weka('nb','-classes {1,2}', mymodel.columns[0], mydata.columns[0],....) 
	 * from `output100M.csv` as mydata applying nb100M_3 as mymodel;
	 *
	 */
	
	@FunctionTemplate(name = "qdm_test_weka", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
	public static class WekaTestUpdateable implements DrillSimpleFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param 	NullableVarCharHolder classifierTxt;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace String function;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			classifier.classifier=null;
			arffHeader.value=null;
			firstRun.value=0;
		}

		public void eval() {
			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
			if(firstRun.value==0){
				firstRun.value=1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				try{
					byte[] classifierBuf = new byte[classifierTxt.end - classifierTxt.start];
					classifierTxt.buffer.getBytes(classifierTxt.start, classifierBuf, 0, classifierTxt.end - classifierTxt.start);
					java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
					try {
						if ("ibk".equals(function)){
							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
						} else if ("nb".equals(function)){
							classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}catch(Exception e){
					e.printStackTrace();
				}
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
//				String[] classes = null;
				String[] options = null;
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
//							classes = options[i+1].substring(1, options[i+1].length()-1).split(",");
							options[i]="";
							options[i+1]="";
						}
					}

				} catch (Exception e1) {
					e1.printStackTrace();
				}
//				arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";
				stBuilder.append("@"+"ATTRIBUTE class numeric\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}

			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData+",0"));
				instances.setClassIndex(instances.numAttributes() - 1);
				String output="";
				double[] predictions = classifier.classifier.distributionForInstance(instances.instance(0));
				if(predictions.length==1){
					output = ""+predictions[0];
				} else {
//					java.util.List b = java.util.Arrays.asList(org.apache.commons.lang.ArrayUtils.toObject(predictions));
					double max = -1;
					for(int i=0;i<predictions.length;i++){
						if(predictions[i]>max){
							max=predictions[i];
							output=""+ (i+1);							
						}
					}
//					if(classes!=null){
////						output= classes[b.indexOf(java.util.Collections.max(b))];
//						double max = -1;
//						for(int i=0;i<predictions.length;i++){
//							if(predictions[i]>max){
//								output=classes[i];
//							}
//						}
//					}else{
////						output= ""+(b.indexOf(java.util.Collections.max(b))+1);
//						double max = -1;
//						for(int i=0;i<predictions.length;i++){
//							if(predictions[i]>max){
//								output=""+ (i+1);
//							}
//						}
//					}
				}

				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(output.getBytes().length);
				out.buffer.setBytes(0, output.getBytes());//.setBytes(0,outbuff);
				out.start=0;
				out.end=output.getBytes().length;
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * @author shadi
	 * 
	 * Create table xzy as 
	 * select qdm_test_weka('nb','-classes {1,2}', mymodel.columns[0], mydata.columns) 
	 * from `output100M.csv` as mydata applying nb100M_3 as mymodel;
	 *
	 */
	
	@FunctionTemplate(name = "qdm_test_weka", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
	public static class WekaTestRepeatedUpdateable implements DrillSimpleFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param 	NullableVarCharHolder classifierTxt;
		@Param  RepeatedVarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace String function;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			classifier.classifier=null;
			arffHeader.value=null;
			firstRun.value=0;
			currVal = new VarCharHolder();
		}

		public void eval() {
			byte[] temp = new byte[features.end - features.start];		
			java.lang.StringBuilder rowBuilder = new java.lang.StringBuilder();
		    for (int i = features.start; i < features.end; i++) {
		        features.vector.getAccessor().get(i, currVal);
		        rowBuilder.append(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(currVal.start, currVal.end, currVal.buffer)+",");
		    } 
		  	String rowData = rowBuilder.substring(0, rowBuilder.length()-1);
			if(firstRun.value==0){
				firstRun.value=1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				try{
					byte[] classifierBuf = new byte[classifierTxt.end - classifierTxt.start];
					classifierTxt.buffer.getBytes(classifierTxt.start, classifierBuf, 0, classifierTxt.end - classifierTxt.start);
					java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
					try {
						if ("ibk".equals(function)){
							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
						} else if ("nb".equals(function)){
							classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}catch(Exception e){
					e.printStackTrace();
				}
				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
//				String[] classes = null;
				String[] options = null;
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
//							classes = options[i+1].substring(1, options[i+1].length()-1).split(",");
							options[i]="";
							options[i+1]="";
						}
					}

				} catch (Exception e1) {
					e1.printStackTrace();
				}
//				arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";
				stBuilder.append("@"+"ATTRIBUTE class numeric\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}

			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));
				
				instances.setClassIndex(instances.numAttributes() - 1);
				String output="";
				double[] predictions = classifier.classifier.distributionForInstance(instances.instance(0));
				if(predictions.length==1){
					output = ""+predictions[0];
				} else {
//					java.util.List b = java.util.Arrays.asList(org.apache.commons.lang.ArrayUtils.toObject(predictions));
					double max = -1;
					for(int i=0;i<predictions.length;i++){
						if(predictions[i]>max){
							max=predictions[i];
							output=""+ (i+1);							
						}
					}
//					if(classes!=null){
////						output= classes[b.indexOf(java.util.Collections.max(b))];
//						double max = -1;
//						for(int i=0;i<predictions.length;i++){
//							if(predictions[i]>max){
//								output=classes[i];
//							}
//						}
//					}else{
////						output= ""+(b.indexOf(java.util.Collections.max(b))+1);
//						double max = -1;
//						for(int i=0;i<predictions.length;i++){
//							if(predictions[i]>max){
//								output=""+ (i+1);
//							}
//						}
//					}
				}

				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(output.getBytes().length);
				out.buffer.setBytes(0, output.getBytes());//.setBytes(0,outbuff);
				out.start=0;
				out.end=output.getBytes().length;
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}


}

