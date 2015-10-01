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
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;



public class Weka {

	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainSupportedArgs implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Workspace  VarCharHolder operationHolder;
		@Workspace  IntHolder operationStringLength;
		@Inject DrillBuf tempBuff;
		@Output VarCharHolder out;

		@Override
		public void setup(RecordBatch incoming) {
			operationHolder = new VarCharHolder();
			operationHolder.start = operationHolder.end = 0; 
			operationHolder.buffer = tempBuff;

			operationStringLength = new IntHolder();
			operationStringLength.value=0;
			//			byte[] operationBuf = new byte[operation.buffer.capacity()];
			//			operation.buffer.getBytes(0, operationBuf, 0, operation.buffer.capacity());
			////			System.out.println(new String(operationBuf,com.google.common.base.Charsets.UTF_8));
			//			operationHolder.buffer.setBytes(0, operationBuf);

		}

		@Override
		public void add() {
			//			operationHolder.buffer=tempBuff;
			byte[] operationBuf = new byte[operation.end - operation.start];
			operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
			//			System.out.println(new String(operationBuf,com.google.common.base.Charsets.UTF_8));
			operationHolder.buffer.setBytes(0, operationBuf);
			operationStringLength.value = (operation.end - operation.start);

		}

		@Override
		public void output() {
			out.buffer = tempBuff;
			//			System.out.println("In out of help function");
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

			//			System.out.println("help function = "+function);
			out.buffer = out.buffer.reallocIfNeeded(function.length()+100);
			out.buffer.setBytes(0,function.getBytes(com.google.common.base.Charsets.UTF_8));
			out.start=0;
			out.end=function.length();
			//			System.out.println(function);

		}

		@Override
		public void reset() {
			operationHolder.start = operationHolder.end = 0; 

		}

	}

	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainUpdateable implements DrillAggFunc{


		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		//		@Param  VarCharHolder modelName;
		//		@Param  IntHolder label;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		//@Inject DrillBuf buffer;
		//		@Workspace IntHolder end;
		//		@Workspace  VarCharHolder modelNameHolder;
		//		@Workspace  IntHolder modelNameStringLength;
		@Inject DrillBuf tempBuff;
		//		@Workspace WekaAttributesHolder attributes;
		@Workspace WekaUpdatableClassifierHolder classifier;

		public void setup(RecordBatch b) {
			classifier = new WekaUpdatableClassifierHolder();
			classifier.classifier=null;

			//			modelNameHolder = new VarCharHolder();
			//			modelNameHolder.start = modelNameHolder.end = 0; 
			//			modelNameHolder.buffer = tempBuff;
			//			
			//			modelNameStringLength = new IntHolder();
			//			modelNameStringLength.value=0;

			//			attributes = new WekaAttributesHolder();
			//			attributes.attributes = null;
			//			end= new IntHolder();
			//			end.value=0;
		}

		@Override
		public void add() {

			//			byte[] modelNameBuf = new byte[modelName.end - modelName.start];
			//			modelName.buffer.getBytes(modelName.start, modelNameBuf, 0, modelName.end - modelName.start);
			//			modelNameHolder.buffer.setBytes(0, modelNameBuf);
			//			modelNameStringLength.value = (modelName.end - modelName.start);

			//value = value.reallocIfNeeded(attributes.buffer.capacity()*2);

			//			System.out.println("attributes = "+attributes.buffer.toString(attributes.start,(attributes.end-attributes.start),com.google.common.base.Charsets.UTF_8));


			byte[] operationBuf = new byte[operation.end - operation.start];
			operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
			String function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();


			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
			//			System.out.println("rowdata = "+rowData);

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
//					System.out.println("option"+i+":"+options[i]);
					if(options[i].indexOf("classes")>0){
						classType = options[i+1];
//						System.out.println("class type:"+classType);
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
//			System.out.println((arffHeader+rowData));

			try {
				// convert String into InputStream
				java.io.InputStream is = new java.io.ByteArrayInputStream((arffHeader+rowData).getBytes("UTF-8"));

				// read it with BufferedReader
				java.io.BufferedReader datafile = new java.io.BufferedReader(new java.io.InputStreamReader(is));



				weka.core.Instances instances = new weka.core.Instances(datafile);
				instances.setClassIndex(instances.numAttributes() - 1);

				//					System.out.println("Instances = " + instances);
				if ("ibk".equals(function)){
					try{

						// train KNN
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
						//						System.out.println("Update classifier = "+classifier.classifier.toString()+" using:"+((weka.classifiers.lazy.IBk)classifier.classifier).getNumTraining());


					}catch(Exception ex){
						//						System.out.println("ex1="+ex.getMessage());
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();

							
							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							//							System.out.println("options: "+((weka.classifiers.lazy.IBk)classifier.classifier).getOptions());
							classifier.classifier.buildClassifier(instances);
							//							System.out.println("New classifier = "+classifier.classifier);
							//						ibk.updateClassifier(instances.instance(0));
						}catch(Exception e){
							//							System.out.println("ex2="+e.getMessage()); 
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function)){
					try{

						// train NaiveBayes
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
						//						System.out.println("Update classifier = "+classifier.classifier.toString()+" using:"+((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).getNumTraining());


					}catch(Exception ex){
						//						System.out.println("ex1="+ex.getMessage());
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();

							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
							//							System.out.println("options: "+((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).getOptions());
							classifier.classifier.buildClassifier(instances);
							//							System.out.println("New classifier = "+classifier.classifier);
							//						ibk.updateClassifier(instances.instance(0));
						}catch(Exception e){
							//							System.out.println("ex2="+e.getMessage()); 
							e.printStackTrace();
						}
					}
				} 

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


		}
		@Override
		public void output() {
			//			System.out.println("In out of main function");
			//			
			//			byte[] modelNameBuf = new byte[modelNameStringLength.value];
			//			modelNameHolder.buffer.getBytes(0, modelNameBuf, 0, modelNameStringLength.value);
			//			String modelName = new String(modelNameBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
			//		
			try {
				// serialize model
				//				weka.core.SerializationHelper.write(modelName, ((weka.classifiers.lazy.IBk)classifier.classifier));
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();

//				weka.classifiers.lazy.IBk orgIBk = ((weka.classifiers.lazy.IBk)classifier.classifier);

				//				System.out.println("write classifier:"+ orgIBk);

//				weka.core.SerializationHelper.write(os, orgIBk);
				weka.core.SerializationHelper.write(os, classifier.classifier);
				//				String modelString = new String(os.toByteArray(),com.google.common.base.Charsets.UTF_8);
				//				System.out.println(modelString);
				out.buffer = tempBuff;

				//String output = "The "+modelName+" model was successfully created.\n";
				//byte[] outbuff = (os.toString(com.google.common.base.Charsets.UTF_8).getBytes(com.google.common.base.Charsets.UTF_8));
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;

				//				byte[] outbuff = new byte[os.toByteArray().length];
				//				out.buffer.getBytes(0, outbuff);

				//				java.io.InputStream is = new java.io.ByteArrayInputStream(outbuff);

				//				System.out.println("reading model");
				//				weka.classifiers.lazy.IBk testIBk = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(is);
				//				System.out.println("read classifier:"+ testIBk);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//				System.out.println("error:"+e.getLocalizedMessage());
			}
			//
			//			out.start = 0;
			//			out.end = out.buffer.capacity();
			//			System.out.println("output length:"+classifier.classifier.toString().length());
			//			System.out.println("capacity: "+out.buffer.capacity());
			//		System.out.println("out = "+out.buffer.toString(out.start,(out.end-out.start),com.google.common.base.Charsets.UTF_8));	

		}
		@Override
		public void reset() {

			//value = null; 
			//value.buffer = buffer;// = buffer.reallocIfNeeded( 1 + (right.end - right.start));
			//			end.value = 0;
		}
	}



	@FunctionTemplate(name = "qdm_test_weka", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
	public static class WekaTestUpdateable implements DrillSimpleFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param 	NullableVarCharHolder classifierTxt;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace String function;

		public void setup(RecordBatch b) {
			classifier = new WekaUpdatableClassifierHolder();
			classifier.classifier=null;

		}


		public void eval() {
			boolean firstRun = false;
			if(function == null || function.length()==0){
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				firstRun = true;
			}

			

				if(firstRun)
				{
					try{
						byte[] classifierBuf = new byte[classifierTxt.end - classifierTxt.start];
						classifierTxt.buffer.getBytes(classifierTxt.start, classifierBuf, 0, classifierTxt.end - classifierTxt.start);
						java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
						//						System.out.println("loading classifier from disk");

						try {
							if ("ibk".equals(function)){
							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
							//							System.out.println("read classifier:"+ classifier.classifier);
							} else if ("nb".equals(function)){
								classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
							}
						} catch (Exception e) {
							//							System.out.println("Failed to read classifier");
							//							e.printStackTrace(System.out);
							e.printStackTrace();
						}
					}catch(Exception e){
						//						e.printStackTrace(System.out);
						e.printStackTrace();
					}
				}


				try {
					byte[] temp = new byte[features.end - features.start];
					features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
					String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
					//			System.out.println("rowdata = "+rowData);

					java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");

					int attributesCount = st.countTokens();
					String arffHeader = "@"+"RELATION Drill\n";
					for(int i=0; i< attributesCount;i++)
					{
						arffHeader+="@"+"ATTRIBUTE att"+i+" numeric\n";
					}
					
					byte[] argsBuf = new byte[arguments.end - arguments.start];
					arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
					
					String[] classes = null;
					String[] options = null;
					try {
						options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
						for(int i=0;i<options.length;i++){
//							System.out.println("option"+i+":"+options[i]);
							if(options[i].indexOf("classes")>0){
								classes = options[i+1].substring(1, options[i+1].length()-1).split(",");
								
//								System.out.println("class type:"+classes);
								options[i]="";
								options[i+1]="";
							}
						}
						
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
//					arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";
					
					arffHeader+="@"+"ATTRIBUTE class numeric\n";
					arffHeader+="@"+"DATA\n";
//					System.out.println((arffHeader+rowData));


					// convert String into InputStream
					java.io.InputStream is = new java.io.ByteArrayInputStream((arffHeader+rowData+",0").getBytes("UTF-8"));

					// read it with BufferedReader
					java.io.BufferedReader datafile = new java.io.BufferedReader(new java.io.InputStreamReader(is));



					weka.core.Instances instances = new weka.core.Instances(datafile);
					instances.setClassIndex(instances.numAttributes() - 1);
					
					
					String output="";
//					if(classType.equalsIgnoreCase("numeric"))
//						out.value = classifier.classifier.classifyInstance(instances.instance(0));
//					else
//					{
						double[] predictions = classifier.classifier.distributionForInstance(instances.instance(0));
						if(predictions.length==1){
							output = ""+predictions[0];
						} else {
//							for(int i=0;i<predictions.length;i++){
//								System.out.print(predictions[i]+",");
//							}
							java.util.List b = java.util.Arrays.asList(org.apache.commons.lang.ArrayUtils.toObject(predictions));
							
//					        System.out.println("\nmax="+java.util.Collections.max(b));
//					        System.out.println("index="+b.indexOf(java.util.Collections.max(b)));
					        if(classes!=null)
					        	output= classes[b.indexOf(java.util.Collections.max(b))];
					        else
					        	output= ""+(b.indexOf(java.util.Collections.max(b))+1);
						}
//					}
					
					out.buffer = tempBuff;
					//String output = "The "+modelName+" model was successfully created.\n";
					//byte[] outbuff = (os.toString(com.google.common.base.Charsets.UTF_8).getBytes(com.google.common.base.Charsets.UTF_8));
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

