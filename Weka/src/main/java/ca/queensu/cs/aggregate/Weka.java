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
import org.apache.drill.exec.expr.holders.BitHolder;
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
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringHolder arffHeader;
		@Workspace  BitHolder firstRun;
		//		@Workspace String[] options;

		public void setup(RecordBatch b) {
			//System.out.println("Shadi: Setup start : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			//System.out.println("Shadi: Setup WekaUpdatableClassifierHolder : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
			//System.out.println("Shadi: Setup classifier.classifier=null; : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
		}

		@Override
		public void add() {
			//			System.out.println("Shadi: add start : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
			//			System.out.println("Shadi: add rowData end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

			String [] options = null;

			//			boolean firstRun = false;
			//System.out.println("Shadi: add check  function "+function.value+": "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			if(firstRun.value==0){
				firstRun.value = 1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				//				System.out.println("Shadi: add function end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
				//System.out.println("Shadi: add tokenizer end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount-1;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}

				//System.out.println("Shadi: add header end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);

				//System.out.println("Shadi: add argsBuf end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

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
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				//System.out.println("Shadi: add options end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();

			}

			try {
				//				//System.out.println("Shadi: arff : "+arffHeader.value+rowData);
				// convert String into InputStream
				//				java.io.InputStream is = new java.io.ByteArrayInputStream((arffHeader.value+rowData).getBytes("UTF-8"));

				//				//System.out.println("Shadi: add is end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				//				java.io.BufferedReader datafile = new java.io.BufferedReader(new java.io.StringReader(arffHeader.value+rowData));

				// read it with BufferedReader
				//				java.io.BufferedReader datafile = new java.io.BufferedReader(new java.io.InputStreamReader(is));

				//				System.out.println("Shadi: add datafile end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));


				//				System.out.println("Shadi: add instances end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				instances.setClassIndex(instances.numAttributes() - 1);

				//System.out.println("Shadi: add setclassindex end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				if ("ibk".equals(function.value)){
					try{

						// train KNN
						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
												//System.out.println("Update classifier = "+classifier.classifier.toString()+" using:"+((weka.classifiers.lazy.IBk)classifier.classifier).getNumTraining());


					}catch(Exception ex){
						//						//System.out.println("ex1="+ex.getMessage());
						try{
							classifier.classifier = new weka.classifiers.lazy.IBk();


							((weka.classifiers.lazy.IBk)classifier.classifier).setOptions(options);
							//							//System.out.println("options: "+((weka.classifiers.lazy.IBk)classifier.classifier).getOptions());
							classifier.classifier.buildClassifier(instances);
							//							//System.out.println("New classifier = "+classifier.classifier);
							//						ibk.updateClassifier(instances.instance(0));
						}catch(Exception e){
							//							//System.out.println("ex2="+e.getMessage()); 
							e.printStackTrace();
						}
					}
				} else if ("nb".equals(function.value)){
					try{

						// train NaiveBayes
						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
						//						//System.out.println("Update classifier = "+classifier.classifier.toString()+" using:"+((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).getNumTraining());
//						System.out.println("Shadi: add update classifier end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

					}catch(Exception ex){
						//						//System.out.println("ex1="+ex.getMessage());
						try{
							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
//							System.out.println("Shadi: add new classifier end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
//							System.out.println("Shadi: add new classiifier set options end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
							//							//System.out.println("options: "+((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).getOptions());
							classifier.classifier.buildClassifier(instances);
//							System.out.println("Shadi: add classifier build end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
							//							//System.out.println("New classifier = "+classifier.classifier);
							//						ibk.updateClassifier(instances.instance(0));
						}catch(Exception e){
							//							//System.out.println("ex2="+e.getMessage()); 
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
			//System.out.println("Shadi: output start : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			try {

				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();

				//System.out.println("Shadi: output os end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				weka.core.SerializationHelper.write(os, classifier.classifier);

				//System.out.println("Shadi: output write end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				out.buffer = tempBuff;

				//System.out.println("Shadi: output out.buffer end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));


				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);

				//System.out.println("Shadi: output buffer realloc end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);

				//System.out.println("Shadi: output buffer.setBytes end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				out.start=0;
				out.end=os.toByteArray().length;

				//System.out.println("Shadi: output start end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));


			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		@Override
		public void reset() {
			//System.out.println("Shadi: reset start : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			//value = null; 
			//value.buffer = buffer;// = buffer.reallocIfNeeded( 1 + (right.end - right.start));
			//			end.value = 0;
		}
	}



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

		public void setup(RecordBatch b) {
//			System.out.println("Shadi: setup start : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			classifier = new WekaUpdatableClassifierHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
//			System.out.println("Shadi: setup classifier end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			classifier.classifier=null;
			arffHeader.value=null;
			firstRun.value=0;
//			System.out.println("Shadi: setup classifier null end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

		}


		public void eval() {
//			System.out.println("Shadi: eval start : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);
//			System.out.println("Shadi: eval rowData end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			//			//System.out.println("rowdata = "+rowData);

			//			boolean firstRun = false;
			//System.out.println("Shadi: eval check  function "+function+": "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			//			if(function == null || function.length()==0){
			if(firstRun.value==0){
				firstRun.value=1;
				byte[] operationBuf = new byte[operation.end - operation.start];
				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
				function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
				//				firstRun = true;
				//			}
				System.out.println("Shadi: eval function end :"+ new java.sql.Timestamp((new java.util.Date()).getTime()));

				//				if(firstRun)
				//				{
				try{
					byte[] classifierBuf = new byte[classifierTxt.end - classifierTxt.start];
//					System.out.println("Shadi: eval classifierBuf end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
					classifierTxt.buffer.getBytes(classifierTxt.start, classifierBuf, 0, classifierTxt.end - classifierTxt.start);
//					System.out.println("Shadi: eval classifierTxt.getBytes end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
					java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
//					System.out.println("Shadi: eval cis end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
					//						//System.out.println("loading classifier from disk");

					try {
						if ("ibk".equals(function)){
							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
							//							//System.out.println("read classifier:"+ classifier.classifier);
						} else if ("nb".equals(function)){
							classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
//							System.out.println("Shadi: eval read classifier end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
						}
					} catch (Exception e) {
						//							//System.out.println("Failed to read classifier");
						//							e.printStackTrace(System.out);
						e.printStackTrace();
					}
				}catch(Exception e){
					//						e.printStackTrace(System.out);
					e.printStackTrace();
				}




				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
//				System.out.println("Shadi: eval tokenizer end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				int attributesCount = st.countTokens();
				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
				stBuilder.append("@"+"RELATION Drill\n");
				for(int i=0; i< attributesCount;i++)
				{
					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
				}
//				System.out.println("Shadi: eval header end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				byte[] argsBuf = new byte[arguments.end - arguments.start];
				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
//				System.out.println("Shadi: eval argsBuf end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				//					String[] classes = null;
				String[] options = null;
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						//							//System.out.println("option"+i+":"+options[i]);
						if(options[i].indexOf("classes")>0){
							//								classes = options[i+1].substring(1, options[i+1].length()-1).split(",");

							//								//System.out.println("class type:"+classes);
							options[i]="";
							options[i+1]="";
						}
					}

				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

//				System.out.println("Shadi: eval options end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				//					arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";

				stBuilder.append("@"+"ATTRIBUTE class numeric\n");
				stBuilder.append("@"+"DATA\n");
				//					//System.out.println((arffHeader+rowData));

				arffHeader.value = stBuilder.toString();
//				System.out.println("Shadi: eval arffHeaer end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
			}

			try {


				// convert String into InputStream
				//				java.io.InputStream is = new java.io.ByteArrayInputStream((arffHeader.value+rowData+",0").getBytes("UTF-8"));
				//				//System.out.println("Shadi: eval is end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				//
				//				// read it with BufferedReader
				//				java.io.BufferedReader datafile = new java.io.BufferedReader(new java.io.InputStreamReader(is));
				//				//System.out.println("Shadi: eval datafile end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				//
				//
				//				weka.core.Instances instances = new weka.core.Instances(datafile);
				//				
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData+",0"));
//System.out.println("instances="+instances);

//				System.out.println("Shadi: eval instances end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				instances.setClassIndex(instances.numAttributes() - 1);
//				System.out.println("Shadi: eval classindex end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

				String output="";
				//					if(classType.equalsIgnoreCase("numeric"))
				//										out.value = classifier.classifier.classifyInstance(instances.instance(0));
				//					else
				//					{
				double[] predictions = classifier.classifier.distributionForInstance(instances.instance(0));
//				System.out.println("Shadi: eval predictions end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
//				System.out.println("predictions.length="+predictions.length);
				if(predictions.length==1){
					output = ""+predictions[0];
				} else {

					//							for(int i=0;i<predictions.length;i++){
					//								//System.out.print(predictions[i]+",");
					//							}
					//					java.util.List b = java.util.Arrays.asList(org.apache.commons.lang.ArrayUtils.toObject(predictions));

					//					        //System.out.println("\nmax="+java.util.Collections.max(b));
					//					        //System.out.println("index="+b.indexOf(java.util.Collections.max(b)));

					double max = -1;
					for(int i=0;i<predictions.length;i++){
//						System.out.print("prediction ["+i+"] = "+predictions[i]+" & ");
						
						if(predictions[i]>max){
							max=predictions[i];
							output=""+ (i+1);
							
						}
					}
					
//					System.out.println(" =====> output = "+output+" and max = "+max);
					
//					System.out.println("Shadi: eval after output loop : "+new java.sql.Timestamp((new java.util.Date()).getTime()));

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
				//System.out.println("Shadi: eval parse predictions end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				//					}

				out.buffer = tempBuff;
//				System.out.println("Shadi: eval out.buffer end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				//String output = "The "+modelName+" model was successfully created.\n";
				//byte[] outbuff = (os.toString(com.google.common.base.Charsets.UTF_8).getBytes(com.google.common.base.Charsets.UTF_8));
				out.buffer = out.buffer.reallocIfNeeded(output.getBytes().length);
//				System.out.println("Shadi: eval out realloc end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				out.buffer.setBytes(0, output.getBytes());//.setBytes(0,outbuff);
//				System.out.println("Shadi: eval out.setBytes end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				out.start=0;
				out.end=output.getBytes().length;
//				System.out.println("Shadi: eval start end : "+new java.sql.Timestamp((new java.util.Date()).getTime()));
				//					//System.out.println("Shadi:"+ output);

			}catch(Exception e)
			{
				e.printStackTrace();
			}


		}
	}

}

