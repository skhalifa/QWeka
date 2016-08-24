package ca.queensu.cs.aggregate;


import io.netty.buffer.DrillBuf;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.RepeatedVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import weka.classifiers.functions.Logistic;


public class Weka {

	/**
	 * @author shadi
	 * 
	 * select qdm_info_weka('?') 
	 * from `output100M.csv` as mydata;
	 * 
	 * OR
	 * 
	 * Select qdm_info_weka('nb') 
	 * from `output100M.csv` as mydata;
	 *
	 */
	@FunctionTemplate(name = "qdm_info_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaInfoSupportedArgs implements DrillAggFunc{

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
			//			System.out.println("In WekaTrainSupportedArgs output");
			out.buffer = tempBuff;
			byte[] operationBuf = new byte[operationStringLength.value];
			operationHolder.buffer.getBytes(0, operationBuf, 0, operationStringLength.value);
			String function = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();

			String helpText = "";

			org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
			java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
					reflections.getSubTypesOf(weka.classifiers.Classifier.class);

			java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
			boolean done = false;
			while(subTypesIterator.hasNext() && !done){
				String className = subTypesIterator.next().toString().substring(6);
				//					System.out.println(className.substring(className.indexOf("weka")));
				try{
					Class c = Class.forName(className.substring(className.indexOf("weka")));

					Object t = c.newInstance();
					Class clazz = Class.forName(c.getCanonicalName());
					Class[] interfaces = clazz.getInterfaces();
					String interfacesImplemented = "";
					for(int i=0;i<interfaces.length;i++){
						interfacesImplemented+=interfaces[i].getSimpleName()+" - ";
					}	
					for(Class superClazz = clazz.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
						interfaces = superClazz.getInterfaces();
						for(int i=0;i<interfaces.length;i++){
							interfacesImplemented+=interfaces[i].getSimpleName()+" - ";
						}	
					}
					helpText += "qdm_weka_train(\'"+c.getSimpleName()+"\',arguments,comma-separated features (label is the last column))\n\r";
					helpText += "Type qdm_weka_train(\'"+c.getSimpleName()+"\') for help\n\r";
					if(interfacesImplemented.contains("Aggregateable"))
						helpText +="Aggregateable"+"\n\r";
					if(interfacesImplemented.contains("UpdateableClassifier"))
						helpText +="Updateable"+"\n\r";
					helpText+="---------------------------------------------------------------------------\n\r";
					if(function.equalsIgnoreCase(c.getSimpleName())){
						helpText = "qdm_weka_train(\'"+c.getSimpleName()+"\',arguments,comma-separated features (label is the last column))\n\r";
						if(interfacesImplemented.contains("Aggregateable"))
							helpText +="Aggregateable"+"\n\r";
						if(interfacesImplemented.contains("UpdateableClassifier"))
							helpText +="Updateable"+"\n\r";
						helpText+="---------------------------------------------------------------------------\n\r";

						try{
							java.lang.reflect.Method m = c.getMethod("globalInfo");
							helpText+=":"+m.invoke(t)+"\n\r";
							m = c.getMethod("listOptions");
							java.util.Enumeration<weka.core.Option> e = (java.util.Enumeration<weka.core.Option>)m.invoke(t);

							while(e.hasMoreElements()){
								weka.core.Option tmp = ((weka.core.Option)e.nextElement());
								helpText+=tmp.name()+" : "+tmp.description()+"\n\r";
							}

							helpText+="-classes {c1,c2,c3}"+" : "+"List possible classes for the dataset. If not specified class becomes NUMERIC"+"\n\r"+"\n\r"+"\n\r";
							done = true;
						} catch (Exception e){
							e.printStackTrace();
						}

					}
				} catch (Exception e){

				}
			}

			if(helpText.length() == 0){
				helpText+=":"+"No Args";
			}

			helpText="info||"+helpText;
			out.buffer = out.buffer.reallocIfNeeded(helpText.length()+100);
			out.buffer.setBytes(0,helpText.getBytes(com.google.common.base.Charsets.UTF_8));
			out.start=0;
			out.end=helpText.length();
		}

		@Override
		public void reset() {
			operationHolder.start = operationHolder.end = 0; 

		}

	}




	/**
	 * @author shadi
	 * 
	 * 
	 * qdm_shuffle() assigns a sample number to each record so that they can be divided among the worker nodes where a classiefier is trained on each
	 * VOTE is then used to combine all workers classiers. 
	 * 
	 * Trian model xyz as 
	 * select qdm_train_weka('vote','args',model) 
	 * from 
	 * 		(select qdm_train_weka('alg','args', mydata.columns) as model 
	 * 		from 
	 * 			(select org_data.columns, qdm_shuffle(number of workers, org_data.columns[0],....) as sample 
	 * 			from `output100M100.csv` as org_data) as mydata 
	 * 			group by sample
	 * 		);
	 *
	 */

	@FunctionTemplate(name = "qdm_ladp", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
	public static class WekaLADP implements DrillSimpleFunc{
		@Param	IntHolder NumWorkers;
		@Param  VarCharHolder features;


		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;

		@Workspace VarCharHolder currVal;
		@Workspace MapHolder<String, Integer> nextSample;

		public void setup() {

			currVal = new VarCharHolder();
			nextSample = new MapHolder<String, Integer>();
			nextSample.map =  new java.util.HashMap<String, Integer>();

		}

		public void eval() {
			byte[] temp = new byte[features.end - features.start];
			features.buffer.getBytes(features.start, temp, 0, features.end - features.start);
			String rowData = new String(temp, com.google.common.base.Charsets.UTF_8);

			String[] attributes = rowData.split(",");
			String label = attributes[attributes.length-1];

			if(nextSample.map.get(label) == null){
				nextSample.map.put(label,0);
			}

			int tmp = Integer.parseInt(""+nextSample.map.get(label));
			String sample = ""+(tmp % NumWorkers.value);
			nextSample.map.put(label,tmp+1);


			out.buffer = tempBuff;
			out.buffer = out.buffer.reallocIfNeeded(sample.getBytes().length);
			out.buffer.setBytes(0, sample.getBytes());//.setBytes(0,outbuff);
			out.start=0;
			out.end=sample.getBytes().length;
		}
	}


	/**
	 * @author shadi
	 * 
	 * 
	 * qdm_shuffle() assigns a sample number to each record so that they can be divided among the worker nodes where a classiefier is trained on each
	 * VOTE is then used to combine all workers classiers. 
	 * 
	 * Train model xyz as 
	 * select qdm_train_weka('vote','args',model) 
	 * from 
	 * 		(select qdm_train_weka('alg','args', mydata.columns) as model 
	 * 		from 
	 * 			(select org_data.columns, qdm_shuffle(number of workers, org_data.columns) as sample 
	 * 			from `output100M100.csv` as org_data) as mydata 
	 * 			group by sample
	 * 		);
	 * Dataset Size
	 * Label count
	 * select count(*) from `output100M100.csv`
	 * select label, count(*) from `output100M100.csv` group by label
	 */

	@FunctionTemplate(name = "qdm_ladp", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
	public static class WekaLADPRepeated implements DrillSimpleFunc{
		@Param	IntHolder NumWorkers;
		@Param  RepeatedVarCharHolder features;


		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;

		@Workspace VarCharHolder currVal;
		@Workspace MapHolder<String, Integer> nextSample;

		public void setup() {

			currVal = new VarCharHolder();
			nextSample = new MapHolder<String, Integer>();
			nextSample.map =  new java.util.HashMap<String, Integer>();

		}

		public void eval() {	
			java.lang.StringBuilder rowBuilder = new java.lang.StringBuilder();
			for (int i = features.start; i < features.end; i++) {
				features.vector.getAccessor().get(i, currVal);
				rowBuilder.append(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(currVal.start, currVal.end, currVal.buffer)+",");
			} 
			String rowData = rowBuilder.substring(0, rowBuilder.length()-1);

			String[] attributes = rowData.split(",");
			String label = attributes[attributes.length-1];

			if(nextSample.map.get(label) == null){
				nextSample.map.put(label,0);
			}

			int tmp = Integer.parseInt(""+nextSample.map.get(label));
			String sample = ""+(tmp % NumWorkers.value);
			nextSample.map.put(label,tmp+1);


			out.buffer = tempBuff;
			out.buffer = out.buffer.reallocIfNeeded(sample.getBytes().length);
			out.buffer.setBytes(0, sample.getBytes());//.setBytes(0,outbuff);
			out.start=0;
			out.end=sample.getBytes().length;
		}
	}

	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_Ensemble_weka('nb','-classes {1,2}', mydata.columns[1], mydata.columns[2], mydata.columns[3], mydata.columns[4], mydata.columns[5]) 
	 * from `output100M.csv` as mydata;
	 *
	 */


	@FunctionTemplate(name = "qdm_ensemble_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainEnsembleColumns implements DrillAggFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace ObjectHolder classifier;
		@Workspace ObjectHolder function;
		@Workspace ObjectHolder arffHeader;
		//		@Workspace LinkedListHolder<weka.core.Instances> instancesList; 
		@Workspace ObjectHolder instancesHolder;
		@Workspace  BitHolder firstRun;
		//		@Workspace VarCharHolder currVal;
		@Workspace IntHolder updatable;
		@Workspace IntHolder aggregatable;

		public void setup() {
			classifier = new ObjectHolder();
			function = new ObjectHolder();
			arffHeader = new ObjectHolder();
			firstRun = new BitHolder();
			instancesHolder = new ObjectHolder();
			instancesHolder.obj = null;
			//			instancesList = new LinkedListHolder<weka.core.Instances>();
			//			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			//			instancesList.algorithm=null;
			//			instancesList.options = null;
			classifier.obj=null;
			function.obj=null;
			arffHeader.obj=null;
			firstRun.value=0;
			//			currVal = new VarCharHolder();
			updatable = new IntHolder();
			updatable.value=-1;
			aggregatable = new IntHolder();
			aggregatable.value=-1;
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
				function.obj = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
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
				arffHeader.obj = stBuilder.toString();

				org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
				java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
						reflections.getSubTypesOf(weka.classifiers.Classifier.class);

				java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
				boolean done = false;
				while(subTypesIterator.hasNext() && !done){
					String className = subTypesIterator.next().toString().substring(6);
					//					System.out.println(className.substring(className.indexOf("weka")));
					try {
						Class c = Class.forName(className.substring(className.indexOf("weka")));
						if(((String)function.obj).equalsIgnoreCase(c.getSimpleName())){
							function.obj = c.getCanonicalName();
							done =true;
						}
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}

				}


				try {
					Class<?> c = Class.forName(((String)function.obj));

					Class[] interfaces = c.getInterfaces();
					updatable.value = 0;
					aggregatable.value = 0;
					for(int i=0;i<interfaces.length;i++){
						if(interfaces[i].getSimpleName().contains("UpdateableClassifier")){
							updatable.value = 1;
						} else if(interfaces[i].getSimpleName().contains("Aggregateable")){
							aggregatable.value = 1;
						}
					}

					if(updatable.value == 0 || aggregatable.value == 0){
						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
							interfaces = superClazz.getInterfaces();
							for(int j=0;j<interfaces.length;j++){
								if(interfaces[j].getSimpleName().contains("UpdateableClassifier")){
									updatable.value = 1;
								} else if(interfaces[j].getSimpleName().contains("Aggregateable")){
									aggregatable.value = 1;
								}
							}	
						}
					}

				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}


			}

			//Start every run
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+rowData));

				instances.setClassIndex(instances.numAttributes() - 1);

				Class<?> c = Class.forName(((String)function.obj));

				//				if(updatable.value == 1 && aggregatable.value == 1){
				if(classifier.obj == null) {
					try{

						//						System.out.println("In WekaTrainAgg1Updateable create MODEL");
						classifier.obj = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();
						//						classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
						//						System.out.println("In WekaTrainAgg1Updateable options MODEL");
						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj), new Object[] {options});
						//						System.out.println("In WekaTrainAgg1Updateable options MODEL done");

						//						classifier.classifier.buildClassifier(instances);
						//						System.out.println("In WekaTrainAgg1Updateable build MODEL");
						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						m = c.getMethod("buildClassifier", weka.core.Instances.class);
						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),instances);
						//						System.out.println("In WekaTrainEnsemble1Updateable build MODEL done");

						if(updatable.value != 1) {
							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {
								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}
						}

					}catch(Exception e){
						e.printStackTrace();
					}
				} else {
					try{

						if(updatable.value == 1) {
							//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));

							//							System.out.println("In WekaTrainEnsemble1Updateable add MODEL updatable");
							java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
							m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),instances.instance(0));
							//							System.out.println("In WekaTrainEnsemble1Updateable updatable MODEL updated");
							//							NaiveBayesUpdateable
						} else {
							//							ZeroR

							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {
								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}

							//							System.out.println("In WekaTrainAgg1Updateable rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
							//							java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
							//							m.invoke(Class.forName(function.value).cast(classifier.classifier),((weka.core.Instances)instancesHolder.obj));
							//							System.out.println("In WekaTrainAgg1Updateable rebuilding MODEL updated");
						}

					}catch(Exception error){

						int MegaBytes = 1024 * 1024;
						System.out.println("Instances has: "+((weka.core.Instances)instancesHolder.obj).size()+ " records causing Out of memory error");
						long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
						long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
						long freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;


						System.out.println("totalMemory in JVM shows current size of java heap:"+totalMemory);
						System.out.println("maxMemory in JVM: " + maxMemory);
						System.out.println("freeMemory in JVM: " + freeMemory);
						System.out.println("Used Memory in JVM: " + (totalMemory - freeMemory));

						//Write instances to disk and reinitialize ((weka.core.Instances)instancesHolder.obj)???????????????????????????????????????????
						//						 PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("1.tmp", true)));
						//						 for(int j=0;j<objects.size();j++)
						//							 writer.append(objects.get(j)+"\n");
						//						 writer.close();
						//						 objects = new ArrayList<String>();

					}
				}
				//				} else {
				//					instancesList.list.add(instances);
				//					instancesList.algorithm = function.value;
				//					instancesList.options = options;
				//				}






				//				if ("ibk".equals(function.value)){
				//					try{
				//						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
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
				//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
				//					}catch(Exception ex){
				//						try{
				//							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
				//							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
				//							classifier.classifier.buildClassifier(instances);
				//						}catch(Exception e){
				//							e.printStackTrace();
				//						}
				//					}
				//				} 

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		@Override
		public void output() {
			try {

				if(instancesHolder.obj != null){
					System.out.println((classifier.obj != null)+" - "+ ((String)function.obj)+" - In WekaTrainEnsemble1Updateable output rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
					Class<?> c = Class.forName(((String)function.obj));
					java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
					m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),((weka.core.Instances)instancesHolder.obj));
					System.out.println("In WekaTrainEnsemble1Updateable output rebuilding MODEL updated");

				} 


				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();

				weka.core.SerializationHelper.write(os, classifier.obj);

				//				if(classifier.classifier!=null && instancesList.list.size() == 0){
				//					weka.core.SerializationHelper.write(os, classifier.classifier);
				//				} else {
				//					//TODO: Handle small datasets. Train model here. 
				//					
				//					weka.core.SerializationHelper.write(os, instancesList);
				//				}

				byte[] data = os.toByteArray();
				tempBuff = tempBuff.reallocIfNeeded(data.length);
				out.buffer = tempBuff;
				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
				out.start=0;
				out.end=data.length;
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
	 * select qdm_Ensemble_weka('nb','-classes {1,2}', mydata.columns) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "qdm_ensemble_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainEnsembleNoSample implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  RepeatedVarCharHolder features;
//		@Param  VarCharHolder sample;

		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace ObjectHolder classifier;
		@Workspace ObjectHolder function;
		@Workspace ObjectHolder arffHeader;
		@Workspace ObjectHolder instancesHolder;
		@Workspace ObjectHolder writerHolder;
		@Workspace ObjectHolder optionsHolder;
		@Workspace ObjectHolder pathsHolder;
		@Workspace ObjectHolder nextPartition;
		@Workspace  BitHolder firstRun;
		@Workspace  BitHolder fillInstances;
		@Workspace IntHolder updatable;
		@Workspace IntHolder aggregatable;
		@Workspace ObjectHolder writingPosition;

		public void setup() {
			classifier = new ObjectHolder();
			function = new ObjectHolder();
			arffHeader = new ObjectHolder();
			firstRun = new BitHolder();
			fillInstances = new BitHolder();
			instancesHolder = new ObjectHolder();
			instancesHolder.obj = null;
			writerHolder = new ObjectHolder();
			writerHolder.obj = null;
			optionsHolder = new ObjectHolder();
			optionsHolder.obj = null;
			pathsHolder = new ObjectHolder();
			pathsHolder.obj = null;
			nextPartition = new ObjectHolder();
			nextPartition.obj =  new java.util.HashMap<String, Integer>();
			classifier.obj=null;
			function.obj=null;
			arffHeader.obj=null;
			firstRun.value=0;
			fillInstances.value=1;
			updatable = new IntHolder();
			updatable.value=-1;
			aggregatable = new IntHolder();
			aggregatable.value=-1;
			writingPosition = new ObjectHolder();
			writingPosition.obj= new Long[]{0L, 0L};
		}

		@Override
		public void add() {

//			byte[] temp = new byte[sample.end - sample.start];
//			sample.buffer.getBytes(sample.start, temp, 0, sample.end - sample.start);



			java.lang.StringBuilder rowBuilder = new java.lang.StringBuilder();
			VarCharHolder currVal = new VarCharHolder();
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
				function.obj = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
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
				arffHeader.obj = stBuilder.toString();

				org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
				java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
						reflections.getSubTypesOf(weka.classifiers.Classifier.class);

				java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
				boolean done = false;
				while(subTypesIterator.hasNext() && !done){
					String className = subTypesIterator.next().toString().substring(6);
					//					System.out.println(className.substring(className.indexOf("weka")));
					try {
						Class c = Class.forName(className.substring(className.indexOf("weka")));
						if(((String)function.obj).equalsIgnoreCase(c.getSimpleName())){
							function.obj = c.getCanonicalName();
							done =true;
						}
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}

				}


				try {
					Class<?> c = Class.forName(((String)function.obj));

					Class[] interfaces = c.getInterfaces();
					updatable.value = 0;
					aggregatable.value = 0;
					for(int i=0;i<interfaces.length;i++){
						if(interfaces[i].getSimpleName().contains("UpdateableClassifier")){
							updatable.value = 1;
						} else if(interfaces[i].getSimpleName().contains("Aggregateable")){
							aggregatable.value = 1;
						}
					}

					if(updatable.value == 0 || aggregatable.value == 0){
						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
							interfaces = superClazz.getInterfaces();
							for(int j=0;j<interfaces.length;j++){
								if(interfaces[j].getSimpleName().contains("UpdateableClassifier")){
									updatable.value = 1;
								} else if(interfaces[j].getSimpleName().contains("Aggregateable")){
									aggregatable.value = 1;
								}
							}	
						}
					}

				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				writerHolder.obj = new java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>();

				try{

					pathsHolder.obj = new String[] {"1_"+System.currentTimeMillis()+".arff","2_"+System.currentTimeMillis()+".arff"};
					((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
					add(java.nio.channels.AsynchronousFileChannel.open(java.nio.file.Paths.get(((String[])pathsHolder.obj)[0]), java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE));

					((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
					add(java.nio.channels.AsynchronousFileChannel.open(java.nio.file.Paths.get(((String[])pathsHolder.obj)[1]), java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE));

					//					java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");

					//					ByteBuffer dataBuffer = java.nio.ByteBuffer.wrap(((String)arffHeader.obj).getBytes(cs));

					//					((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
					//					get(0).
					//					write(dataBuffer, ((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(0).size());
					//
					//					((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
					//					get(1).
					//					write(dataBuffer, ((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(1).size());

				} catch(Exception ex){
					ex.printStackTrace();
				}
			}

			//Start every run
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+rowData));

				instances.setClassIndex(instances.numAttributes() - 1);
//				System.out.println("num attributes = "+instances.numAttributes() + " - ClassIndex = "+instances.classIndex());

				Class<?> c = Class.forName(((String)function.obj));

				//				if(updatable.value == 1 && aggregatable.value == 1){
				if(classifier.obj == null) {
					try{

						optionsHolder.obj = options;

						//						System.out.println("In WekaTrainAgg1Updateable create MODEL");
						classifier.obj = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();

						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj), new Object[] {options});

						m = c.getMethod("buildClassifier", weka.core.Instances.class);
						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),instances);

						//						System.out.println("In WekaTrainEnsemble1Updateable build MODEL done");

						if(updatable.value != 1) {
							//TODO: handle first record for NonUpdatable_NonAggregatable, Currently first record is discarded
							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {

								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}
							//							System.out.println("In WekaTrainEnsemble1Updateable build MODEL Not updatable add instances");
						}

					}catch(Exception e){
						e.printStackTrace();
					}
				} else {


					if(updatable.value == 1) {
						//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));

						//							System.out.println("In WekaTrainEnsemble1Updateable add MODEL updatable");
						java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),instances.instance(0));
						//							System.out.println("In WekaTrainEnsemble1Updateable updatable MODEL updated");
						//							NaiveBayesUpdateable
					} else {
						//							ZeroR



						if(instancesHolder.obj == null){

							instancesHolder.obj = instances;
						} else {

							if(aggregatable.value > 0 && ((weka.core.Instances)instancesHolder.obj).size() > 10000){

								if(aggregatable.value == 1){
									classifier.obj = (weka.classifiers.Classifier) c.newInstance(); 

									java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
									m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj), new Object[] {options});

									m = c.getMethod("buildClassifier", weka.core.Instances.class);
									m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),((weka.core.Instances)instancesHolder.obj));
									//											System.out.println("Init Aggregation");
									aggregatable.value = 2;

								} else {

									weka.classifiers.Classifier newClassifier = (weka.classifiers.Classifier) c.newInstance(); 

									java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
									m.invoke(Class.forName(((String)function.obj)).cast(newClassifier), new Object[] {options});

									m = c.getMethod("buildClassifier", weka.core.Instances.class);
									m.invoke(Class.forName(((String)function.obj)).cast(newClassifier),((weka.core.Instances)instancesHolder.obj));
									//										System.out.println("Aggregation instance class index is:"+((weka.core.Instances)instancesHolder.obj).classIndex());

									try{
										m = c.getMethod("aggregate",c);
										m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
										//									System.out.println("In WekaTrainAgg2Updateable add MODEL aggregated");
									} catch (java.lang.NoSuchMethodException ex){
										m = c.getMethod("aggregate",c.getSuperclass());
										m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
										//									System.out.println("In WekaTrainAgg2Updateable add MODEL parent aggregated");
									}
									//											System.out.println("Do Aggregation");

								}

								// reinitialize instancesHolder.obj
								((weka.core.Instances)instancesHolder.obj).delete();
								instancesHolder.obj = instances;

							} else {
								//TODO: need handing for memory allocation
								String[] attributes = rowData.split(",");
								String label = attributes[attributes.length-1];

								if(((java.util.HashMap<String, Integer>)nextPartition.obj).get(label) == null){
									((java.util.HashMap<String, Integer>)nextPartition.obj).put(label,0);
								}

								int tmp = Integer.parseInt(""+((java.util.HashMap<String, Integer>)nextPartition.obj).get(label));
								((java.util.HashMap<String, Integer>)nextPartition.obj).put(label,((tmp+1) % 2));


								java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");

								java.nio.ByteBuffer dataBuffer = java.nio.ByteBuffer.wrap((rowData+"\n").getBytes(cs));

//								long position = ((java.nio.channels.AsynchronousFileChannel)((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(tmp)).size();

								//TODO: FIX WRITTING RECORDS
								((java.nio.channels.AsynchronousFileChannel)((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(tmp)).write(dataBuffer, ((Long[])writingPosition.obj)[tmp]);
								((Long[])writingPosition.obj)[tmp] = ((Long[])writingPosition.obj)[tmp] + (rowData+"\n").getBytes(cs).length;
										
								if(fillInstances.value==1){								
									try{

										((weka.core.Instances)instancesHolder.obj).add(instances.get(0));

									} catch(Exception error){
										error.printStackTrace();

										fillInstances.value = 0;

										int MegaBytes = 1024 * 1024;
										System.out.println("In 1:  Instances has: "+((weka.core.Instances)instancesHolder.obj).size()+ " records causing Out of memory error");
										long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
										long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
										long freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;


										System.out.println("totalMemory in JVM shows current size of java heap:"+totalMemory);
										System.out.println("maxMemory in JVM: " + maxMemory);
										System.out.println("freeMemory in JVM: " + freeMemory);
										System.out.println("Used Memory in JVM: " + (totalMemory - freeMemory));

										((weka.core.Instances)instancesHolder.obj).delete();


									}
								}
							}
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

				if(instancesHolder.obj != null){
					System.out.println((classifier.obj != null)+" - "+ ((String)function.obj)+" - In WekaTrainEnsemble1Updateable output rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
					Class<?> c = Class.forName(((String)function.obj));

					if(aggregatable.value > 0){
						System.out.println("In ensemble1 agg in out");

						if(aggregatable.value == 1){
							classifier.obj = (weka.classifiers.Classifier) c.newInstance(); 

							java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
							m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),((weka.core.Instances)instancesHolder.obj));
							System.out.println("Init Aggregation");
							aggregatable.value = 2;

						} else {

							weka.classifiers.Classifier newClassifier = (weka.classifiers.Classifier) c.newInstance(); 


							java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
							m.invoke(Class.forName(((String)function.obj)).cast(newClassifier), new Object[] {(String[])optionsHolder.obj});


							m = c.getMethod("buildClassifier", weka.core.Instances.class);
							m.invoke(Class.forName(((String)function.obj)).cast(newClassifier),((weka.core.Instances)instancesHolder.obj));
							//										System.out.println("Aggregation instance class index is:"+((weka.core.Instances)instancesHolder.obj).classIndex());

							try{
								m = c.getMethod("aggregate",c);
								m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
								//									System.out.println("In WekaTrainAgg2Updateable add MODEL aggregated");
							} catch (java.lang.NoSuchMethodException ex){
								m = c.getMethod("aggregate",c.getSuperclass());
								m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
								//									System.out.println("In WekaTrainAgg2Updateable add MODEL parent aggregated");
							}
							System.out.println("Do Aggregation");

						}

						// reinitialize instancesHolder.obj
						((weka.core.Instances)instancesHolder.obj).delete();
						instancesHolder.obj = null;

						System.out.println("In ensemble1 agg in out DONE");

					} else {

						//TODO: Handle the memory for this case

						java.io.File f = new java.io.File(((String[])pathsHolder.obj)[0]);

						int MegaBytes = 1024 * 1024;
						long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
						long fileSize = f.length() / MegaBytes;
						long numSubSamples = fileSize*2/maxMemory;

						System.out.println("In function:  data size: "+(2*fileSize)+ " with "+maxMemory+" Max Memory.");

						if(2*fileSize*2/maxMemory < 2){
							try{

								System.out.println("Attempting to train using all data of size:"+ ((weka.core.Instances)instancesHolder.obj).size());
								java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);

								m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),(weka.core.Instances)instancesHolder.obj);
								System.out.println("Attempt SUCCESS to train using all data of size:"+ ((weka.core.Instances)instancesHolder.obj).size());

							} catch(Exception error){
								error.printStackTrace();
							}
						} else if (numSubSamples == 1) {
							//If file can fit in memory train 2 models one on each file
							System.out.println("In 4: Train using all data failed where Instances has: "+((weka.core.Instances)instancesHolder.obj).size()+ " records causing Out of memory error ("+maxMemory+"MB RAM)");
							System.out.println("Now trying to train from the 2 subpartion files");


							//								classifier.obj = this.trainLDAPfile((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj);


							System.out.println("In function:  file size: "+fileSize+ " with "+maxMemory+" Max Memory. Num SubSamples = "+numSubSamples);


							classifier.obj = new weka.classifiers.meta.Vote();

							for(int i=0;i<2;i++){
								System.out.println("Using only the 2 files. Reading file"+((String[])pathsHolder.obj)[i]);

								java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(((String[])pathsHolder.obj)[i]));

								((weka.core.Instances)instancesHolder.obj).delete();

								System.out.println("Before Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");

								String record = "";
								int failCount = 0;
								long startTime = System.currentTimeMillis();
								while ((record = br.readLine()) != null) {
									try {
//										((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+record)));
										((weka.core.Instances)instancesHolder.obj).add(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+record)).get(0));
									} catch (Exception e) {
										// TODO Auto-generated catch block
										//													e.printStackTrace();
										System.out.println("Failed at this record\n"+record);
										failCount++;
									}

								}
								long endTime = System.currentTimeMillis();

								System.out.println("After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances. loaded in "+((endTime-startTime)/1000)+" sec");
								System.out.println("Failed to load: "+failCount+" instances");

								startTime = System.currentTimeMillis();
								try{
									weka.classifiers.Classifier subClassifier = (weka.classifiers.Classifier) c.newInstance(); 

									java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
									m.invoke(Class.forName(((String)function.obj)).cast(subClassifier), new Object[] {(String[])optionsHolder.obj});

									System.out.println("Building subClassifier");
									m = c.getMethod("buildClassifier", weka.core.Instances.class);
									m.invoke(Class.forName(((String)function.obj)).cast(subClassifier),((weka.core.Instances)instancesHolder.obj));


									System.out.println("add subClassifier to voter");
									((weka.classifiers.meta.Vote)classifier.obj).addPreBuiltClassifier(subClassifier);							
									System.out.println("subClassifier added ");

								} catch (Exception ex){
									ex.printStackTrace();
								}

								endTime = System.currentTimeMillis();
								
								System.out.println("subclassifier trained in "+((endTime-startTime)/1000)+" sec");
								
								br.close();
								((java.nio.channels.AsynchronousFileChannel)((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(i)).close();
								java.io.File file = new java.io.File(((String[])pathsHolder.obj)[i]);
								System.out.println("File "+((String[])pathsHolder.obj)[i]+(file.delete()?" Deleted":" Failed to Delete"));


							} 

						} else {
							//else read files, split using ladp to the number of samples and train models

							//combine models using voting

							System.out.println("More partions needed!!!");

						}
					} 

					System.out.println("In WekaTrainEnsemble1Updateable output rebuilding MODEL updated");
				} 


				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();

				weka.core.SerializationHelper.write(os, classifier.obj);


				byte[] data = os.toByteArray();
				tempBuff = tempBuff.reallocIfNeeded(data.length);
				out.buffer = tempBuff;
				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
				out.start=0;
				out.end=data.length;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		@Override
		public void reset() {
		}

		/*		public weka.classifiers.Classifier trainLDAPfile(java.util.ArrayList<java.nio.channels.AsynchronousFileChannel> afcList) {

			try {
				int MegaBytes = 1024 * 1024;
				long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
				long fileSize = afcList.get(0).size() / MegaBytes;

				long numSubSamples = fileSize*2/maxMemory;

				System.out.println("In function:  file size: "+fileSize+ " with "+maxMemory+" Max Memory. Num SubSamples = "+numSubSamples);


				System.out.println("Data won't fit in memory. Training a voting ensmble on subpartitions");
				classifier.obj = new weka.classifiers.meta.Vote();


				//If file can fit in memory train 2 models one on each file
				if(numSubSamples == 1){
					java.io.File f = new java.io.File(".");
					java.io.File[] matchingFiles = f.listFiles(new java.io.FilenameFilter() {
					    public boolean accept(java.io.File dir, String name) {
					        return name.endsWith("arff");
					    }
					});


					for(int i=0;i<matchingFiles.length;i++){
						System.out.println("Using only the 2 files. Reading file"+matchingFiles[i].toPath());

						System.out.println("Before 1 Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");


						java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(matchingFiles[i]));

						((weka.core.Instances)instancesHolder.obj).delete();

						System.out.println("Before 2 Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");


						String record;
					    while ((record = br.readLine()) != null) {
					    	try {
								((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+record)));
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					    }

					    System.out.println("After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");

						try{
							Class<?> c = Class.forName(((String)function.obj));
							weka.classifiers.Classifier subClassifier = (weka.classifiers.Classifier) c.newInstance(); 

							java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
							m.invoke(Class.forName(((String)function.obj)).cast(subClassifier), new Object[] {(String[])optionsHolder.obj});

							System.out.println("Building subClassifier");
							m = c.getMethod("buildClassifier", weka.core.Instances.class);
							m.invoke(Class.forName(((String)function.obj)).cast(subClassifier),((weka.core.Instances)instancesHolder.obj));


							System.out.println("add subClassifier to voter");
							((weka.classifiers.meta.Vote)classifier.obj).addPreBuiltClassifier(subClassifier);							
							System.out.println("subClassifier added ");

						} catch (Exception ex){
							ex.printStackTrace();
						}

//						java.nio.channels.AsynchronousFileChannel afc = java.nio.channels.AsynchronousFileChannel.open(matchingFiles[i].toPath(), java.nio.file.StandardOpenOption.READ);
//					    ReadHandler handler = new ReadHandler();
//					    FinalReadHandler finalHandler = new FinalReadHandler();
//					    int fSize = (int) afc.size();
//						
//					    long readSoFar = 0; //Math.min(Integer.MAX_VALUE -1 , fSize);
//					    while(readSoFar < fSize){
//					    	
//						    java.nio.ByteBuffer dataBuffer = java.nio.ByteBuffer.allocate((int)Math.min(Integer.MAX_VALUE -1 , (fSize-readSoFar)));
//	
//						    Attachment attach = new Attachment();
//						    attach.asyncChannel = afc;
//						    attach.buffer = dataBuffer;
//						    attach.path = matchingFiles[i].toPath();
//	
//						    if((Integer.MAX_VALUE -1) < (fSize-readSoFar)){
//						    	System.out.println("regular handler");
//						    	afc.read(dataBuffer, 0, attach, handler);
//						    } else {
//						    	System.out.println("last handler");
//						    	afc.read(dataBuffer, 0, attach, finalHandler);
//						    }
//						    
//						    readSoFar += dataBuffer.capacity();
//						    
//						    System.out.println("Read "+ dataBuffer.capacity() + " bytes and remaing "+(fSize-readSoFar)+" bytes");
//						    
//					    }




					}


				} else {
				//else read files, split using ladp to the number of samples and train models

				//combine models using voting

					System.out.println("More partions needed!!!");

				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			return null;
		}
		 */


		/*		class Attachment {
			  public java.nio.file.Path path;
			  public java.nio.ByteBuffer buffer;
			  public java.nio.channels.AsynchronousFileChannel asyncChannel;
		}

		class ReadHandler implements java.nio.channels.CompletionHandler<Integer, Attachment> {
			  @Override
			  public void completed(Integer result, Attachment attach) {
//			    System.out.format("%s bytes read   from  %s%n", result, attach.path);
//			    System.out.format("Read data is:%n");
			    byte[] byteData = attach.buffer.array();
			    java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");
			    String data = new String(byteData, cs);
			    data = data.substring(data.indexOf("\n"), data.lastIndexOf("\n"));
//			    System.out.println(data);

			    System.out.println("Regular Before Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");

			    try {
					((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+data)));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}


			    System.out.println("Regular After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
			  }

			  @Override
			  public void failed(Throwable e, Attachment attach) {
			    System.out.format("Read operation  on  %s  file failed."
			        + "The  error is: %s%n", attach.path, e.getMessage());
			    try {
			      // Close the channel
//			      attach.asyncChannel.close();
			    } catch (Exception e1) {
			      e1.printStackTrace();
			    }
			  }
			}

		class FinalReadHandler implements java.nio.channels.CompletionHandler<Integer, Attachment> {
			  @Override
			  public void completed(Integer result, Attachment attach) {
//				    System.out.format("%s bytes read   from  %s%n", result, attach.path);
//				    System.out.format("Read data is:%n");
				    byte[] byteData = attach.buffer.array();
				    java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");
				    String data = new String(byteData, cs);
				    data = data.substring(data.indexOf("\n"), data.lastIndexOf("\n"));
//				    System.out.println(data);

				    System.out.println("Final Before Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");

				    try {
						((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+data)));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}


				    System.out.println("Final After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");


				    ((weka.core.Instances)instancesHolder.obj).setClassIndex(((weka.core.Instances)instancesHolder.obj).numAttributes() - 1);


			    try {
			      // Close the channel
			      attach.asyncChannel.close();
			      System.out.println(attach.path+" is now closed");
			      //delete file
			      java.nio.file.Files.delete(attach.path);
			      System.out.println(attach.path + " is now deleted");

			    } catch (Exception e) {
			      e.printStackTrace();
			    }
			  }

			  @Override
			  public void failed(Throwable e, Attachment attach) {
			    System.out.format("Read operation  on  %s  file failed."
			        + "The  error is: %s%n", attach.path, e.getMessage());
			    try {
			      // Close the channel
			      attach.asyncChannel.close();
			    } catch (Exception e1) {
			      e1.printStackTrace();
			    }
			  }
			}
		 */
	}

	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_Ensemble_weka('nb','-classes {1,2}', mydata.columns, sample) 
	 * from `output100M.csv` as mydata;
	 *
	 */

//	@FunctionTemplate(name = "qdm_ensemble_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
//	public static class WekaTrainEnsemble implements DrillAggFunc{
//
//		@Param  VarCharHolder operation;
//		@Param  VarCharHolder arguments;
//		@Param  RepeatedVarCharHolder features;
//		@Param  VarCharHolder sample;
//
//		@Output VarCharHolder out;
//		@Inject DrillBuf tempBuff;
//		@Workspace ObjectHolder classifier;
//		@Workspace ObjectHolder function;
//		@Workspace ObjectHolder arffHeader;
//		@Workspace ObjectHolder instancesHolder;
//		@Workspace ObjectHolder writerHolder;
//		@Workspace ObjectHolder optionsHolder;
//		@Workspace ObjectHolder pathsHolder;
//		@Workspace ObjectHolder nextPartition;
//		@Workspace  BitHolder firstRun;
//		@Workspace  BitHolder fillInstances;
//		@Workspace IntHolder updatable;
//		@Workspace IntHolder aggregatable;
//		@Workspace ObjectHolder writingPosition;
//
//		public void setup() {
//			classifier = new ObjectHolder();
//			function = new ObjectHolder();
//			arffHeader = new ObjectHolder();
//			firstRun = new BitHolder();
//			fillInstances = new BitHolder();
//			instancesHolder = new ObjectHolder();
//			instancesHolder.obj = null;
//			writerHolder = new ObjectHolder();
//			writerHolder.obj = null;
//			optionsHolder = new ObjectHolder();
//			optionsHolder.obj = null;
//			pathsHolder = new ObjectHolder();
//			pathsHolder.obj = null;
//			nextPartition = new ObjectHolder();
//			nextPartition.obj =  new java.util.HashMap<String, Integer>();
//			classifier.obj=null;
//			function.obj=null;
//			arffHeader.obj=null;
//			firstRun.value=0;
//			fillInstances.value=1;
//			updatable = new IntHolder();
//			updatable.value=-1;
//			aggregatable = new IntHolder();
//			aggregatable.value=-1;
//			writingPosition = new ObjectHolder();
//			writingPosition.obj= new Long[]{0L, 0L};
//		}
//
//		@Override
//		public void add() {
//
//			byte[] temp = new byte[sample.end - sample.start];
//			sample.buffer.getBytes(sample.start, temp, 0, sample.end - sample.start);
//
//
//
//			java.lang.StringBuilder rowBuilder = new java.lang.StringBuilder();
//			VarCharHolder currVal = new VarCharHolder();
//			for (int i = features.start; i < features.end; i++) {
//				features.vector.getAccessor().get(i, currVal);
//				rowBuilder.append(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(currVal.start, currVal.end, currVal.buffer)+",");
//			}
//			String rowData = rowBuilder.substring(0, rowBuilder.length()-1);
//			String [] options = null;
//			if(firstRun.value==0){
//				firstRun.value = 1;
//				byte[] operationBuf = new byte[operation.end - operation.start];
//				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
//				function.obj = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
//				java.util.StringTokenizer st = new java.util.StringTokenizer(rowData, ",");
//				int attributesCount = st.countTokens();
//				java.lang.StringBuilder stBuilder = new java.lang.StringBuilder();
//
//				byte[] argsBuf = new byte[arguments.end - arguments.start];
//				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
//				String classType = "numeric";
//				try {
//					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
//					for(int i=0;i<options.length;i++){
//						if(options[i].indexOf("classes")>0){
//							classType = options[i+1];
//							options[i]="";
//							options[i+1]="";
//						}
//					}
//				} catch (Exception e1) {
//					e1.printStackTrace();
//				}
//				//				stBuilder.append(function.value+"||"+options+"\n");
//				stBuilder.append("@"+"RELATION Drill\n");
//				for(int i=0; i< attributesCount-1;i++)
//				{
//					stBuilder.append("@"+"ATTRIBUTE att"+i+" numeric\n");
//				}
//				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
//				stBuilder.append("@"+"DATA\n");
//				arffHeader.obj = stBuilder.toString();
//
//				org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
//				java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
//						reflections.getSubTypesOf(weka.classifiers.Classifier.class);
//
//				java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
//				boolean done = false;
//				while(subTypesIterator.hasNext() && !done){
//					String className = subTypesIterator.next().toString().substring(6);
//					//					System.out.println(className.substring(className.indexOf("weka")));
//					try {
//						Class c = Class.forName(className.substring(className.indexOf("weka")));
//						if(((String)function.obj).equalsIgnoreCase(c.getSimpleName())){
//							function.obj = c.getCanonicalName();
//							done =true;
//						}
//					} catch (ClassNotFoundException e) {
//						e.printStackTrace();
//					}
//
//				}
//
//
//				try {
//					Class<?> c = Class.forName(((String)function.obj));
//
//					Class[] interfaces = c.getInterfaces();
//					updatable.value = 0;
//					aggregatable.value = 0;
//					for(int i=0;i<interfaces.length;i++){
//						if(interfaces[i].getSimpleName().contains("UpdateableClassifier")){
//							updatable.value = 1;
//						} else if(interfaces[i].getSimpleName().contains("Aggregateable")){
//							aggregatable.value = 1;
//						}
//					}
//
//					if(updatable.value == 0 || aggregatable.value == 0){
//						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
//							interfaces = superClazz.getInterfaces();
//							for(int j=0;j<interfaces.length;j++){
//								if(interfaces[j].getSimpleName().contains("UpdateableClassifier")){
//									updatable.value = 1;
//								} else if(interfaces[j].getSimpleName().contains("Aggregateable")){
//									aggregatable.value = 1;
//								}
//							}	
//						}
//					}
//
//				} catch (ClassNotFoundException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//
//				writerHolder.obj = new java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>();
//
//				try{
//
//					pathsHolder.obj = new String[] {"1_"+System.currentTimeMillis()+".arff","2_"+System.currentTimeMillis()+".arff"};
//					((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
//					add(java.nio.channels.AsynchronousFileChannel.open(java.nio.file.Paths.get(((String[])pathsHolder.obj)[0]), java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE));
//
//					((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
//					add(java.nio.channels.AsynchronousFileChannel.open(java.nio.file.Paths.get(((String[])pathsHolder.obj)[1]), java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.CREATE));
//
//					//					java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");
//
//					//					ByteBuffer dataBuffer = java.nio.ByteBuffer.wrap(((String)arffHeader.obj).getBytes(cs));
//
//					//					((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
//					//					get(0).
//					//					write(dataBuffer, ((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(0).size());
//					//
//					//					((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).
//					//					get(1).
//					//					write(dataBuffer, ((ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(1).size());
//
//				} catch(Exception ex){
//					ex.printStackTrace();
//				}
//			}
//
//			//Start every run
//			try {
//				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+rowData));
//
//				instances.setClassIndex(instances.numAttributes() - 1);
//
//				Class<?> c = Class.forName(((String)function.obj));
//
//				//				if(updatable.value == 1 && aggregatable.value == 1){
//				if(classifier.obj == null) {
//					try{
//
//						optionsHolder.obj = options;
//
//						//						System.out.println("In WekaTrainAgg1Updateable create MODEL");
//						classifier.obj = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();
//
//						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
//						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj), new Object[] {options});
//
//						m = c.getMethod("buildClassifier", weka.core.Instances.class);
//						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),instances);
//
//						//						System.out.println("In WekaTrainEnsemble1Updateable build MODEL done");
//
//						if(updatable.value != 1) {
//							//TODO: handle first record for NonUpdatable_NonAggregatable, Currently first record is discarded
//							if(instancesHolder.obj == null){
//								instancesHolder.obj = instances;
//							} else {
//
//								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
//							}
//							//							System.out.println("In WekaTrainEnsemble1Updateable build MODEL Not updatable add instances");
//						}
//
//					}catch(Exception e){
//						e.printStackTrace();
//					}
//				} else {
//
//
//					if(updatable.value == 1) {
//						//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
//
//						//							System.out.println("In WekaTrainEnsemble1Updateable add MODEL updatable");
//						java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
//						m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),instances.instance(0));
//						//							System.out.println("In WekaTrainEnsemble1Updateable updatable MODEL updated");
//						//							NaiveBayesUpdateable
//					} else {
//						//							ZeroR
//
//
//
//						if(instancesHolder.obj == null){
//
//							instancesHolder.obj = instances;
//						} else {
//
//							if(aggregatable.value > 0 && ((weka.core.Instances)instancesHolder.obj).size() > 10000){
//
//								if(aggregatable.value == 1){
//									classifier.obj = (weka.classifiers.Classifier) c.newInstance(); 
//
//									java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
//									m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj), new Object[] {options});
//
//									m = c.getMethod("buildClassifier", weka.core.Instances.class);
//									m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),((weka.core.Instances)instancesHolder.obj));
//									//											System.out.println("Init Aggregation");
//									aggregatable.value = 2;
//
//								} else {
//
//									weka.classifiers.Classifier newClassifier = (weka.classifiers.Classifier) c.newInstance(); 
//
//									java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
//									m.invoke(Class.forName(((String)function.obj)).cast(newClassifier), new Object[] {options});
//
//									m = c.getMethod("buildClassifier", weka.core.Instances.class);
//									m.invoke(Class.forName(((String)function.obj)).cast(newClassifier),((weka.core.Instances)instancesHolder.obj));
//									//										System.out.println("Aggregation instance class index is:"+((weka.core.Instances)instancesHolder.obj).classIndex());
//
//									try{
//										m = c.getMethod("aggregate",c);
//										m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
//										//									System.out.println("In WekaTrainAgg2Updateable add MODEL aggregated");
//									} catch (java.lang.NoSuchMethodException ex){
//										m = c.getMethod("aggregate",c.getSuperclass());
//										m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
//										//									System.out.println("In WekaTrainAgg2Updateable add MODEL parent aggregated");
//									}
//									//											System.out.println("Do Aggregation");
//
//								}
//
//								// reinitialize instancesHolder.obj
//								((weka.core.Instances)instancesHolder.obj).delete();
//								instancesHolder.obj = instances;
//
//							} else {
//								//TODO: need handing for memory allocation
//								String[] attributes = rowData.split(",");
//								String label = attributes[attributes.length-1];
//
//								if(((java.util.HashMap<String, Integer>)nextPartition.obj).get(label) == null){
//									((java.util.HashMap<String, Integer>)nextPartition.obj).put(label,0);
//								}
//
//								int tmp = Integer.parseInt(""+((java.util.HashMap<String, Integer>)nextPartition.obj).get(label));
//								((java.util.HashMap<String, Integer>)nextPartition.obj).put(label,((tmp+1) % 2));
//
//
//								java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");
//
//								java.nio.ByteBuffer dataBuffer = java.nio.ByteBuffer.wrap((rowData+"\n").getBytes(cs));
//
////								long position = ((java.nio.channels.AsynchronousFileChannel)((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(tmp)).size();
//
//								//TODO: FIX WRITTING RECORDS
//								((java.nio.channels.AsynchronousFileChannel)((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(tmp)).write(dataBuffer, ((Long[])writingPosition.obj)[tmp]);
//								((Long[])writingPosition.obj)[tmp] = ((Long[])writingPosition.obj)[tmp] + (rowData+"\n").getBytes(cs).length;
//										
//								if(fillInstances.value==1){								
//									try{
//
//										((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
//
//									} catch(Exception error){
//										error.printStackTrace();
//
//										fillInstances.value = 0;
//
//										int MegaBytes = 1024 * 1024;
//										System.out.println("In 1:  Instances has: "+((weka.core.Instances)instancesHolder.obj).size()+ " records causing Out of memory error");
//										long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
//										long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
//										long freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
//
//
//										System.out.println("totalMemory in JVM shows current size of java heap:"+totalMemory);
//										System.out.println("maxMemory in JVM: " + maxMemory);
//										System.out.println("freeMemory in JVM: " + freeMemory);
//										System.out.println("Used Memory in JVM: " + (totalMemory - freeMemory));
//
//										((weka.core.Instances)instancesHolder.obj).delete();
//
//
//									}
//								}
//							}
//						}
//					}
//
//				}
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//		}
//		@Override
//		public void output() {
//			try {
//
//				if(instancesHolder.obj != null){
//					System.out.println((classifier.obj != null)+" - "+ ((String)function.obj)+" - In WekaTrainEnsemble1Updateable output rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
//					Class<?> c = Class.forName(((String)function.obj));
//
//					if(aggregatable.value > 0){
//						System.out.println("In ensemble1 agg in out");
//
//						if(aggregatable.value == 1){
//							classifier.obj = (weka.classifiers.Classifier) c.newInstance(); 
//
//							java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
//							m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),((weka.core.Instances)instancesHolder.obj));
//							System.out.println("Init Aggregation");
//							aggregatable.value = 2;
//
//						} else {
//
//							weka.classifiers.Classifier newClassifier = (weka.classifiers.Classifier) c.newInstance(); 
//
//
//							java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
//							m.invoke(Class.forName(((String)function.obj)).cast(newClassifier), new Object[] {(String[])optionsHolder.obj});
//
//
//							m = c.getMethod("buildClassifier", weka.core.Instances.class);
//							m.invoke(Class.forName(((String)function.obj)).cast(newClassifier),((weka.core.Instances)instancesHolder.obj));
//							//										System.out.println("Aggregation instance class index is:"+((weka.core.Instances)instancesHolder.obj).classIndex());
//
//							try{
//								m = c.getMethod("aggregate",c);
//								m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
//								//									System.out.println("In WekaTrainAgg2Updateable add MODEL aggregated");
//							} catch (java.lang.NoSuchMethodException ex){
//								m = c.getMethod("aggregate",c.getSuperclass());
//								m.invoke(Class.forName((String)function.obj).cast(classifier.obj),Class.forName((String)function.obj).cast(newClassifier));
//								//									System.out.println("In WekaTrainAgg2Updateable add MODEL parent aggregated");
//							}
//							System.out.println("Do Aggregation");
//
//						}
//
//						// reinitialize instancesHolder.obj
//						((weka.core.Instances)instancesHolder.obj).delete();
//						instancesHolder.obj = null;
//
//						System.out.println("In ensemble1 agg in out DONE");
//
//					} else {
//
//						//TODO: Handle the memory for this case
//
//						java.io.File f = new java.io.File(((String[])pathsHolder.obj)[0]);
//
//						int MegaBytes = 1024 * 1024;
//						long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
//						long fileSize = f.length() / MegaBytes;
//						long numSubSamples = fileSize*2/maxMemory;
//
//						System.out.println("In function:  data size: "+(2*fileSize)+ " with "+maxMemory+" Max Memory.");
//
//						if(2*fileSize*2/maxMemory < 2){
//							try{
//
//								System.out.println("Attempting to train using all data of size:"+ ((weka.core.Instances)instancesHolder.obj).size());
//								java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
//
//								m.invoke(Class.forName(((String)function.obj)).cast(classifier.obj),(weka.core.Instances)instancesHolder.obj);
//								System.out.println("Attempt SUCCESS to train using all data of size:"+ ((weka.core.Instances)instancesHolder.obj).size());
//
//							} catch(Exception error){
//								error.printStackTrace();
//							}
//						} else if (numSubSamples == 1) {
//							//If file can fit in memory train 2 models one on each file
//							System.out.println("In 4: Train using all data failed where Instances has: "+((weka.core.Instances)instancesHolder.obj).size()+ " records causing Out of memory error ("+maxMemory+"MB RAM)");
//							System.out.println("Now trying to train from the 2 subpartion files");
//
//
//							//								classifier.obj = this.trainLDAPfile((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj);
//
//
//							System.out.println("In function:  file size: "+fileSize+ " with "+maxMemory+" Max Memory. Num SubSamples = "+numSubSamples);
//
//
//							classifier.obj = new weka.classifiers.meta.Vote();
//
//							for(int i=0;i<2;i++){
//								System.out.println("Using only the 2 files. Reading file"+((String[])pathsHolder.obj)[i]);
//
//								java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(((String[])pathsHolder.obj)[i]));
//
//								((weka.core.Instances)instancesHolder.obj).delete();
//
//								System.out.println("Before Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//								String record = "";
//								int failCount = 0;
//								long startTime = System.currentTimeMillis();
//								while ((record = br.readLine()) != null) {
//									try {
////										((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+record)));
//										((weka.core.Instances)instancesHolder.obj).add(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+record)).get(0));
//									} catch (Exception e) {
//										// TODO Auto-generated catch block
//										//													e.printStackTrace();
//										System.out.println("Failed at this record\n"+record);
//										failCount++;
//									}
//
//								}
//								long endTime = System.currentTimeMillis();
//
//								System.out.println("After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances. loaded in "+((endTime-startTime)/1000)+" sec");
//								System.out.println("Failed to load: "+failCount+" instances");
//
//								startTime = System.currentTimeMillis();
//								try{
//									weka.classifiers.Classifier subClassifier = (weka.classifiers.Classifier) c.newInstance(); 
//
//									java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
//									m.invoke(Class.forName(((String)function.obj)).cast(subClassifier), new Object[] {(String[])optionsHolder.obj});
//
//									System.out.println("Building subClassifier");
//									m = c.getMethod("buildClassifier", weka.core.Instances.class);
//									m.invoke(Class.forName(((String)function.obj)).cast(subClassifier),((weka.core.Instances)instancesHolder.obj));
//
//
//									System.out.println("add subClassifier to voter");
//									((weka.classifiers.meta.Vote)classifier.obj).addPreBuiltClassifier(subClassifier);							
//									System.out.println("subClassifier added ");
//
//								} catch (Exception ex){
//									ex.printStackTrace();
//								}
//
//								endTime = System.currentTimeMillis();
//								
//								System.out.println("subclassifier trained in "+((endTime-startTime)/1000)+" sec");
//								
//								br.close();
//								((java.nio.channels.AsynchronousFileChannel)((java.util.ArrayList<java.nio.channels.AsynchronousFileChannel>)writerHolder.obj).get(i)).close();
//								java.io.File file = new java.io.File(((String[])pathsHolder.obj)[i]);
//								System.out.println("File "+((String[])pathsHolder.obj)[i]+(file.delete()?" Deleted":" Failed to Delete"));
//
//
//							} 
//
//						} else {
//							//else read files, split using ladp to the number of samples and train models
//
//							//combine models using voting
//
//							System.out.println("More partions needed!!!");
//
//						}
//					} 
//
//					System.out.println("In WekaTrainEnsemble1Updateable output rebuilding MODEL updated");
//				} 
//
//
//				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
//
//				weka.core.SerializationHelper.write(os, classifier.obj);
//
//
//				byte[] data = os.toByteArray();
//				tempBuff = tempBuff.reallocIfNeeded(data.length);
//				out.buffer = tempBuff;
//				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
//				out.start=0;
//				out.end=data.length;
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//		}
//
//		@Override
//		public void reset() {
//		}
//
//		/*		public weka.classifiers.Classifier trainLDAPfile(java.util.ArrayList<java.nio.channels.AsynchronousFileChannel> afcList) {
//
//			try {
//				int MegaBytes = 1024 * 1024;
//				long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;
//				long fileSize = afcList.get(0).size() / MegaBytes;
//
//				long numSubSamples = fileSize*2/maxMemory;
//
//				System.out.println("In function:  file size: "+fileSize+ " with "+maxMemory+" Max Memory. Num SubSamples = "+numSubSamples);
//
//
//				System.out.println("Data won't fit in memory. Training a voting ensmble on subpartitions");
//				classifier.obj = new weka.classifiers.meta.Vote();
//
//
//				//If file can fit in memory train 2 models one on each file
//				if(numSubSamples == 1){
//					java.io.File f = new java.io.File(".");
//					java.io.File[] matchingFiles = f.listFiles(new java.io.FilenameFilter() {
//					    public boolean accept(java.io.File dir, String name) {
//					        return name.endsWith("arff");
//					    }
//					});
//
//
//					for(int i=0;i<matchingFiles.length;i++){
//						System.out.println("Using only the 2 files. Reading file"+matchingFiles[i].toPath());
//
//						System.out.println("Before 1 Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//
//						java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(matchingFiles[i]));
//
//						((weka.core.Instances)instancesHolder.obj).delete();
//
//						System.out.println("Before 2 Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//
//						String record;
//					    while ((record = br.readLine()) != null) {
//					    	try {
//								((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+record)));
//							} catch (IOException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
//					    }
//
//					    System.out.println("After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//						try{
//							Class<?> c = Class.forName(((String)function.obj));
//							weka.classifiers.Classifier subClassifier = (weka.classifiers.Classifier) c.newInstance(); 
//
//							java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
//							m.invoke(Class.forName(((String)function.obj)).cast(subClassifier), new Object[] {(String[])optionsHolder.obj});
//
//							System.out.println("Building subClassifier");
//							m = c.getMethod("buildClassifier", weka.core.Instances.class);
//							m.invoke(Class.forName(((String)function.obj)).cast(subClassifier),((weka.core.Instances)instancesHolder.obj));
//
//
//							System.out.println("add subClassifier to voter");
//							((weka.classifiers.meta.Vote)classifier.obj).addPreBuiltClassifier(subClassifier);							
//							System.out.println("subClassifier added ");
//
//						} catch (Exception ex){
//							ex.printStackTrace();
//						}
//
////						java.nio.channels.AsynchronousFileChannel afc = java.nio.channels.AsynchronousFileChannel.open(matchingFiles[i].toPath(), java.nio.file.StandardOpenOption.READ);
////					    ReadHandler handler = new ReadHandler();
////					    FinalReadHandler finalHandler = new FinalReadHandler();
////					    int fSize = (int) afc.size();
////						
////					    long readSoFar = 0; //Math.min(Integer.MAX_VALUE -1 , fSize);
////					    while(readSoFar < fSize){
////					    	
////						    java.nio.ByteBuffer dataBuffer = java.nio.ByteBuffer.allocate((int)Math.min(Integer.MAX_VALUE -1 , (fSize-readSoFar)));
////	
////						    Attachment attach = new Attachment();
////						    attach.asyncChannel = afc;
////						    attach.buffer = dataBuffer;
////						    attach.path = matchingFiles[i].toPath();
////	
////						    if((Integer.MAX_VALUE -1) < (fSize-readSoFar)){
////						    	System.out.println("regular handler");
////						    	afc.read(dataBuffer, 0, attach, handler);
////						    } else {
////						    	System.out.println("last handler");
////						    	afc.read(dataBuffer, 0, attach, finalHandler);
////						    }
////						    
////						    readSoFar += dataBuffer.capacity();
////						    
////						    System.out.println("Read "+ dataBuffer.capacity() + " bytes and remaing "+(fSize-readSoFar)+" bytes");
////						    
////					    }
//
//
//
//
//					}
//
//
//				} else {
//				//else read files, split using ladp to the number of samples and train models
//
//				//combine models using voting
//
//					System.out.println("More partions needed!!!");
//
//				}
//
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//
//			return null;
//		}
//		 */
//
//
//		/*		class Attachment {
//			  public java.nio.file.Path path;
//			  public java.nio.ByteBuffer buffer;
//			  public java.nio.channels.AsynchronousFileChannel asyncChannel;
//		}
//
//		class ReadHandler implements java.nio.channels.CompletionHandler<Integer, Attachment> {
//			  @Override
//			  public void completed(Integer result, Attachment attach) {
////			    System.out.format("%s bytes read   from  %s%n", result, attach.path);
////			    System.out.format("Read data is:%n");
//			    byte[] byteData = attach.buffer.array();
//			    java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");
//			    String data = new String(byteData, cs);
//			    data = data.substring(data.indexOf("\n"), data.lastIndexOf("\n"));
////			    System.out.println(data);
//
//			    System.out.println("Regular Before Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//			    try {
//					((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+data)));
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//
//
//			    System.out.println("Regular After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//			  }
//
//			  @Override
//			  public void failed(Throwable e, Attachment attach) {
//			    System.out.format("Read operation  on  %s  file failed."
//			        + "The  error is: %s%n", attach.path, e.getMessage());
//			    try {
//			      // Close the channel
////			      attach.asyncChannel.close();
//			    } catch (Exception e1) {
//			      e1.printStackTrace();
//			    }
//			  }
//			}
//
//		class FinalReadHandler implements java.nio.channels.CompletionHandler<Integer, Attachment> {
//			  @Override
//			  public void completed(Integer result, Attachment attach) {
////				    System.out.format("%s bytes read   from  %s%n", result, attach.path);
////				    System.out.format("Read data is:%n");
//				    byte[] byteData = attach.buffer.array();
//				    java.nio.charset.Charset cs = java.nio.charset.Charset.forName("UTF-8");
//				    String data = new String(byteData, cs);
//				    data = data.substring(data.indexOf("\n"), data.lastIndexOf("\n"));
////				    System.out.println(data);
//
//				    System.out.println("Final Before Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//				    try {
//						((weka.core.Instances)instancesHolder.obj).addAll(new weka.core.Instances(new java.io.StringReader(((String)arffHeader.obj)+data)));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//
//
//				    System.out.println("Final After Instance has "+ ((weka.core.Instances)instancesHolder.obj).size()+" instances");
//
//
//				    ((weka.core.Instances)instancesHolder.obj).setClassIndex(((weka.core.Instances)instancesHolder.obj).numAttributes() - 1);
//
//
//			    try {
//			      // Close the channel
//			      attach.asyncChannel.close();
//			      System.out.println(attach.path+" is now closed");
//			      //delete file
//			      java.nio.file.Files.delete(attach.path);
//			      System.out.println(attach.path + " is now deleted");
//
//			    } catch (Exception e) {
//			      e.printStackTrace();
//			    }
//			  }
//
//			  @Override
//			  public void failed(Throwable e, Attachment attach) {
//			    System.out.format("Read operation  on  %s  file failed."
//			        + "The  error is: %s%n", attach.path, e.getMessage());
//			    try {
//			      // Close the channel
//			      attach.asyncChannel.close();
//			    } catch (Exception e1) {
//			      e1.printStackTrace();
//			    }
//			  }
//			}
//		 */
//	}


	/**
	 * @author shadi
	 * 
	 * AGGREGATOR FOR
	 * Train model xzy as 
	 * select qdm_ensemble_train_weka('nb','-classes {1,2}', columns) 
	 * from `output100M.csv` as mydata;
	 * 
	 */

	@FunctionTemplate(name = "qdm_ensemble_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainEnsemble2 implements DrillAggFunc{

		@Param  VarCharHolder model;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace ObjectHolder classifierAgg;
		//		@Workspace LinkedListHolder<weka.core.Instances> instancesList;
		@Workspace ObjectHolder function;
		@Workspace IntHolder aggregatable;
		@Workspace BitHolder firstRun;




		public void setup() {
			classifierAgg = new ObjectHolder();
			function = new ObjectHolder();
			aggregatable = new IntHolder();
			//			instancesList = new LinkedListHolder<weka.core.Instances>();
			//			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			//			instancesList.algorithm=null;
			//			instancesList.options = null;
			classifierAgg.obj=null;
			function.obj=null;
			aggregatable.value=-1;
			firstRun.value = 0;


		}

		@Override
		public void add() {
						System.out.println("In WekaTrainEnsemble2");
			byte[] classifierBuf = new byte[model.end - model.start];
			model.buffer.getBytes(model.start, classifierBuf, 0, model.end - model.start);


			String input = new String(classifierBuf, com.google.common.base.Charsets.UTF_8);
			//			System.out.println("In WekaTrainAgg2Updateable add (input): "+input);
			//			System.out.println("In WekaTrainAgg2Updateable add (input legnth): "+input.length()+" - input.contains('|Info|'): "+input.indexOf("|Info|"));

			//			if(input.length()>100 && !input.contains("|Info|") && input.indexOf("weka.classifiers")>-1){
			//			System.out.println("In WekaTrainEnsemble2Updateable add In Model agg");

			if(firstRun.value == 0){
				firstRun.value = 1;
				int i = input.indexOf("weka.classifiers")+18;
				String className = input.substring(input.indexOf("weka.classifiers"),i);
				while("abcdefghijklmnopqrstuvwxyz1234567890.".contains(input.substring(i,i+1).toLowerCase())){
					className = input.substring(input.indexOf("weka.classifiers"),i++);
					//					System.out.println("className: "+className);
				}
				System.out.println("In WekaTrainEnsemble2 "+ className);
				className = input.substring(input.indexOf("weka.classifiers"),i+5);

				while(className.length()>0){
					System.out.println("In WekaTrainEnsemble2 "+ className);
					try{
						Class<?> c = Class.forName(className);
						break;
					} catch(Exception ex){
						className = className.substring(0,className.length()-1);
					}
				}
				function.obj = className;
			}
			//				System.out.println("In WekaTrainAgg2Updateable class name = "+function.value);

			//				System.out.println("In WekaTrainAgg2Updateable add MODEL");
			try{
				java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
				try {
					//						Class.forName(function.value).cast(classifierAgg.classifier);

					Class<?> c = Class.forName((String)function.obj);

					if(aggregatable.value<0){
						Class[] interfaces = c.getInterfaces();
						String interfacesImplemented = "";
						for(int j=0;j<interfaces.length;j++){
							interfacesImplemented+=interfaces[j].getSimpleName()+" - ";
						}	
						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
							interfaces = superClazz.getInterfaces();
							for(int j=0;j<interfaces.length;j++){
								interfacesImplemented+=interfaces[j].getSimpleName()+" - ";
							}	
						}

						if(interfacesImplemented.contains("Aggregateable")){
							aggregatable.value=1;
						} else {
							aggregatable.value=0;
						}
					}

					//						classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);						
					weka.classifiers.Classifier classifier = (weka.classifiers.Classifier) weka.core.SerializationHelper.read(cis);

					//						System.out.println("In WekaTrainAgg2Updateable add MODEL read");

					if(classifierAgg.obj==null){
						System.out.println("In WekaTrainEnsembleAgg2Updateable add MODEL new ");
						if(aggregatable.value==1){
							System.out.println("In WekaTrainEnsembleAgg2Updateable add MODEL new agg");
							classifierAgg.obj = classifier;
						} else if(aggregatable.value==0){
							System.out.println("In WekaTrainEnsembleAgg2Updateable add MODEL new vote ");
							classifierAgg.obj = new weka.classifiers.meta.Vote();
							((weka.classifiers.meta.Vote)classifierAgg.obj).addPreBuiltClassifier(classifier);
						}
						//							System.out.println("In WekaTrainAgg2Updateable add MODEL new  set");
					} else {
						System.out.println("In WekaTrainEnsembleAgg2Updateable add MODEL update");
						// aggregate classifiers
						//							((weka.classifiers.bayes.NaiveBayesUpdateable) classifierAgg.classifier).aggregate((weka.classifiers.bayes.NaiveBayesUpdateable)classifier);


						if(aggregatable.value==1){
							System.out.println("In WekaTrainEnsembleAgg2Updateable add MODEL aggregatable");
							try{
								java.lang.reflect.Method m = c.getMethod("aggregate",c);
								m.invoke(Class.forName((String)function.obj).cast(classifierAgg.obj),Class.forName((String)function.obj).cast(classifier));
								//									System.out.println("In WekaTrainAgg2Updateable add MODEL aggregated");
							} catch (java.lang.NoSuchMethodException ex){
								java.lang.reflect.Method m = c.getMethod("aggregate",c.getSuperclass());
								m.invoke(Class.forName((String)function.obj).cast(classifierAgg.obj),Class.forName((String)function.obj).cast(classifier));
								//									System.out.println("In WekaTrainAgg2Updateable add MODEL parent aggregated");
							}
						} else if (aggregatable.value==0){
							System.out.println("In WekaTrainEnsembleAgg2Updateable add MODEL NOT aggregatable");
							((weka.classifiers.meta.Vote)classifierAgg.obj).addPreBuiltClassifier(classifier);
						}
					}



				} catch (Exception e) {
					e.printStackTrace();
				}
			}catch(Exception e){
				e.printStackTrace();
			}



		}

		@Override
		public void output() {
			try {


				System.out.println("In WekaTrainEnsembleAgg2Updateable out writing agg model");
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifierAgg.obj);
				tempBuff = tempBuff.reallocIfNeeded(os.toByteArray().length);
				out.buffer = tempBuff;
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
				//				}
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
	 * select qdm_Ensemble_weka('vote','-R MAJ', model) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	//	@FunctionTemplate(name = "qdm_AggEnsemble_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	//	public static class WekaTrainEnsembleAgg implements DrillAggFunc{
	//
	//		@Param  VarCharHolder operation;
	//		@Param  VarCharHolder arguments;
	//		@Param  VarCharHolder model;
	//		@Output VarCharHolder out;
	//		@Inject DrillBuf tempBuff;
	//		@Workspace WekaUpdatableClassifierHolder classifierAgg;
	//		@Workspace StringHolder function;
	//		@Workspace IntHolder aggregatable;
	//		@Workspace BitHolder firstRun;
	//
	//
	//
	//
	//		public void setup() {
	//			classifierAgg = new WekaUpdatableClassifierHolder();
	//			function = new StringHolder();
	//			aggregatable = new IntHolder();
	//			classifierAgg.classifier=null;
	//			function.value=null;
	//			aggregatable.value=-1;
	//			firstRun.value = 0;
	//
	//
	//		}
	//
	//		@Override
	//		public void add() {
	//			//			System.out.println("In WekaTrainAgg2Updateable add");
	//
	//
	//
	//
	//			byte[] classifierBuf = new byte[model.end - model.start];
	//			model.buffer.getBytes(model.start, classifierBuf, 0, model.end - model.start);
	//
	//
	//			String input = new String(classifierBuf, com.google.common.base.Charsets.UTF_8);
	//			System.out.println("In WekaTrainAgg2Updateable add In Model agg");
	//
	//			String classType = "numeric";
	//			String [] options = null;
	//
	//			if(firstRun.value == 0){
	//				firstRun.value = 1;
	//
	//				byte[] operationBuf = new byte[operation.end - operation.start];
	//				operation.buffer.getBytes(operation.start, operationBuf, 0, operation.end - operation.start);
	//				function.value = new String(operationBuf, com.google.common.base.Charsets.UTF_8).toLowerCase();
	//
	//				org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
	//				java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
	//						reflections.getSubTypesOf(weka.classifiers.Classifier.class);
	//
	//				java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
	//				boolean done = false;
	//				while(subTypesIterator.hasNext() && !done){
	//					String className = subTypesIterator.next().toString().substring(6);
	//					//					System.out.println(className.substring(className.indexOf("weka")));
	//					try {
	//						Class c = Class.forName(className.substring(className.indexOf("weka")));
	//						if(((String)function.value).equalsIgnoreCase(c.getSimpleName())){
	//							function.value = c.getCanonicalName();
	//							done =true;
	//						}
	//					} catch (ClassNotFoundException e) {
	//						e.printStackTrace();
	//					}
	//
	//				}
	//
	//
	//				byte[] argsBuf = new byte[arguments.end - arguments.start];
	//				arguments.buffer.getBytes(arguments.start, argsBuf, 0, arguments.end - arguments.start);
	//
	//				try {
	//					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
	//					for(int i=0;i<options.length;i++){
	//						if(options[i].indexOf("classes")>0){
	//							classType = options[i+1];
	//							options[i]="";
	//							options[i+1]="";
	//						}
	//					}
	//				} catch (Exception e1) {
	//					e1.printStackTrace();
	//				}
	//
	//
	//			}
	//			try{
	//				java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
	//				try {
	//					Class<?> c = Class.forName(function.value);
	//
	//					weka.classifiers.Classifier classifier = (weka.classifiers.Classifier) weka.core.SerializationHelper.read(cis);
	//
	//					//						System.out.println("In WekaTrainAgg2Updateable add MODEL read");
	//
	//					if(classifierAgg.classifier==null){
	//						System.out.println("In WekaEnsembleAgg2Updateable add MODEL new vote ");
	//
	//						classifierAgg.classifier = (weka.classifiers.Classifier) c.newInstance(); 
	//
	//						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
	//						m.invoke(Class.forName(((String)function.value)).cast(classifierAgg.classifier), new Object[] {options});
	//						
	//					} 
	//
	//
	//					System.out.println("In WekaEnsembleAgg2Updateable add MODEL update");
	//					java.lang.reflect.Method m = c.getMethod("addPreBuiltClassifier", weka.classifiers.Classifier.class);
	//					m.invoke(Class.forName(((String)function.value)).cast(classifierAgg.classifier),classifier);
	//					System.out.println("In WekaEnsembleAgg21Updateable build MODEL done");
	//					
	//
	//				} catch (Exception e) {
	//					e.printStackTrace();
	//				}
	//			}catch(Exception e){
	//				e.printStackTrace();
	//			}
	//
	//
	//
	//		}
	//
	//		@Override
	//		public void output() {
	//			try {
	//
	//				System.out.println("In WekaEnsembleAgg2Updateable out writing agg model");
	//				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
	//				weka.core.SerializationHelper.write(os, classifierAgg.classifier);
	//				tempBuff = tempBuff.reallocIfNeeded(os.toByteArray().length);
	//				out.buffer = tempBuff;
	//				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
	//				out.start=0;
	//				out.end=os.toByteArray().length;
	//			} catch (Exception e) {
	//				e.printStackTrace();
	//			}
	//
	//		}
	//		@Override
	//		public void reset() {
	//		}
	//
	//
	//	}




	/**
	 * @author shadi
	 * 
	 * Train model xzy as 
	 * select qdm_train_weka('nb','-classes {1,2}', mydata.columns[1], mydata.columns[2], mydata.columns[3], mydata.columns[4], mydata.columns[5]) 
	 * from `output100M.csv` as mydata;
	 *
	 */


	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainAgg1UpdateableColumns implements DrillAggFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringHolder arffHeader;
		//		@Workspace LinkedListHolder<weka.core.Instances> instancesList; 
		@Workspace ObjectHolder instancesHolder;
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;
		@Workspace IntHolder updatable;
		@Workspace IntHolder aggregatable;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			instancesHolder = new ObjectHolder();
			instancesHolder.obj = null;
			//			instancesList = new LinkedListHolder<weka.core.Instances>();
			//			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			//			instancesList.algorithm=null;
			//			instancesList.options = null;
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
			currVal = new VarCharHolder();
			updatable = new IntHolder();
			updatable.value=-1;
			aggregatable = new IntHolder();
			aggregatable.value=-1;
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

				org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
				java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
						reflections.getSubTypesOf(weka.classifiers.Classifier.class);

				java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
				boolean done = false;
				while(subTypesIterator.hasNext() && !done){
					String className = subTypesIterator.next().toString().substring(6);
					//					System.out.println(className.substring(className.indexOf("weka")));
					try {
						Class c = Class.forName(className.substring(className.indexOf("weka")));
						if(function.value.equalsIgnoreCase(c.getSimpleName())){
							function.value = c.getCanonicalName();
							done =true;
						}
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}

				}


				try {
					Class<?> c = Class.forName(function.value);

					Class[] interfaces = c.getInterfaces();
					updatable.value = 0;
					aggregatable.value = 0;
					for(int i=0;i<interfaces.length;i++){
						if(interfaces[i].getSimpleName().contains("UpdateableClassifier")){
							updatable.value = 1;
						} else if(interfaces[i].getSimpleName().contains("Aggregateable")){
							aggregatable.value = 1;
						}
					}

					if(updatable.value == 0 || aggregatable.value == 0){
						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
							interfaces = superClazz.getInterfaces();
							for(int j=0;j<interfaces.length;j++){
								if(interfaces[j].getSimpleName().contains("UpdateableClassifier")){
									updatable.value = 1;
								} else if(interfaces[j].getSimpleName().contains("Aggregateable")){
									aggregatable.value = 1;
								}
							}	
						}
					}

				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}


			}

			//Start every run
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));

				instances.setClassIndex(instances.numAttributes() - 1);

				Class<?> c = Class.forName(function.value);

				//				if(updatable.value == 1 && aggregatable.value == 1){
				if(classifier.classifier == null) {
					try{

						//						System.out.println("In WekaTrainAgg1Updateable create MODEL");
						classifier.classifier = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();
						//						classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
						//						System.out.println("In WekaTrainAgg1Updateable options MODEL");
						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
						m.invoke(Class.forName(function.value).cast(classifier.classifier), new Object[] {options});
						//						System.out.println("In WekaTrainAgg1Updateable options MODEL done");

						//						classifier.classifier.buildClassifier(instances);
						//						System.out.println("In WekaTrainAgg1Updateable build MODEL");
						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						m = c.getMethod("buildClassifier", weka.core.Instances.class);
						m.invoke(Class.forName(function.value).cast(classifier.classifier),instances);
						//						System.out.println("In WekaTrainAgg1Updateable build MODEL done");

						if(updatable.value != 1) {
							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {
								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}
						}

					}catch(Exception e){
						e.printStackTrace();
					}
				} else {
					try{

						if(updatable.value == 1) {
							//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));

							//							System.out.println("In WekaTrainAgg1Updateable add MODEL updatable");
							java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
							m.invoke(Class.forName(function.value).cast(classifier.classifier),instances.instance(0));
							//							System.out.println("In WekaTrainAgg1Updateable updatable MODEL updated");
							//							NaiveBayesUpdateable
						} else {
							//							ZeroR

							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {
								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}

							//							System.out.println("In WekaTrainAgg1Updateable rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
							//							java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
							//							m.invoke(Class.forName(function.value).cast(classifier.classifier),((weka.core.Instances)instancesHolder.obj));
							//							System.out.println("In WekaTrainAgg1Updateable rebuilding MODEL updated");
						}

					}catch(Exception ex){
						ex.printStackTrace();

					}
				}
				//				} else {
				//					instancesList.list.add(instances);
				//					instancesList.algorithm = function.value;
				//					instancesList.options = options;
				//				}






				//				if ("ibk".equals(function.value)){
				//					try{
				//						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
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
				//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
				//					}catch(Exception ex){
				//						try{
				//							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
				//							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
				//							classifier.classifier.buildClassifier(instances);
				//						}catch(Exception e){
				//							e.printStackTrace();
				//						}
				//					}
				//				} 

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		@Override
		public void output() {
			try {

				if(instancesHolder.obj != null){
					System.out.println((classifier.classifier != null)+" - "+ function.value+" - In WekaTrainAgg1Updateable output rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
					Class<?> c = Class.forName(function.value);
					java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
					m.invoke(Class.forName(function.value).cast(classifier.classifier),((weka.core.Instances)instancesHolder.obj));
					System.out.println("In WekaTrainAgg1Updateable output rebuilding MODEL updated");

				} 


				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();

				weka.core.SerializationHelper.write(os, classifier.classifier);

				//				if(classifier.classifier!=null && instancesList.list.size() == 0){
				//					weka.core.SerializationHelper.write(os, classifier.classifier);
				//				} else {
				//					//TODO: Handle small datasets. Train model here. 
				//					
				//					weka.core.SerializationHelper.write(os, instancesList);
				//				}
				byte[] data = os.toByteArray();
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(data.length);
				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
				out.start=0;
				out.end=data.length;
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
	 * select qdm_train_weka('nb','-classes {1,2}', mydata.columns) 
	 * from `output100M.csv` as mydata;
	 *
	 */

	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainAgg1Updateable implements DrillAggFunc{

		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param  RepeatedVarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace StringHolder function;
		@Workspace StringHolder arffHeader;
		//		@Workspace LinkedListHolder<weka.core.Instances> instancesList; 
		@Workspace ObjectHolder instancesHolder;
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;
		@Workspace IntHolder updatable;
		@Workspace IntHolder aggregatable;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			instancesHolder = new ObjectHolder();
			instancesHolder.obj = null;
			//			instancesList = new LinkedListHolder<weka.core.Instances>();
			//			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			//			instancesList.algorithm=null;
			//			instancesList.options = null;
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
			currVal = new VarCharHolder();
			updatable = new IntHolder();
			updatable.value=-1;
			aggregatable = new IntHolder();
			aggregatable.value=-1;
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

				org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
				java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
						reflections.getSubTypesOf(weka.classifiers.Classifier.class);

				java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
				boolean done = false;
				while(subTypesIterator.hasNext() && !done){
					String className = subTypesIterator.next().toString().substring(6);
					//					System.out.println(className.substring(className.indexOf("weka")));
					try {
						Class c = Class.forName(className.substring(className.indexOf("weka")));
						if(function.value.equalsIgnoreCase(c.getSimpleName())){
							function.value = c.getCanonicalName();
							done =true;
						}
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}

				}


				try {
					Class<?> c = Class.forName(function.value);

					Class[] interfaces = c.getInterfaces();
					updatable.value = 0;
					aggregatable.value = 0;
					for(int i=0;i<interfaces.length;i++){
						if(interfaces[i].getSimpleName().contains("UpdateableClassifier")){
							updatable.value = 1;
						} else if(interfaces[i].getSimpleName().contains("Aggregateable")){
							aggregatable.value = 1;
						}
					}

					if(updatable.value == 0 || aggregatable.value == 0){
						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
							interfaces = superClazz.getInterfaces();
							for(int j=0;j<interfaces.length;j++){
								if(interfaces[j].getSimpleName().contains("UpdateableClassifier")){
									updatable.value = 1;
								} else if(interfaces[j].getSimpleName().contains("Aggregateable")){
									aggregatable.value = 1;
								}
							}	
						}
					}

				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}


			}

			//Start every run
			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));

				instances.setClassIndex(instances.numAttributes() - 1);

				Class<?> c = Class.forName(function.value);

				//				if(updatable.value == 1 && aggregatable.value == 1){
				if(classifier.classifier == null) {
					try{

						//						System.out.println("In WekaTrainAgg1Updateable create MODEL");
						classifier.classifier = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();
						//						classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
						//						System.out.println("In WekaTrainAgg1Updateable options MODEL");
						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
						m.invoke(Class.forName(function.value).cast(classifier.classifier), new Object[] {options});
						//						System.out.println("In WekaTrainAgg1Updateable options MODEL done");

						//						classifier.classifier.buildClassifier(instances);
						//						System.out.println("In WekaTrainAgg1Updateable build MODEL");
						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						m = c.getMethod("buildClassifier", weka.core.Instances.class);
						m.invoke(Class.forName(function.value).cast(classifier.classifier),instances);
						//						System.out.println("In WekaTrainAgg1Updateable build MODEL done");

						if(updatable.value != 1) {
							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {
								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}
						}

					}catch(Exception e){
						e.printStackTrace();
					}
				} else {
					try{

						if(updatable.value == 1) {
							//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));

							//							System.out.println("In WekaTrainAgg1Updateable add MODEL updatable");
							java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
							m.invoke(Class.forName(function.value).cast(classifier.classifier),instances.instance(0));
							//							System.out.println("In WekaTrainAgg1Updateable updatable MODEL updated");
							//							NaiveBayesUpdateable
						} else {
							//							ZeroR

							if(instancesHolder.obj == null){
								instancesHolder.obj = instances;
							} else {
								((weka.core.Instances)instancesHolder.obj).add(instances.get(0));
							}

							//							System.out.println("In WekaTrainAgg1Updateable rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
							//							java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
							//							m.invoke(Class.forName(function.value).cast(classifier.classifier),((weka.core.Instances)instancesHolder.obj));
							//							System.out.println("In WekaTrainAgg1Updateable rebuilding MODEL updated");
						}

					}catch(Exception ex){
						ex.printStackTrace();

					}
				}
				//				} else {
				//					instancesList.list.add(instances);
				//					instancesList.algorithm = function.value;
				//					instancesList.options = options;
				//				}






				//				if ("ibk".equals(function.value)){
				//					try{
				//						((weka.classifiers.lazy.IBk)classifier.classifier).updateClassifier(instances.instance(0));
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
				//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
				//					}catch(Exception ex){
				//						try{
				//							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
				//							((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
				//							classifier.classifier.buildClassifier(instances);
				//						}catch(Exception e){
				//							e.printStackTrace();
				//						}
				//					}
				//				} 

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		@Override
		public void output() {
			try {

				if(instancesHolder.obj != null){
					System.out.println((classifier.classifier != null)+" - "+ function.value+" - In WekaTrainAgg1Updateable output rebuild MODEL using instances:"+((weka.core.Instances)instancesHolder.obj).numInstances());
					Class<?> c = Class.forName(function.value);
					java.lang.reflect.Method m = c.getMethod("buildClassifier", weka.core.Instances.class);
					m.invoke(Class.forName(function.value).cast(classifier.classifier),((weka.core.Instances)instancesHolder.obj));
					System.out.println("In WekaTrainAgg1Updateable output rebuilding MODEL updated");

				} 


				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();

				weka.core.SerializationHelper.write(os, classifier.classifier);

				//				if(classifier.classifier!=null && instancesList.list.size() == 0){
				//					weka.core.SerializationHelper.write(os, classifier.classifier);
				//				} else {
				//					//TODO: Handle small datasets. Train model here. 
				//					
				//					weka.core.SerializationHelper.write(os, instancesList);
				//				}
				byte[] data = os.toByteArray();
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(data.length);
				out.buffer.setBytes(0, data);//.setBytes(0,outbuff);
				out.start=0;
				out.end=data.length;
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

	@FunctionTemplate(name = "qdm_train_weka", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
	public static class WekaTrainAgg2Updateable implements DrillAggFunc{

		@Param  VarCharHolder model;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifierAgg;
		//		@Workspace LinkedListHolder<weka.core.Instances> instancesList;
		@Workspace StringHolder function;
		@Workspace IntHolder aggregatable;
		@Workspace BitHolder firstRun;




		public void setup() {
			classifierAgg = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			aggregatable = new IntHolder();
			//			instancesList = new LinkedListHolder<weka.core.Instances>();
			//			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			//			instancesList.algorithm=null;
			//			instancesList.options = null;
			classifierAgg.classifier=null;
			function.value=null;
			aggregatable.value=-1;
			firstRun.value = 0;


		}

		@Override
		public void add() {
			//			System.out.println("In WekaTrainAgg2Updateable add");
			byte[] classifierBuf = new byte[model.end - model.start];
			model.buffer.getBytes(model.start, classifierBuf, 0, model.end - model.start);


			String input = new String(classifierBuf, com.google.common.base.Charsets.UTF_8);
			//			System.out.println("In WekaTrainAgg2Updateable add (input): "+input);
			//			System.out.println("In WekaTrainAgg2Updateable add (input legnth): "+input.length()+" - input.contains('|Info|'): "+input.indexOf("|Info|"));

			//			if(input.length()>100 && !input.contains("|Info|") && input.indexOf("weka.classifiers")>-1){
			System.out.println("In WekaTrainAgg2Updateable add In Model agg");

			if(firstRun.value == 0){
				firstRun.value = 1;
				int i = input.indexOf("weka.classifiers")+18;
				String className = input.substring(input.indexOf("weka.classifiers"),i);
				while("abcdefghijklmnopqrstuvwxyz1234567890.".contains(input.substring(i,i+1).toLowerCase())){
					className = input.substring(input.indexOf("weka.classifiers"),i++);
					//					System.out.println("className: "+className);
				}
				className = input.substring(input.indexOf("weka.classifiers"),i+5);
				while(className.length()>0){
					System.out.println("In WekaTrainAgg2Updateable "+ className);
					try{
						Class<?> c = Class.forName(className);
						break;
					} catch(Exception ex){
						className = className.substring(0,className.length()-1);
					}
				}
					
				function.value = className;
			}
			//				System.out.println("In WekaTrainAgg2Updateable class name = "+function.value);

			//				System.out.println("In WekaTrainAgg2Updateable add MODEL");
			try{
				java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
				try {
					//						Class.forName(function.value).cast(classifierAgg.classifier);

					Class<?> c = Class.forName(function.value);

					if(aggregatable.value<0){
						Class[] interfaces = c.getInterfaces();
						String interfacesImplemented = "";
						for(int j=0;j<interfaces.length;j++){
							interfacesImplemented+=interfaces[j].getSimpleName()+" - ";
						}	
						for(Class superClazz = c.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
							interfaces = superClazz.getInterfaces();
							for(int j=0;j<interfaces.length;j++){
								interfacesImplemented+=interfaces[j].getSimpleName()+" - ";
							}	
						}

						if(interfacesImplemented.contains("Aggregateable")){
							aggregatable.value=1;
						} else {
							aggregatable.value=0;
						}
					}

					//						classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);						
					weka.classifiers.Classifier classifier = (weka.classifiers.Classifier) weka.core.SerializationHelper.read(cis);

					//						System.out.println("In WekaTrainAgg2Updateable add MODEL read");

					if(classifierAgg.classifier==null){
						System.out.println("In WekaTrainAgg2Updateable add MODEL new ");
						if(aggregatable.value==1){
							System.out.println("In WekaTrainAgg2Updateable add MODEL new agg");
							classifierAgg.classifier = classifier;
						} else if(aggregatable.value==0){
							System.out.println("In WekaTrainAgg2Updateable add MODEL new vote ");
							classifierAgg.classifier = new weka.classifiers.meta.Vote();
							((weka.classifiers.meta.Vote)classifierAgg.classifier).addPreBuiltClassifier(classifier);
						}
						//							System.out.println("In WekaTrainAgg2Updateable add MODEL new  set");
					} else {
						System.out.println("In WekaTrainAgg2Updateable add MODEL update");
						// aggregate classifiers
						//							((weka.classifiers.bayes.NaiveBayesUpdateable) classifierAgg.classifier).aggregate((weka.classifiers.bayes.NaiveBayesUpdateable)classifier);


						if(aggregatable.value==1){
							System.out.println("In WekaTrainAgg2Updateable add MODEL aggregatable");
							try{
								java.lang.reflect.Method m = c.getMethod("aggregate",c);
								m.invoke(Class.forName(function.value).cast(classifierAgg.classifier),Class.forName(function.value).cast(classifier));
								//									System.out.println("In WekaTrainAgg2Updateable add MODEL aggregated");
							} catch (java.lang.NoSuchMethodException ex){
								java.lang.reflect.Method m = c.getMethod("aggregate",c.getSuperclass());
								m.invoke(Class.forName(function.value).cast(classifierAgg.classifier),Class.forName(function.value).cast(classifier));
								//									System.out.println("In WekaTrainAgg2Updateable add MODEL parent aggregated");
							}
						} else if (aggregatable.value==0){
							System.out.println("In WekaTrainAgg2Updateable add MODEL NOT aggregatable");
							((weka.classifiers.meta.Vote)classifierAgg.classifier).addPreBuiltClassifier(classifier);
						}
					}

					//						if ("ibk".equals(function.value)){
					//						classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
					//						if(classifierAgg.classifier==null){
					//							classifierAgg.classifier = classifier;
					//						} else {
					//							// aggregate classifiers
					//							((weka.classifiers.lazy.IBk) classifierAgg.classifier).
					//						}
					//						} else if ("nb".equals(function.value)){
					//							classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
					//							if(classifierAgg.classifier==null){
					//								classifierAgg.classifier = classifier;
					//							} else {
					//								// aggregate classifiers
					//								((weka.classifiers.bayes.NaiveBayesUpdateable) classifierAgg.classifier).aggregate((weka.classifiers.bayes.NaiveBayesUpdateable)classifier);
					//							}
					//						}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}catch(Exception e){
				e.printStackTrace();
			}

			//			} else {
			//				System.out.println("deserializing the instances list");
			////
			//				try{
			//					java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
			//
			//					if(instancesList.list.size() == 0) {
			//						System.out.println("deserializing the instances list NEW");
			//						instancesList = (LinkedListHolder<weka.core.Instances>) weka.core.SerializationHelper.read(cis);
			//					} else {
			//						System.out.println("deserializing the instances list ADD");
			//						instancesList.list.addAll(((LinkedListHolder<weka.core.Instances>) weka.core.SerializationHelper.read(cis)).list);
			//					}
			//
			//					System.out.println("deserializing the instances list COMPLETE");
			//
			//					System.out.println("Algorithm deserialized is: "+instancesList.algorithm+" - instances size = "+instancesList.list.size());
			//
			//
			//
			//				}catch(Exception e){
			//					//					e.printStackTrace();
			//					if(input.contains("|Info|")){
			//						function.value = input.substring(0,input.indexOf("|Info|")-1);
			//						System.out.println("Info function name = "+function.value);
			//					} else{
			//						function.value = input;
			//						System.out.println("function name = "+function.value);
			//						//						info.value = input.substring(input.indexOf("||")+2);
			//						//						System.out.println(info.value);
			//						//						classifierBuf =  info.value.getBytes();
			//					}
			//				}
			//			} 


		}

		@Override
		public void output() {
			try {

				//				System.out.println("instancesList.list.size() = "+instancesList.list.size());
				//				if(classifierAgg.classifier==null && instancesList.list.size() == 0){
				//					//					System.out.println("In WekaTrainSupportedArgs output");
				//
				//					String helpText = "";
				//
				//					org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
				//					java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
				//							reflections.getSubTypesOf(weka.classifiers.Classifier.class);
				//
				//					java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
				//					boolean done = false;
				//					while(subTypesIterator.hasNext() && !done){
				//						String className = subTypesIterator.next().toString().substring(6);
				//						//						System.out.println(className.substring(className.indexOf("weka")));
				//						Class c = Class.forName(className.substring(className.indexOf("weka")));
				//						try{
				//							Object t = c.newInstance();
				//							Class clazz = Class.forName(c.getCanonicalName());
				//							Class[] interfaces = clazz.getInterfaces();
				//							String interfacesImplemented = "";
				//							for(int i=0;i<interfaces.length;i++){
				//								interfacesImplemented+=interfaces[i].getSimpleName()+" - ";
				//							}	
				//							for(Class superClazz = clazz.getSuperclass(); superClazz!=null; superClazz = superClazz.getSuperclass()){
				//								interfaces = superClazz.getInterfaces();
				//								for(int i=0;i<interfaces.length;i++){
				//									interfacesImplemented+=interfaces[i].getSimpleName()+" - ";
				//								}	
				//							}
				//							helpText += "qdm_weka_train(\'"+c.getSimpleName()+"\',arguments,comma-separated features (label is the last column))\n\r";
				//							helpText += "Type qdm_weka_train(\'"+c.getSimpleName()+"\') for help\n\r";
				//							if(interfacesImplemented.contains("Aggregateable"))
				//								helpText +="Aggregateable"+"\n\r";
				//							if(interfacesImplemented.contains("UpdateableClassifier"))
				//								helpText +="Updateable"+"\n\r";
				//							helpText+="---------------------------------------------------------------------------\n\r";
				//							if(function.value.equalsIgnoreCase(c.getSimpleName())){
				//								helpText = "qdm_weka_train(\'"+c.getSimpleName()+"\',arguments,comma-separated features (label is the last column))\n\r";
				//								if(interfacesImplemented.contains("Aggregateable"))
				//									helpText +="Aggregateable"+"\n\r";
				//								if(interfacesImplemented.contains("UpdateableClassifier"))
				//									helpText +="Updateable"+"\n\r";
				//								helpText+="---------------------------------------------------------------------------\n\r";
				//
				//								try{
				//									java.lang.reflect.Method m = c.getMethod("globalInfo");
				//									helpText+=":"+m.invoke(t)+"\n\r";
				//									m = c.getMethod("listOptions");
				//									java.util.Enumeration<weka.core.Option> e = (java.util.Enumeration<weka.core.Option>)m.invoke(t);
				//
				//									while(e.hasMoreElements()){
				//										weka.core.Option tmp = ((weka.core.Option)e.nextElement());
				//										helpText+=tmp.name()+" : "+tmp.description()+"\n\r";
				//									}
				//
				//									helpText+="-classes {c1,c2,c3}"+" : "+"List possible classes for the dataset. If not specified class becomes NUMERIC"+"\n\r"+"\n\r"+"\n\r";
				//									done = true;
				//								} catch (Exception e){
				//									e.printStackTrace();
				//								}
				//
				//							}
				//						} catch (Exception e){
				//
				//						}
				//					}
				//
				//					if(helpText.length() == 0){
				//						helpText+=":"+"No Args";
				//					}
				//					helpText = function.value+" |Info|:"+helpText;
				//					out.buffer = tempBuff;
				//					out.buffer = out.buffer.reallocIfNeeded(helpText.length()+100);
				//					out.buffer.setBytes(0,helpText.getBytes(com.google.common.base.Charsets.UTF_8));
				//					out.start=0;
				//					out.end=helpText.length();
				//
				//				} else if (instancesList.list.size()>0) {
				//					function.value = instancesList.algorithm;
				//					System.out.println("In WekaTrainAgg2Updateable output list size:"+instancesList.list.size());
				//					Class<?> c = Class.forName(function.value);
				//
				//					try{
				//
				//						System.out.println("In WekaTrainAgg2Updateable output create MODEL");
				//						classifierAgg.classifier = (weka.classifiers.Classifier) c.newInstance(); //new weka.classifiers.bayes.NaiveBayesUpdateable();
				//						System.out.println("In WekaTrainAgg1Updateable out options MODEL");
				//						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
				//						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
				//						m.invoke(Class.forName(function.value).cast(classifierAgg.classifier), new Object[] {instancesList.options});
				//						System.out.println("In WekaTrainAgg2Updateable out options MODEL done");
				//
				//						//						classifier.classifier.buildClassifier(instances);
				//						System.out.println("In WekaTrainAgg2Updateable out build MODEL");
				//						//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
				//
				//						weka.core.Instances instances = new weka.core.Instances((weka.core.Instances)instancesList.list.get(0), instancesList.list.size());
				//
				//						for(int i=1; i<instancesList.list.size();i++){
				//							instances.addAll((weka.core.Instances)instancesList.list.get(i));
				//						}
				//
				//
				//						instances.setClassIndex(instances.numAttributes() - 1);
				//
				//						m = c.getMethod("buildClassifier", weka.core.Instances.class);
				//
				//						m.invoke(Class.forName(function.value).cast(classifierAgg.classifier),instances);
				//						System.out.println("In WekaTrainAgg2Updateable out build MODEL done");
				//
				//					}catch(Exception e){
				//						e.printStackTrace();
				//					}
				//
				//
				//					java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				//					weka.core.SerializationHelper.write(os, classifierAgg.classifier);
				//					out.buffer = tempBuff;
				//					out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				//					out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				//					out.start=0;
				//					out.end=os.toByteArray().length;
				//
				//				} else {
				System.out.println("In WekaTrainAgg2Updateable out writing agg model");
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				weka.core.SerializationHelper.write(os, classifierAgg.classifier);
				out.buffer = tempBuff;
				out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
				out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
				out.start=0;
				out.end=os.toByteArray().length;
				//				}
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
	 * select qdm_score_weka('nb','-classes {1,2}', mymodel.columns[0], mydata.columns[0],....) 
	 * from `output100M.csv` as mydata applying nb100M_3 as mymodel;
	 *
	 */

	@FunctionTemplate(name = "qdm_score_weka", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
	public static class WekaScoreUpdateable implements DrillSimpleFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param 	NullableVarCharHolder classifierTxt;
		@Param  VarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace String function;
		@Workspace String[] classes;
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
						classifier.classifier = (weka.classifiers.Classifier) weka.core.SerializationHelper.read(cis);
						//						if ("ibk".equals(function)){
						//							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
						//						} else if ("nb".equals(function)){
						//							classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
						//						}
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
				String classType = "numeric";
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){
						if(options[i].indexOf("classes")>0){
							classType = options[i+1];
							classes = options[i+1].substring(1, options[i+1].length()-1).split(",");
							options[i]="";
							options[i+1]="";
						}
					}

				} catch (Exception e1) {
					e1.printStackTrace();
				}
				//				arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}

			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData+",0"));
				instances.setClassIndex(instances.numAttributes() - 1);
				String output="";
				double[] predictions = classifier.classifier.distributionForInstance(instances.instance(0));
				if(predictions.length==1){
					if(classes!=null){
						output=classes[(int)predictions[0]];
					} else {
						output = ""+predictions[0];
					}
				} else {
					//					java.util.List b = java.util.Arrays.asList(org.apache.commons.lang.ArrayUtils.toObject(predictions));
					//					double max = -1;
					//					for(int i=0;i<predictions.length;i++){
					//						if(predictions[i]>max){
					//							max=predictions[i];
					//							output=""+ (i+1);							
					//						}
					//					}
					if(classes!=null){
						//						output= classes[b.indexOf(java.util.Collections.max(b))];
						double max = -1;
						for(int i=0;i<predictions.length;i++){
							if(predictions[i]>max){
								max=predictions[i];
								output=classes[i];
							}
						}
					}else{
						//						output= ""+(b.indexOf(java.util.Collections.max(b))+1);
						double max = -1;
						for(int i=0;i<predictions.length;i++){
							if(predictions[i]>max){
								max=predictions[i];
								output=""+ (i+1);
							}
						}
					}
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
	 * select qdm_score_weka('nb','-classes {1,2}', mymodel.columns[0], mydata.columns) 
	 * from `output100M.csv` as mydata applying nb100M_3 as mymodel;
	 *
	 */

	@FunctionTemplate(name = "qdm_score_weka", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
	public static class WekaScoreRepeatedUpdateable implements DrillSimpleFunc{
		@Param  VarCharHolder operation;
		@Param  VarCharHolder arguments;
		@Param 	NullableVarCharHolder classifierTxt;
		@Param  RepeatedVarCharHolder features;
		@Output VarCharHolder out;
		@Inject DrillBuf tempBuff;
		@Workspace WekaUpdatableClassifierHolder classifier;
		@Workspace String function;
		@Workspace String[] classes;
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
					
					
					//////////////////////////////////////////////////////////////////////////////////////////
					String inputPath = new String(classifierBuf, com.google.common.base.Charsets.UTF_8);
//					
					System.out.println(inputPath);
					
//					org.apache.hadoop.fs.Path hadoopPath;
//					java.io.File file;
//					java.io.FileInputStream fin;
					
//					inputPath = "hdfs:/ec2-54-164-156-232.compute-1.amazonaws.com:8020/tmp/htHIGGS_unae10/0_0_0.model";
					
//					try{
//                        Path pt=new Path("hdfs://npvm11.np.wc1.yellowpages.com:9000/user/john/abc.txt");
//                        FileSystem fs = FileSystem.get(new Configuration());
//                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
//                        String line;
//                        line=br.readLine();
//                        while (line != null){
//                                System.out.println(line);
//                                line=br.readLine();
//                        }
//                }catch(Exception e){
//                }
					
//					org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
//					org.apache.hadoop.fs.FileSystem fileSystem = null;
//					if (inputPath.startsWith("hdfs:/")) {
//					  fileSystem = org.apache.hadoop.fs.FileSystem.get(configuration);
//					} else if (inputPath.startsWith("file:/")) {
//					  fileSystem = FileSystem.getLocal(configuration).getRawFileSystem();
//					}
//					
					org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//					    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
//					    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));

				

						org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(inputPath);
						System.out.println("Path = "+path.toUri());
					    org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
					    System.out.println("Got FS = "+fs.getScheme());
					    org.apache.hadoop.fs.FSDataInputStream inputStream = fs.open(path);
					    System.out.println("Got file of size: "+inputStream.available());
					    
//					    byte[] outbuff = new byte[inputStream.available()];
//					    byte[] tmpbuff = new byte[inputStream.available()];
					    
					    
					    java.io.ByteArrayOutputStream bo = new java.io.ByteArrayOutputStream();
						byte[] b = new byte[1];
						while(inputStream.read(b)!=-1){
							bo.write(b);
						}

						byte[] outbuff = bo.toByteArray();
//					    while(inputStream.read(tmpbuff, 0, tmpbuff.length) > 0){
//					    	
//					    }
						
					    System.out.println("Data Loaded = "+outbuff.length);
//					    fs.close();
/*					
 
					try{
//						hadoopPath = new org.apache.hadoop.fs.Path(inputPath);
						file = new java.io.File(inputPath);
						fin = new java.io.FileInputStream(file);
					} catch (java.io.FileNotFoundException e) {
//						e.printStackTrace();
						System.out.println("Not found: "+inputPath);
						try{
							inputPath = inputPath.replace("file", "C");
							System.out.println(inputPath);
//							hadoopPath = new org.apache.hadoop.fs.Path(inputPath);
							file = new java.io.File(inputPath);
							fin = new java.io.FileInputStream(file);
						} catch (java.io.FileNotFoundException ex) {
//							ex.printStackTrace();
							System.out.println("Not found: "+inputPath);
							inputPath = inputPath.replace("C:", "~");
							System.out.println(inputPath);
//							hadoopPath = new org.apache.hadoop.fs.Path(inputPath);
							file = new java.io.File(inputPath);
							fin = new java.io.FileInputStream(file);
						}
					}
					
//					
//					java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(inputPath));
//					java.io.File file = new java.io.File(inputPath);
					
//					
					byte fileContent[] = new byte[(int)file.length()];
		            fin.read(fileContent);
		            fin.close();

*/
					    
//		            String s = new String(fileContent);
//		            System.out.println("File content: " + s);
//
//					java.io.InputStream freader = new java.io.ByteArrayInputStream(classifierBuf);
//					java.io.InputStream fis = (java.io.InputStream) weka.core.SerializationHelper.read(freader);
//					
//					java.io.BufferedInputStream stream = new java.io.BufferedInputStream(fis); 
//
//						byte[] outbuff = new byte[stream.available()];
//						stream.read(outbuff, 0, outbuff.length);
//								
//						stream.close();


						
					////////////////////////////////////////////////////////////////////////////////////////
						
						
					java.io.InputStream cis = new java.io.ByteArrayInputStream(outbuff);
					try {
						classifier.classifier = (weka.classifiers.Classifier) weka.core.SerializationHelper.read(cis);
						System.out.println("Model LOADED");
						//						if ("ibk".equals(function)){
						//							classifier.classifier = (weka.classifiers.lazy.IBk) weka.core.SerializationHelper.read(cis);
						//						} else if ("nb".equals(function)){
						//							classifier.classifier = (weka.classifiers.bayes.NaiveBayesUpdateable) weka.core.SerializationHelper.read(cis);
						//						}
//						System.out.println("Classifier Loaded First Run "+((weka.classifiers.Classifier)classifier.classifier).toString());
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
				String classType = "numeric";
				try {
					options = weka.core.Utils.splitOptions((new String(argsBuf, com.google.common.base.Charsets.UTF_8)));
					for(int i=0;i<options.length;i++){				
						if(options[i].indexOf("classes")>0){
							classType = options[i+1];
							classes = options[i+1].substring(1, options[i+1].length()-1).split(",");
							options[i]="";
							options[i+1]="";
						}
					}

				} catch (Exception e1) {
					e1.printStackTrace();
				}
				//				arffHeader+="@"+"ATTRIBUTE class "+classType+"\n";
				stBuilder.append("@"+"ATTRIBUTE class "+classType+"\n");
				stBuilder.append("@"+"DATA\n");
				arffHeader.value = stBuilder.toString();
			}

			try {
				weka.core.Instances instances = new weka.core.Instances(new java.io.StringReader(arffHeader.value+rowData));
//				System.out.println("Classifier NEXT Run "+(classifier.classifier == null?"NULL":"Loaded"));
				instances.setClassIndex(instances.numAttributes() - 1);
				String output="";
				double[] predictions = classifier.classifier.distributionForInstance(instances.instance(0));
				if(predictions.length==1){
					if(classes!=null){
						output=classes[(int)predictions[0]];
					} else {
						output = ""+predictions[0];
					}
				} else {
					//					java.util.List b = java.util.Arrays.asList(org.apache.commons.lang.ArrayUtils.toObject(predictions));
					//					double max = -1;
					//					for(int i=0;i<predictions.length;i++){
					//						if(predictions[i]>max){
					//							max=predictions[i];
					//							output=""+ (i+1);							
					//						}
					//					}
					if(classes!=null){
						//						output= classes[b.indexOf(java.util.Collections.max(b))];
						double max = -1;
						for(int i=0;i<predictions.length;i++){
							if(predictions[i]>max){
								max=predictions[i];
								output=classes[i];
							}
						}
					}else{
						//						output= ""+(b.indexOf(java.util.Collections.max(b))+1);
						double max = -1;
						for(int i=0;i<predictions.length;i++){
							if(predictions[i]>max){
								max=predictions[i];
								output=""+ (i+1);
							}
						}
					}
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

