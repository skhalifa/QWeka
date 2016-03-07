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
import org.apache.drill.exec.expr.holders.RepeatedVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;


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
				System.out.println("In WekaTrainSupportedArgs output");
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
		@Workspace LinkedListHolder<weka.core.Instances> instancesList; 
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;
		@Workspace IntHolder updatable;
		@Workspace IntHolder aggregatable;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			instancesList = new LinkedListHolder<weka.core.Instances>();
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			instancesList.algorithm=null;
			instancesList.options = null;
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
				
				if(updatable.value == 1 && aggregatable.value == 1){
					
					try{

	//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
						
//							System.out.println("In WekaTrainAgg1Updateable add MODEL updatable");
							java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
							m.invoke(Class.forName(function.value).cast(classifier.classifier),instances.instance(0));
//							System.out.println("In WekaTrainAgg1Updateable add MODEL updated");
					
			
					}catch(Exception ex){
//						ex.printStackTrace();
						try{
							
//							System.out.println("In WekaTrainAgg1Updateable create MODEL");
							classifier.classifier = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();
//							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
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
							System.out.println("In WekaTrainAgg1Updateable build MODEL done");
							
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else {
					instancesList.list.add(instances);
					instancesList.algorithm = function.value;
					instancesList.options = options;
				}
				
				
				
				
				
				
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
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				if(classifier.classifier!=null && instancesList.list.size() == 0){
					weka.core.SerializationHelper.write(os, classifier.classifier);
				} else {
					weka.core.SerializationHelper.write(os, instancesList);
				}
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
	 * select qdm_train_weka('nb','-classes {1,2}', columns) 
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
		@Workspace LinkedListHolder<weka.core.Instances> instancesList; 
		@Workspace  BitHolder firstRun;
		@Workspace VarCharHolder currVal;
		@Workspace IntHolder updatable;
		@Workspace IntHolder aggregatable;

		public void setup() {
			classifier = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			arffHeader = new StringHolder();
			firstRun = new BitHolder();
			instancesList = new LinkedListHolder<weka.core.Instances>();
			classifier.classifier=null;
			function.value=null;
			arffHeader.value=null;
			firstRun.value=0;
			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			instancesList.algorithm=null;
			instancesList.options = null;
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
				
				if(updatable.value == 1 && aggregatable.value == 1){
					
					try{

	//					((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).updateClassifier(instances.instance(0));
						
//							System.out.println("In WekaTrainAgg1Updateable add MODEL updatable");
							java.lang.reflect.Method m = c.getMethod("updateClassifier", weka.core.Instance.class);
							m.invoke(Class.forName(function.value).cast(classifier.classifier),instances.instance(0));
	//						System.out.println("In WekaTrainAgg1Updateable add MODEL updated");
					
			
					}catch(Exception ex){
	//					ex.printStackTrace();
						try{
							
	//						System.out.println("In WekaTrainAgg1Updateable create MODEL");
							classifier.classifier = (weka.classifiers.Classifier) c.newInstance(); // new weka.classifiers.bayes.NaiveBayesUpdateable();
//							classifier.classifier = new weka.classifiers.bayes.NaiveBayesUpdateable();
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
							System.out.println("In WekaTrainAgg1Updateable build MODEL done");
							
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				} else {
					instancesList.list.add(instances);
					instancesList.algorithm = function.value;
					instancesList.options = options;
				}
				
				
				
				
				
				
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
				java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
				if(classifier.classifier!=null && instancesList.list.size() == 0){
					weka.core.SerializationHelper.write(os, classifier.classifier);
				} else {
					weka.core.SerializationHelper.write(os, instancesList);
				}
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
		@Workspace LinkedListHolder<weka.core.Instances> instancesList;
		@Workspace StringHolder function;
		@Workspace IntHolder aggregatable;
		@Workspace BitHolder firstRun;

		


		public void setup() {
			classifierAgg = new WekaUpdatableClassifierHolder();
			function = new StringHolder();
			aggregatable = new IntHolder();
			instancesList = new LinkedListHolder<weka.core.Instances>();
			instancesList.list = new java.util.LinkedList<weka.core.Instances>();
			instancesList.algorithm=null;
			instancesList.options = null;
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

			if(input.length()>100 && !input.contains("|Info|") && input.indexOf("weka.classifiers")>-1){
				System.out.println("In WekaTrainAgg2Updateable add In Model agg");
				
				if(firstRun.value == 0){
					firstRun.value = 1;
					int i = input.indexOf("weka.classifiers")+18;
					String className = input.substring(input.indexOf("weka.classifiers"),i);
					while("abcdefghijklmnopqrstuvwxyz1234567890.".contains(input.substring(i,i+1).toLowerCase())){
						className = input.substring(input.indexOf("weka.classifiers"),i++);
	//					System.out.println("className: "+className);
					}
					className = input.substring(input.indexOf("weka.classifiers"),i--);
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
//								classifierAgg.classifier = new weka.classifiers.meta.Vote();
//								((weka.classifiers.meta.Vote)classifierAgg.classifier).addPreBuiltClassifier(classifier);
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
//								((weka.classifiers.meta.Vote)classifierAgg.classifier).addPreBuiltClassifier(classifier);
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
			
			} else {
				System.out.println("deserializing the instances list");

				try{
					java.io.InputStream cis = new java.io.ByteArrayInputStream(classifierBuf);
					
					if(instancesList.list.size() == 0) {
						System.out.println("deserializing the instances list NEW");
						instancesList = (LinkedListHolder<weka.core.Instances>) weka.core.SerializationHelper.read(cis);
					} else {
						System.out.println("deserializing the instances list ADD");
						instancesList.list.addAll(((LinkedListHolder<weka.core.Instances>) weka.core.SerializationHelper.read(cis)).list);
					}
					
					System.out.println("deserializing the instances list COMPLETE");

					System.out.println("Algorithm deserialized is: "+instancesList.algorithm+" - instances size = "+instancesList.list.size());
					

						
				}catch(Exception e){
//					e.printStackTrace();
					if(input.contains("|Info|")){
						function.value = input.substring(0,input.indexOf("|Info|")-1);
						System.out.println("Info function name = "+function.value);
					} else{
						function.value = input;
						System.out.println("function name = "+function.value);
//						info.value = input.substring(input.indexOf("||")+2);
//						System.out.println(info.value);
//						classifierBuf =  info.value.getBytes();
					}
				}
			} 
			

		}
		
		@Override
		public void output() {
			try {
				
				System.out.println("instancesList.list.size() = "+instancesList.list.size());
				if(classifierAgg.classifier==null && instancesList.list.size() == 0){
//					System.out.println("In WekaTrainSupportedArgs output");

					String helpText = "";
					
					org.reflections.Reflections reflections = new org.reflections.Reflections("weka.classifiers"); 
					java.util.Set<Class<? extends weka.classifiers.Classifier>> subTypes = 
					           reflections.getSubTypesOf(weka.classifiers.Classifier.class);
					
					java.util.Iterator<Class<? extends weka.classifiers.Classifier>> subTypesIterator = subTypes.iterator();
					boolean done = false;
					while(subTypesIterator.hasNext() && !done){
						String className = subTypesIterator.next().toString().substring(6);
//						System.out.println(className.substring(className.indexOf("weka")));
						Class c = Class.forName(className.substring(className.indexOf("weka")));
						try{
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
							if(function.value.equalsIgnoreCase(c.getSimpleName())){
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
					helpText = function.value+" |Info|:"+helpText;
					out.buffer = tempBuff;
					out.buffer = out.buffer.reallocIfNeeded(helpText.length()+100);
					out.buffer.setBytes(0,helpText.getBytes(com.google.common.base.Charsets.UTF_8));
					out.start=0;
					out.end=helpText.length();
					
				} else if (instancesList.list.size()>0) {
					function.value = instancesList.algorithm;
					System.out.println("In WekaTrainAgg2Updateable output list size:"+instancesList.list.size());
					Class<?> c = Class.forName(function.value);
					
					try{
						
						System.out.println("In WekaTrainAgg2Updateable output create MODEL");
						classifierAgg.classifier = (weka.classifiers.Classifier) c.newInstance(); //new weka.classifiers.bayes.NaiveBayesUpdateable();
						System.out.println("In WekaTrainAgg1Updateable out options MODEL");
//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						java.lang.reflect.Method m = c.getMethod("setOptions", String[].class);
						m.invoke(Class.forName(function.value).cast(classifierAgg.classifier), new Object[] {instancesList.options});
						System.out.println("In WekaTrainAgg2Updateable out options MODEL done");
						
//						classifier.classifier.buildClassifier(instances);
						System.out.println("In WekaTrainAgg2Updateable out build MODEL");
//						((weka.classifiers.bayes.NaiveBayesUpdateable)classifier.classifier).setOptions(options);
						
						weka.core.Instances instances = new weka.core.Instances((weka.core.Instances)instancesList.list.get(0), instancesList.list.size());

						for(int i=1; i<instancesList.list.size();i++){
							instances.addAll((weka.core.Instances)instancesList.list.get(i));
						}
						

						instances.setClassIndex(instances.numAttributes() - 1);
						
						m = c.getMethod("buildClassifier", weka.core.Instances.class);
						
						m.invoke(Class.forName(function.value).cast(classifierAgg.classifier),instances);
						System.out.println("In WekaTrainAgg2Updateable out build MODEL done");
						
					}catch(Exception e){
						e.printStackTrace();
					}
					
			
					java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
					weka.core.SerializationHelper.write(os, classifierAgg.classifier);
					out.buffer = tempBuff;
					out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
					out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
					out.start=0;
					out.end=os.toByteArray().length;

				} else {
					System.out.println("In WekaTrainAgg2Updateable out writing agg model");
					java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
					weka.core.SerializationHelper.write(os, classifierAgg.classifier);
					out.buffer = tempBuff;
					out.buffer = out.buffer.reallocIfNeeded(os.toByteArray().length);
					out.buffer.setBytes(0, os.toByteArray());//.setBytes(0,outbuff);
					out.start=0;
					out.end=os.toByteArray().length;
				}
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

