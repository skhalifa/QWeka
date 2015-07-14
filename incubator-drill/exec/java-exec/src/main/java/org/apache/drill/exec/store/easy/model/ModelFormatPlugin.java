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
package org.apache.drill.exec.store.easy.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.store.model.DrillModelReader;
import org.apache.drill.exec.store.model.DrillModelWriter;
import org.apache.drill.exec.store.text.DrillTextRecordReader;
import org.apache.drill.exec.store.text.DrillTextRecordWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class ModelFormatPlugin extends EasyFormatPlugin<ModelFormatPlugin.ModelFormatConfig> {


  public ModelFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig storageConfig) {
    super(name, context, fs, storageConfig, new ModelFormatConfig(), true, false, false, false, new ArrayList<String>(), "model");
  }

  public ModelFormatPlugin(String name, DrillbitContext context, DrillFileSystem fs, StoragePluginConfig config, ModelFormatConfig formatPluginConfig) {
    super(name, context, fs, config, formatPluginConfig, true, false, false, false, formatPluginConfig.getExtensions(), "model");
  }


  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
      List<SchemaPath> columns) throws ExecutionSetupException {
//    Path path = dfs.makeQualified(new Path(fileWork.getPath()));
//    FileSplit split = new FileSplit(path, fileWork.getStart(), fileWork.getLength(), new String[]{""});
//    Preconditions.checkArgument(((ModelFormatConfig)formatConfig).getDelimiter().length() == 1, "Only single character delimiter supported");
//    return new DrillModelReader(split, context, ((ModelFormatConfig) formatConfig).getDelimiter().charAt(0), columns);
//    return new DrillModelReader(split, context, columns);
    return new DrillModelReader(context, fileWork.getPath(), dfs, columns);
  }

 
  
  @Override
  public AbstractGroupScan getGroupScan(FileSelection selection, List<SchemaPath> columns) throws IOException {
    return new EasyGroupScan(selection, this, columns, selection.selectionRoot); //TODO : textformat supports project?
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    Map<String, String> options = Maps.newHashMap();

    options.put("location", writer.getLocation());

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("separator", ((ModelFormatConfig)getConfig()).getDelimiter());
    options.put(FileSystem.FS_DEFAULT_NAME_KEY, ((FileSystemConfig)writer.getStorageConfig()).connection);

    options.put("extension", ((ModelFormatConfig)getConfig()).getExtensions().get(0));

    RecordWriter recordWriter = new DrillModelWriter(/*context.getAllocator()*/);
    recordWriter.init(options);

    return recordWriter;
  }

  @JsonTypeName("model")
  public static class ModelFormatConfig implements FormatPluginConfig {

    public List<String> extensions;
    public String delimiter = "";

    public List<String> getExtensions() {
      return extensions;
    }

    public String getDelimiter() {
      return delimiter;
    }

    @Override
    public int hashCode() {
      return -1;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (!(obj instanceof ModelFormatConfig)) {
        return false;
      }

      ModelFormatConfig that = (ModelFormatConfig) obj;
      if (this.delimiter.equals(that.delimiter)) {
        return true;
      }
      return false;
    }

  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.MODEL_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.MODEL_WRITER_VALUE;
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

}
