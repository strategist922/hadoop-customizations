/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ci.backports.avro.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumWriter;


/**
 * A {@link org.apache.hadoop.mapreduce.OutputFormat} for Avro container files.
 */
public class AvroOutputFormat<T>
    extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  /** Enable output compression using the deflate codec and specify its level.*/
  public static void setDeflateLevel(Job job, int level) {
    FileOutputFormat.setCompressOutput(job, true);
    job.getConfiguration().setInt(
        org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY, level);
  }

  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException {
    Schema schema = AvroJob.getOutputSchema(context.getConfiguration());
    if (schema == null) {
      throw new RuntimeException("AvroOutputFormat requires an output schema.");
    }

    final DataFileWriter<T> writer
        = new DataFileWriter<T>(new SpecificDatumWriter<T>());

    if (FileOutputFormat.getCompressOutput(context)) {
      int level = context.getConfiguration().getInt(
          org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY,
          org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
      writer.setCodec(CodecFactory.deflateCodec(level));
    }

    Path path = getDefaultWorkFile(
        context, org.apache.avro.mapred.AvroOutputFormat.EXT);
    writer.create(
        schema, path.getFileSystem(context.getConfiguration()).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
      public void write(AvroWrapper<T> record, NullWritable ignore)
          throws IOException {
        writer.append(record.datum());
      }
      public void close(TaskAttemptContext context) throws IOException {
        writer.close();
      }
    };
  }
}
