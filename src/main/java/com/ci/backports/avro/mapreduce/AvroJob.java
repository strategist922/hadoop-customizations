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

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

/** Setters to configure jobs for Avro data. */
public class AvroJob {
  private AvroJob() {}  // no public constructor.

  public static final String INPUT_SCHEMA_CONFIG_FIELD = "avro.schema.input";
  private static final String KEY_MAP_OUTPUT_SCHEMA_CONFIG_FIELD
    = "avro.schema.mapoutput.key";
  private static final String VALUE_MAP_OUTPUT_SCHEMA_CONFIG_FIELD
    = "avro.schema.mapoutput.value";
  private static final String OUTPUT_SCHEMA_CONFIG_FIELD = "avro.schema.output";

  /** Configure a job's map input schema. */
  public static void setInputSchema(Job job, Schema schema) {
    job.getConfiguration().set(INPUT_SCHEMA_CONFIG_FIELD, schema.toString());
  }

  /** Configure a job's map output key schema. */
  public static void setMapOutputKeySchema(Job job, Schema schema) {
    job.setMapOutputKeyClass(AvroKey.class);
    job.getConfiguration().set(
        KEY_MAP_OUTPUT_SCHEMA_CONFIG_FIELD, schema.toString());
    job.setGroupingComparatorClass(AvroKeyComparator.class);
    job.setSortComparatorClass(AvroKeyComparator.class);
    addAvroSerialization(job.getConfiguration());
  }

  /** Configure a job's map output value schema. */
  public static void setMapOutputValueSchema(Job job, Schema schema) {
    job.setMapOutputValueClass(AvroValue.class);
    job.getConfiguration().set(
        VALUE_MAP_OUTPUT_SCHEMA_CONFIG_FIELD, schema.toString());
    addAvroSerialization(job.getConfiguration());
  }

  /** Configure a job's output schema. */
  public static void setOutputSchema(Job job, Schema schema) {
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    job.getConfiguration().set(OUTPUT_SCHEMA_CONFIG_FIELD, schema.toString());
    addAvroSerialization(job.getConfiguration());
  }

  private static void addAvroSerialization(Configuration conf) {
    Collection<String> serializations
        = conf.getStringCollection("io.serializations");
    if (!serializations.contains(AvroSerialization.class.getName())) {
      serializations.add(AvroSerialization.class.getName());
      conf.setStrings("io.serializations",
          serializations.toArray(new String[0]));
    }
  }

  /** Return a job's map input schema. */
  public static Schema getInputSchema(Configuration conf) {
    String schemaString = conf.get(INPUT_SCHEMA_CONFIG_FIELD);
    return schemaString != null ? Schema.parse(schemaString) : null;
  }

  /** Return a job's map output key schema. */
  public static Schema getMapOutputKeySchema(Configuration conf) {
    String schemaString = conf.get(KEY_MAP_OUTPUT_SCHEMA_CONFIG_FIELD);
    return schemaString != null ? Schema.parse(schemaString) : null;
  }

  /** Return a job's map output value schema. */
  public static Schema getMapOutputValueSchema(Configuration conf) {
    String schemaString = conf.get(VALUE_MAP_OUTPUT_SCHEMA_CONFIG_FIELD);
    return schemaString != null ? Schema.parse(schemaString) : null;
  }

  /** Return a job's output schema. */
  public static Schema getOutputSchema(Configuration conf) {
    String schemaString = conf.get(OUTPUT_SCHEMA_CONFIG_FIELD);
    return schemaString != null ? Schema.parse(schemaString) : null;
  }
}
