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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;

/**
 * The {@link org.apache.hadoop.io.serializer.Serialization} used by jobs
 * configured with {@link AvroJob}.
 */
public class AvroSerialization<T>
    extends com.ci.backports.avro.mapred.AvroSerialization<T> {

  @Override
  protected Schema getDeserializerSchema(Class<AvroWrapper<T>> c) {
    // We need not rely on mapred.task.is.map here to determine whether map
    // output or final output is desired, since the mapreduce framework never
    // creates a deserializer for final output, only for map output.
    boolean isKey = AvroKey.class.isAssignableFrom(c);
    return isKey
        ? AvroJob.getMapOutputKeySchema(getConf())
        : AvroJob.getMapOutputValueSchema(getConf());
  }

  @Override
  protected Schema getSerializerSchema(Class<AvroWrapper<T>> c) {
    // Here we must rely on mapred.task.is.map to tell whether the map output
    // or final output is needed.
    boolean isMap = getConf().getBoolean("mapred.task.is.map", false);
    return !isMap
        ? AvroJob.getOutputSchema(getConf())
        : (AvroKey.class.isAssignableFrom(c)
            ? AvroJob.getMapOutputKeySchema(getConf())
            : AvroJob.getMapOutputValueSchema(getConf()));
  }
}
