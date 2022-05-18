/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.hivelink.spark2;

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.IcebergSource;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import com.linkedin.hivelink.spark.HivelinkSourceCommonUtils;


public class HivelinkSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister {

  /**
   * Ideally we'd like to use the decorator pattern but the findTable function has a protected
   * classification so we extend it and override the relevant logic to minimize code duplication
   * with IcebergSource
   */
  private static class HivelinkIcebergSourceDelegate extends IcebergSource {
    protected Table findTable(DataSourceOptions options, Configuration conf) {
      return HivelinkSourceCommonUtils.findTable(options.asMap(), conf);
    }
  }

  private final IcebergSource icebergSourceDelegate;

  /** This needs to be a public constructor for spark to invoke the source */
  public HivelinkSource() {
    icebergSourceDelegate = new HivelinkIcebergSourceDelegate();
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return createReader(null, options);
  }

  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    Map<String, String> optionsMap = options.asMap();
    // Fetching locality currently causes huge slowdowns for naive queries like df.show(1) due to
    // 1. HDFS CRS causing significant increase in latency for getting file block information (20x)
    // 2. Iceberg fetching locality information for each file serially (probably an oversight, not
    // too huge impact).
    // So we disable locality till we can either parallelize fetching block info and/or reduce
    // latency due to CRS
    if (!options.get("locality").isPresent()) {
      optionsMap.put("locality", "false");
    }
    // We have hive tables which contain timestamp types,
    // hive timestamp type is timezone-less, but spark only supports read of timestamp with timezone
    // type, so in iceberg-spark we add support to implicitly convert timestamp without timezone
    // type to with timezone type for spark to be able to read
    // see linkedin iceberg pr #105, we need to turn on this flag to enable such conversion
    optionsMap.put("read-timestamp-without-zone", "true");

    // if the split-size option is already set, then it means user
    // has set the config, or the user has explicitly called the option() themselves,
    // then we don't need to set it again here.
    // From a sparkSQL user's perspective, they can still specify the spark maxPartitionBytes
    // config, it will be mapped to iceberg's internal option here.
    if (!optionsMap.containsKey("split-size")) {
      String splitSize = SparkSession.builder().getOrCreate().conf().get("spark.sql.files.maxPartitionBytes");
      optionsMap.put("split-size", splitSize);
    }

    options = new DataSourceOptions(optionsMap);
    return icebergSourceDelegate.createReader(schema, options);
  }

  @Override
  public String shortName() {
    return "hivelink";
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode,
      DataSourceOptions options) {
    return icebergSourceDelegate.createWriter(writeUUID, schema, mode, options);
  }
}
