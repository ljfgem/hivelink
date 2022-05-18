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
package com.linkedin.hivelink.spark3;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.spark.source.IcebergSource;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.linkedin.hivelink.spark.HivelinkSourceCommonUtils;


public class HivelinkSource implements DataSourceRegister, TableProvider {

  private static class HivelinkIcebergSourceDelegate extends IcebergSource {

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
      Configuration iConf = Optional.ofNullable(SparkSession.getActiveSession().getOrElse(null))
          .map(s -> ((SparkSession) s).sessionState().newHadoopConf()).orElse(new Configuration());

      org.apache.iceberg.Table iTable = HivelinkSourceCommonUtils.findTable(withHivelinkDefaultOptions(options), iConf);
      CaseInsensitiveStringMap insensitiveOptions = new CaseInsensitiveStringMap(options);
      boolean cacheEnabled = Boolean.parseBoolean(insensitiveOptions.getOrDefault("cache-enabled", "true"));
      return new SparkTable(iTable, schema, !cacheEnabled) {
        @Override
        public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
          return super.newScanBuilder(new CaseInsensitiveStringMap(withHivelinkDefaultOptions(options)));
        }
      };
    }

    private Map<String, String> withHivelinkDefaultOptions(Map<String, String> options) {
      Map<String, String> mp = new HashMap<>(options);
      // Fetching locality currently causes huge slowdowns for naive queries like df.show(1) due to
      // 1. HDFS CRS causing significant increase in latency for getting file block information
      // (20x)
      // 2. Iceberg fetching locality information for each file serially (probably an oversight, not
      // too huge impact).
      // So we disable locality till we can either parallelize fetching block info and/or reduce
      // latency due to CRS
      mp.putIfAbsent("locality", "false");
      // We have hive tables which contain timestamp types,
      // hive timestamp type is timezone-less, but spark only supports read of timestamp with
      // timezone
      // type, so in iceberg-spark we add support to implicitly convert timestamp without timezone
      // type to with timezone type for spark to be able to read
      // see linkedin iceberg pr #105, we need to turn on this flag to enable such conversion
      mp.putIfAbsent("read-timestamp-without-zone", "true");
      // if the split-size option is already set, then it means user
      // has set the config, or the user has explicitly called the option()
      // themselves,
      // then we don't need to set it again here.
      // From a sparkSQL user's perspective, they can still specify the spark maxPartitionBytes
      // config, it will be mapped to iceberg's internal option here.
      long splitSizeFromConf = SQLConf.get().filesMaxPartitionBytes();
      mp.putIfAbsent("split-size", Long.toString(splitSizeFromConf));
      return mp;
    }
  }

  private final IcebergSource icebergSourceDelegate;

  /** This needs to be a public constructor for spark to invoke the source */
  public HivelinkSource() {
    icebergSourceDelegate = new HivelinkIcebergSourceDelegate();
  }

  @Override
  public String shortName() {
    return "hivelink";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return icebergSourceDelegate.inferSchema(options);
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).partitioning();
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return icebergSourceDelegate.getTable(schema, partitioning, properties);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return icebergSourceDelegate.supportsExternalMetadata();
  }
}
