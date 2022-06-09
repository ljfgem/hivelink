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
package com.linkedin.hivelink.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.hivelink.core.TestHiveMetastore;


public abstract class TestHivelinkSource {

  protected static final String DB_NAME = "test_hivelink_source";
  protected static SparkSession spark;
  protected static TestHiveMetastore metastore;

  @BeforeClass
  public static void setup() {
    spark = SparkSession.builder().appName("unit test").master("local[2]").getOrCreate();
    metastore = new TestHiveMetastore();
    metastore.start();
    metastore.createDatabase(DB_NAME);
  }

  @AfterClass
  public static void teardown() {
    spark.stop();
    metastore.stop();
  }

  @Test
  public void testHivelinkIcebergSourceDelegate() {
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] { "bar1.1", "bar2.1" });
    stringAsList.add(new String[] { "bar1.2", "bar2.2" });

    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));
    StructType schema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("foe1",
        DataTypes.StringType, false), DataTypes.createStructField("foe2", DataTypes.StringType, false) });

    Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "test_delegate");
    HiveCatalog catalog = HiveCatalogs.loadCatalog(spark.sessionState().newHadoopConf());
    catalog.createTable(identifier, SparkSchemaUtil.convert(schema));
    df.write().format("hivelink").mode("append").save(identifier.toString());
    Dataset<Row> rows = spark.read().format("hivelink").load(identifier.toString());
    Assert.assertEquals(2, rows.count());
  }

  @Test
  public void testIncrementalApi() {
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] { "bar1.1", "bar2.1" });
    stringAsList.add(new String[] { "bar1.2", "bar2.2" });

    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));
    StructType schema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("foe1",
        DataTypes.StringType, false), DataTypes.createStructField("foe2", DataTypes.StringType, false) });

    Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "test_incremental_api");
    HiveCatalog catalog = HiveCatalogs.loadCatalog(spark.sessionState().newHadoopConf());
    catalog.createTable(identifier, SparkSchemaUtil.convert(schema));
    df.write().format("hivelink").mode("append").save(identifier.toString());
    // refresh metadata
    Table t = catalog.loadTable(identifier);
    Snapshot start = t.currentSnapshot();
    df.write().format("hivelink").mode("append").save(identifier.toString());
    t = catalog.loadTable(identifier);
    Snapshot end = t.currentSnapshot();
    Dataset<Row> rows = spark.read().format("hivelink").option("start-snapshot-id", start.snapshotId())
        .option("end-snapshot-id", end.snapshotId()).load(identifier.toString());
    Assert.assertEquals(2, rows.count());
  }

  @Test
  public void testSplitSizeOption() {
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] { "bar1.1", "bar2.1" });
    stringAsList.add(new String[] { "bar1.2", "bar2.2" });

    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map(RowFactory::create);
    StructType schema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("foe1",
        DataTypes.StringType, false), DataTypes.createStructField("foe2", DataTypes.StringType, false) });

    Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "test_split_size_table");
    HiveCatalog catalog = HiveCatalogs.loadCatalog(spark.sessionState().newHadoopConf());
    catalog.createTable(identifier, SparkSchemaUtil.convert(schema));
    df.write().option("write-format", "orc").format("hivelink").mode("overwrite").save(identifier.toString());
    // refresh metadata
    catalog.loadTable(identifier);

    spark.conf().set("spark.sql.files.maxPartitionBytes", "10");
    int nSplitsBefore = spark.read().format("hivelink").load(identifier.toString()).javaRDD().getNumPartitions();

    spark.conf().set("spark.sql.files.maxPartitionBytes", "5");
    int nSplitsAfter = spark.read().format("hivelink").load(identifier.toString()).javaRDD().getNumPartitions();

    Assert.assertEquals(2, Math.round(nSplitsAfter / (double) nSplitsBefore));
  }
}
