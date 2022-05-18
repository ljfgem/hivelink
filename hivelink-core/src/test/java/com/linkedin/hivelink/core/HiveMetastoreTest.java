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
package com.linkedin.hivelink.core;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.AfterClass;
import org.junit.BeforeClass;


/**
 * hivelink refactoring:
 * This class is copied from iceberg-hive-metastore module test code
 */
public abstract class HiveMetastoreTest {

  protected static final String DB_NAME = "hivedb";

  protected static HiveMetaStoreClient metastoreClient;
  protected static HiveCatalog catalog;
  protected static HiveConf hiveConf;
  protected static TestHiveMetastore metastore;

  @BeforeClass
  public static void startMetastore() throws MetaException {
    metastore = new TestHiveMetastore();
    metastore.start();
    hiveConf = metastore.hiveConf();
    catalog = new HiveCatalog(hiveConf);
    metastoreClient = metastore.getMetastoreClient();
    metastore.createDatabase(DB_NAME);
  }

  @AfterClass
  public static void stopMetastore() {
    catalog.close();
    HiveMetastoreTest.catalog = null;

    metastoreClient.close();
    HiveMetastoreTest.metastoreClient = null;

    metastore.stop();
    HiveMetastoreTest.metastore = null;
  }
}
