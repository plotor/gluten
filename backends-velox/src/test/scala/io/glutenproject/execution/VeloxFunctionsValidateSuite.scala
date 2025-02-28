/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.glutenproject.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types._

import java.nio.file.Files

import scala.collection.JavaConverters._

class VeloxFunctionsValidateSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  override protected val backend: String = "velox"

  private var parquetPath: String = _

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set(
        "spark.sql.optimizer.excludedRules",
        ConstantFolding.ruleName + "," +
          NullPropagation.ruleName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(
      Array(
        StructField("double_field1", DoubleType, true),
        StructField("int_field1", IntegerType, true),
        StructField("string_field1", StringType, true)
      ))
    val rowData = Seq(
      Row(1.025, 1, "{\"a\":\"b\"}"),
      Row(1.035, 2, null),
      Row(1.045, 3, null)
    )

    var dfParquet = spark.createDataFrame(rowData.asJava, schema)
    dfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("datatab", parquetPath, fileFormat)
  }

  test("Test bit_count function") {
    runQueryAndCompare("SELECT bit_count(l_partkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test bit_get function") {
    runQueryAndCompare("SELECT bit_get(l_partkey, 0) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test chr function") {
    runQueryAndCompare("SELECT chr(l_orderkey + 64) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test abs function") {
    runQueryAndCompare("SELECT abs(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test ceil function") {
    runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test floor function") {
    runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Exp function") {
    runQueryAndCompare("SELECT exp(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Power function") {
    runQueryAndCompare("SELECT power(l_orderkey, 2) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test Pmod function") {
    runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("Test round function") {
    runQueryAndCompare(
      "SELECT round(cast(l_orderkey as int), 2)" +
        "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test greatest function") {
    runQueryAndCompare(
      "SELECT greatest(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test least function") {
    runQueryAndCompare(
      "SELECT least(l_orderkey, l_orderkey)" +
        "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test hash function") {
    runQueryAndCompare("SELECT hash(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object datatab function") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.a') " +
        "from datatab limit 1;") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object lineitem function") {
    runQueryAndCompare(
      "SELECT l_orderkey, get_json_object('{\"a\":\"b\"}', '$.a') " +
        "from lineitem limit 1;") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("json_array_length") {
    runQueryAndCompare(
      s"select *, json_array_length(string_field1) " +
        s"from datatab limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, json_array_length('[1,2,3,4]') " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, json_array_length(null) " +
        s"from lineitem limit 5")(checkOperatorMatch[ProjectExecTransformer])
  }

  test("Test acos function") {
    runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test asin function") {
    runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test atan function") {
    runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("Test atan2 function datatab") {
    runQueryAndCompare("SELECT atan2(double_field1, 0) from datatab limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test ceiling function") {
    runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test cos function") {
    runQueryAndCompare("SELECT cos(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test cosh function") {
    runQueryAndCompare("SELECT cosh(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test degrees function") {
    runQueryAndCompare("SELECT degrees(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test log10 function") {
    runQueryAndCompare("SELECT log10(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test shiftleft function") {
    val df = runQueryAndCompare("SELECT shiftleft(int_field1, 1) from datatab limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test shiftright function") {
    val df = runQueryAndCompare("SELECT shiftright(int_field1, 1) from datatab limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("date_add") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("2022-03-11"), 1: Integer),
          (java.sql.Date.valueOf("2022-03-12"), 2: Integer),
          (java.sql.Date.valueOf("2022-03-13"), 3: Integer),
          (java.sql.Date.valueOf("2022-03-14"), 4: Integer),
          (java.sql.Date.valueOf("2022-03-15"), 5: Integer),
          (java.sql.Date.valueOf("2022-03-16"), 6: Integer)
        )
          .toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT date_add(a, b) from view") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("date_diff") {
    withTempPath {
      path =>
        Seq(
          (java.sql.Date.valueOf("2022-03-11"), java.sql.Date.valueOf("2022-02-11")),
          (java.sql.Date.valueOf("2022-03-12"), java.sql.Date.valueOf("2022-01-12")),
          (java.sql.Date.valueOf("2022-09-13"), java.sql.Date.valueOf("2022-05-12")),
          (java.sql.Date.valueOf("2022-07-14"), java.sql.Date.valueOf("2022-03-12")),
          (java.sql.Date.valueOf("2022-06-15"), java.sql.Date.valueOf("2022-01-12")),
          (java.sql.Date.valueOf("2022-05-16"), java.sql.Date.valueOf("2022-06-12"))
        )
          .toDF("a", "b")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("view")

        runQueryAndCompare("SELECT datediff(a, b) from view") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("array_aggregate") {
    withTempPath {
      path =>
        Seq[Seq[Integer]](
          Seq(1, 9, 8, 7),
          Seq(5, null, 8, 9, 7, 2),
          Seq.empty,
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare(
          "select aggregate(i, 0, (acc, x) -> acc + x," +
            " acc -> acc * 3) as v from array_tbl;") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
    withTempPath {
      path =>
        Seq(
          (1, Array[Int](1, 2, 3)),
          (5, Array[Int](4, 5, 6))
        )
          .toDF("x", "ys")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("array_tbl")

        runQueryAndCompare("select aggregate(ys, 0, (y, a) -> y + a + x) as v from array_tbl;") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  test("map literal - offload") {
    def validateOffloadResult(sql: String): Unit = {
      // allow constant folding
      withSQLConf("spark.sql.optimizer.excludedRules" -> "") {
        runQueryAndCompare(sql) {
          df =>
            val plan = df.queryExecution.executedPlan
            assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined, sql)
            assert(plan.find(_.isInstanceOf[ProjectExec]).isEmpty, sql)
        }
      }
    }

    validateOffloadResult("SELECT map('b', 'a', 'e', 'e')")
    validateOffloadResult("SELECT map(1, 'a', 2, 'e')")
    validateOffloadResult("SELECT map(1, map(1,2,3,4))")
    validateOffloadResult("SELECT array(map(1,2,3,4))")
    validateOffloadResult("SELECT map(array(1,2,3), array(1))")
    validateOffloadResult("SELECT array(map(array(1,2), map(1,2,3,4)))")
    validateOffloadResult("SELECT map(array(1,2), map(1,2))")
  }

  test("map literal - fallback") {
    def validateFallbackResult(sql: String): Unit = {
      withSQLConf("spark.sql.optimizer.excludedRules" -> "") {
        runQueryAndCompare(sql) {
          df =>
            val plan = df.queryExecution.executedPlan
            assert(plan.find(_.isInstanceOf[ProjectExecTransformer]).isEmpty, sql)
            assert(plan.find(_.isInstanceOf[ProjectExec]).isDefined, sql)
        }
      }
    }

    validateFallbackResult("SELECT array()")
    validateFallbackResult("SELECT array(array())")
    validateFallbackResult("SELECT array(null)")
    validateFallbackResult("SELECT array(map())")
    validateFallbackResult("SELECT map()")
    validateFallbackResult("SELECT map(1, null)")
    validateFallbackResult("SELECT map(1, array())")
    validateFallbackResult("SELECT map(1, map())")
  }

  test("map extract - getmapvalue") {
    withTempPath {
      path =>
        Seq(
          Map[Int, Int](1 -> 100, 2 -> 200),
          Map[Int, Int](),
          Map[Int, Int](1 -> 100, 2 -> 200, 3 -> 300),
          null
        )
          .toDF("i")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("map_tbl")

        runQueryAndCompare("select i[\"1\"] from map_tbl") {
          checkOperatorMatch[ProjectExecTransformer]
        }
    }
  }
}
