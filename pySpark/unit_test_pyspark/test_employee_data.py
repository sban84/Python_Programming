import logging
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark_test import assert_pyspark_df_equal

from employee_data import increase_sal


class PySparkTest(unittest.TestCase):
    spark = None

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master("local[2]")
                .appName("my - local - testing - pyspark - context")
                .enableHiveSupport()
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestEmployeeData(PySparkTest):
    spark = None

    # def setUpClass(self):
    #     # conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
    #     # cls.sc = pyspark.SparkContext(conf=conf)
    #     # cls.spark = pyspark.SQLContext(cls.sc)
    #     self.spark = SparkSession.builder.getOrCreate()
    #
    # def tearDownClass(self):
    #     self.spark.stop()

    def test_increase_sal(self):
        test_data_df = self.spark.createDataFrame(
            [
                ("a", "IT", 100),
                ("b", "HR", 200),
                ("c", "SALES", 500),
                ("a1", "IT", 200),
                ("c1", "SALES", 500)
            ], ["name", "dept", "sal"]
        )
        actual_result_df = increase_sal(test_data_df)
        #result_df.printSchema()

        expected_result = {"IT": 105.0}
        result_dict = {k: float(v) for k, v in actual_result_df.filter(col("name") == "a").select("dept", "sal").collect()}
        # print(result_df.filter(col("name") == "a").select("dept","sal").collect())
        self.assertEqual(expected_result, result_dict)

    def test_employee_df(self):
        test_data_df = self.spark.createDataFrame(
            [
                ("a", "IT", 100),
                ("b", "HR", 200),
                ("c", "SALES", 500),
                ("a1", "IT", 200),
                ("c1", "SALES", 500)
            ], ["name", "dept", "sal"]
        )

        actual_result_df = increase_sal(test_data_df)

        expected_df = self.spark.createDataFrame(
            [
                ("a", "IT", "105.0"),
                ("b", "HR", "210.0"),
                ("c", "SALES", "525.0"),
                ("a1", "IT", "210.0"),
                ("c1", "SALES", "525.0")
            ], ["name", "dept", "sal"]
        )
        assert_pyspark_df_equal(expected_df, actual_result_df)

