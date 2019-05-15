package com.sankir;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DataPartDemo {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Integer,Integer> rdd1 = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer,Integer>(1,10),
                new Tuple2<Integer,Integer>(2,10),
                new Tuple2<Integer,Integer>(3,10),
                new Tuple2<Integer,Integer>(4,10),
                new Tuple2<Integer,Integer>(5,10),
                new Tuple2<Integer,Integer>(6,10),
                new Tuple2<Integer,Integer>(7,10),
                new Tuple2<Integer,Integer>(8,10),
                new Tuple2<Integer,Integer>(9,10),
                new Tuple2<Integer,Integer>(10,10)));

        System.out.println("Output of rdd1");
        System.out.println(rdd1.collect());
        System.out.println("Partitions (Before): " + rdd1.partitions().size());

        String s = new String("SANKIRTECHNOLOGIES");
        System.out.println("Hashcode: " + s.hashCode() + "Partition: " + s.hashCode() % 3 );

        JavaPairRDD<Integer,Integer> partRDD = rdd1.partitionBy(new HashPartitioner(3));
        System.out.println("Partitions(After partitioning): " + partRDD.partitions().size());
        System.out.println(partRDD.collect());

        JavaRDD<String> mapPartRDD = partRDD.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list=new ArrayList<>();
            while(tupleIterator.hasNext()){
                list.add("Partition number:"+index+",key:"+ tupleIterator.next()._1());
            }
            return list.iterator();
        }, false);
        System.out.println(mapPartRDD.collect());

        JavaPairRDD<Integer,Integer> repartRDD = partRDD.repartitionAndSortWithinPartitions(new HashPartitioner(4));
        System.out.println(repartRDD.collect());

        JavaRDD<String> repartRDDIndexes = repartRDD.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list=new ArrayList<>();
            while(tupleIterator.hasNext()){
                list.add("Partition number:"+index+",key:"+ tupleIterator.next()._1());
            }
            return list.iterator();
        }, false);
        System.out.println(repartRDDIndexes.collect());

        JavaRDD<String> RDD1 = jsc.parallelize(Arrays.asList("one","two", "three","four","five","one","two","three","four"),3);

        JavaRDD<String> mapRDD = RDD1.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> strIter) throws Exception {
                List<String> outList = new ArrayList<String>();
                while(strIter.hasNext()) {
                    String currentObj = strIter.next();
                    outList.add("NEW " + currentObj);
                }
                return  (outList.iterator());
            }
        });


        System.out.println("RDD after mapPartitions:");
        System.out.println(mapRDD.collect());


    }
}
