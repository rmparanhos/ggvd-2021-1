/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngramsspark;

/**
 *
 * @author hadoop
 */

import java.util.ArrayList;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
 
public class NGrams {
    private static final Pattern SPACE = Pattern.compile(" ");
    
    public static String[] montaNGram(String linha, int n){
        String[] linha_a = linha.split(" ");
        List<String> ngrams_l = new ArrayList<String>();
        for (int i = 0; i < (linha_a.length-(n-1)); i++){
            String ngram = "";
            for (int j = i; j < (n + i); j++){
                if (j == i){
                    ngram = linha_a[j]; 
                }
                else{
                    ngram = ngram + " " + linha_a[j];
                }
            }
            //System.out.println(ngram);
            ngrams_l.add(ngram);
        }
        String[] ngrams_a = new String[ngrams_l.size()];
        ngrams_l.toArray(ngrams_a);
        return ngrams_a;
    }
    
    public static void main(String[] args) throws Exception{
        
        if (args.length < 4){
            System.err.println("Usage: NGrams <n> <min> <input> <ouput>");
            System.exit(1);
        }
        
        SparkSession spark = SparkSession.builder().appName("NGrams").getOrCreate();
        
        JavaRDD<String> lines = spark.read().textFile(args[2]+"/*").javaRDD();
        
        //Dataset<String> textDS = spark.read().textFile(args[2]+"/*");
        //System.out.println("------------");
        //String nome_arq = textDS.inputFiles()[0];
        
        //JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(montaNGram(s,Integer.parseInt(args[0]))).iterator());
        
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s,1));
        
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2) -> i1 + i2);
        
        //Function<Tuple2<String, String>, Boolean> filterMinimo = w -> (Integer.parseInt(w._2.split(" ")[0]) >= Integer.parseInt(args[1]));
        Function<Tuple2<String, Integer>, Boolean> filterMinimo = w -> (w._2 >= Integer.parseInt(args[1]));
        
        JavaPairRDD<String, Integer> result = counts.filter(filterMinimo);
        
        //salva no arquivo
        result.saveAsTextFile(args[3]);
        
        List<Tuple2<String,Integer>> output = result.collect();
        for (Tuple2<?,?> tuple : output){
           System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();    
    }
}
