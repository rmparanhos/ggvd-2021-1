/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngrams;

import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class NGrams {
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        // Captura o parâmetros passados após o nome da Classe driver.
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Criar uma configuração
        Configuration conf = new Configuration(true);
 
        // Criar o job
        Job job = new Job(conf, "ContaPalavras");
        job.setJarByClass(NGrams.class);
 
        // Definir classes para Map e Reduce
        job.setMapperClass(NGramsMapper.class);
        job.setReducerClass(NGramsReducer.class);
        job.setNumReduceTasks(1);
 
        // Definir as chaves e valor
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // Entradas
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Saidas
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // EXcluir saida se existir
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Executa job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
}