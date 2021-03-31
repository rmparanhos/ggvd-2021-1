/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngrams;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
 
public class NGramsReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
 
    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        //pega arg1, no caso min
        Configuration conf = context.getConfiguration();
        String min = conf.get("min");
        int i_min = Integer.parseInt(min);
        
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //escreve no context apenas se a soma das ocorrencias for maior que
        //o minimo pedido via arg1
        if (sum >= i_min){
            context.write(text, new IntWritable(sum));
        }
    }
}