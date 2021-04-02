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
        Reducer<Text, Text, Text, Text> {
 
    @Override
    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        //pega arg1, no caso min
        Configuration conf = context.getConfiguration();
        String min = conf.get("min");
        int i_min = Integer.parseInt(min);
        
        Text textValue = new Text();
        
        int sum = 0;
        String arqs = "";
        //for (IntWritable value : values) {
        //    sum += value.get();
        //}
        for (Text value : values){
            String line = value.toString();
            sum += Integer.parseInt(line.split(" ")[0]);
            if (arqs.contains(line.split(" ")[1])){
                arqs = arqs;
            }
            else{
                arqs += line.split(" ")[1] + " ";
            }
        }
        //escreve no context apenas se a soma das ocorrencias for maior que
        //o minimo pedido via arg1
        if (sum >= i_min){
            String finalizada = sum + " " + arqs;
            textValue.set(finalizada);
            context.write(text, textValue);
        }
    }
}