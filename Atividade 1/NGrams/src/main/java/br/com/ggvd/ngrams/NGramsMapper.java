/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.ggvd.ngrams;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;


public class NGramsMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line, " ");
        List<String> st_list = new ArrayList<String>();
        //while (st.hasMoreTokens()) {
        //    word.set(st.nextToken());
        //    context.write(word, ONE);
        //}
        //cria lista de tokens
        while (st.hasMoreTokens()){
            st_list.add(st.nextToken());
        }
        //pega arg0, no caso n
        Configuration conf = context.getConfiguration();
        String n = conf.get("n");
        int i_n = Integer.parseInt(n);
        //gera os ngrams
        for (int i = 0; i < (st_list.size()-(i_n-1)); i++){
            String ngram = "";
            for (int j = i; j < (i_n + i); j++){
                if (j == i) {
                    ngram = st_list.get(j);
                }
                else{
                    ngram = ngram + " " + st_list.get(j); 
                }
            }
            word.set(ngram);
            context.write(word, ONE);
        }
    }
}
