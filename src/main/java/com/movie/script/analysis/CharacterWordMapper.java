package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Add your mapper implementation here
            // Split the line by colon to separate character and dialogue
            String[] parts = value.toString().split(":");

            if (parts.length > 1) {
                String character = parts[0].trim(); // Character's name
                String dialogue = parts[1].trim(); // Words spoken by the character

                // Tokenize the dialogue into words
                StringTokenizer itr = new StringTokenizer(dialogue);
                while (itr.hasMoreTokens()) {
                    String characterWord = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Clean the word
                    if (!characterWord.isEmpty()) {
                        word.set(character + ":" + characterWord);
                        context.write(word, one); // Emit (character:word, 1)
                    }
                }
            }
        }

}

