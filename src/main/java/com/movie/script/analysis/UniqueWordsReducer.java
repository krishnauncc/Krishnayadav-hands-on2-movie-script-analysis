package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    private Text uniqueWordsText = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueWordSet = new HashSet<>();

        for (Text val: values){
            uniqueWordSet.add(val.toString());
        }

        StringBuilder uniqWordsBuilder = new StringBuilder();
        for (String word: uniqueWordSet){
            uniqWordsBuilder.append(word + ", ");
        }

        if (uniqWordsBuilder.length() > 0){
            uniqWordsBuilder.setLength(uniqWordsBuilder.length() - 2);
        }

        uniqueWordsText.set(uniqWordsBuilder.toString());

        context.write(key, uniqueWordsText); //emit (charachter, (unique words seperated by , ))
    }
}
