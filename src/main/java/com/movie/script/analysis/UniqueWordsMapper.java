package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the input line to a string and trim whitespace
        String[] parts = value.toString().split(":");

            if (parts.length > 1) {
                String characterNameString = parts[0].trim(); // Character's name
                String dialogue = parts[1].trim(); // Words spoken by the character
                character.set(characterNameString);

                // Tokenize the dialogue into words
                StringTokenizer itr = new StringTokenizer(dialogue);
                while (itr.hasMoreTokens()) {
                    String characterWord = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Clean the word
                    if (!characterWord.isEmpty()) {
                        word.set(characterWord);
                        context.write(character, word); // Emit (character:word, 1)
                    }
                }
            }
    }
}

