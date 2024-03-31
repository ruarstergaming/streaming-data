//Import all the appropriate packages needed for the program to run
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;

import au.com.bytecode.opencsv.*;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.stream.*;

import scala.Tuple2;

public class CS1003P4 {

    public static void main(String[] args) throws Exception {

        //Ensure the correct amount of parameters are passed
        if(!(args.length == 3)){
            System.out.println("Error: Incorrect arguments passed. Please pass: The path to the directory that contains the text files, the search term, similiarity threshold");
            System.exit(0);
        }

        //Take in the command line arguments and store them in variables
        String textFilesPath = args[0];
        String searchItem = args[1];
        float similarityThreshold = Float.parseFloat(args[2]);

        //Check the similarity threshhold of the results 
        if(similarityThreshold > 1 || similarityThreshold < 0){
            System.out.println("Error: similarity threshold provided must be between 0 and 1 inclusive.");
            System.exit(0);
        }

        //Find the length of the phrase in words
        String[] searchWords = searchItem.split(" ");
        int searchLength = searchWords.length;

        //Set up the Spark Connection
        String appName = "HelloSpark";
        String cluster = "local[*]"; 

        SparkConf conf = new SparkConf()
                                        .setAppName(appName)
                                        .setMaster(cluster); 

        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read in the data files
        JavaPairRDD<String, String> bookFile = sc.wholeTextFiles(textFilesPath);

        //Combine the string values from the files aka the actual words in the all the books into one RDD
        JavaRDD<String> books = bookFile.values();

        //Call methods to store the phrases that are the same word length as the search term and store in RDD
        JavaRDD<String> phrases = findPhrases(books, searchLength);

        //Create an RDD for a list of strings of phrases by running the cut phrases method on the strings in the phrases RDD
        JavaRDD<ArrayList<String>> cutPhrases = phrases.map(s -> cutPhrases(s, searchLength));

        //Flatten the array list RDD of phrases into a simple String one
        JavaRDD<String> finalPhrases = cutPhrases.flatMap(s -> s.iterator());

        //Call a method to calculate the similarity index of each phrase and store it in a pair RDD
        JavaRDD<String> similarPhrases = finalPhrases.filter(s -> calculateJaccard(s, searchItem) >= similarityThreshold);

        //Now that all the phrases that are similiar enough to be above the threshold, simply stream it out to the display.
        similarPhrases.foreach(x -> System.out.println(x));

    }

    /**
     * Finds the phrases in the given RDD that have the same length as a given search term
     * @param books, JavaRDD<String> this is the RDD the phrases are gotten from 
     * @param searchLength, integer that represents the length the phrase should be in words
     * @return JavaRDD<String>, the RDD of the string phrases
     */
    private static JavaRDD<String> findPhrases(JavaRDD<String> books, int searchLength){

        //Store the RDD passed into the local RDD
        JavaRDD<String> phrases = books;

        //Filter the RDD by spltting the strings into phrases the should be in words by calling a method
        phrases = phrases.filter(s -> getValidPhraseLengths(s, searchLength))
                         .map(s -> s.replaceAll("[^a-zA-Z0-9]", " ")) 
                         .map(s -> s.toLowerCase());
        return phrases;
    }

    /**
     * Takes in a given phrase, cleans the texual data and then checks if it is of a usable length 
     * (e.g. if the search term has 4 words you can compare it with a phrase thats 3 words but you can split up one thats 8 words)
     * @param phrase, a string from the RDD thats the phrase in the book
     * @param searchLength, the int length of the search term i.e. what the phrase's length should eventually be
     * @return Boolean, a boolean value representing if the passed in phrase is of a valid length   
     */
    private static Boolean getValidPhraseLengths(String phrase, int searchLength){

        //Clean up the string being passed in a bit
        phrase = phrase.replaceAll("[^a-zA-Z0-9]", " ");
        phrase = phrase.toLowerCase();

        //Split the phrase into an array of string containing thats words
        String[] phraseWords = phrase.split(" ");
        
        //If the array is greater in length or equal in length to the search terms word length then return true other wise return false
        if(phraseWords.length >= searchLength){
            return true;
        }
        return false;

    }

    /**
     * This cuts the phrases passed in to the exact length to be able to compare them.
     * @param phrase, String phrase being passed is going to be cut down
     * @param searchLength, Integer of the length in words the phrase should be i.e. the same as the search term
     * @return ArrayList<String>, The list of string phrases that are now the correct length  
     */
    private static ArrayList<String> cutPhrases(String phrase, int searchLength){

        //Split the phrase into the individual words in an array
        String[] phraseWords = phrase.split(" ");

        //Declare an array list to get rid of any lingering null characters and spaces
        ArrayList<String> cleanPhrases = new ArrayList<String>();

        //Loop for each word in the array
        for(String word:phraseWords){

            //If there are some lingering null or space characters ignore them otherwise trim them and add them to the array list 
            if(!word.equals(" ") && !word.equals("")){
                word = word.trim();
                cleanPhrases.add(word);
            }
        }

        //Now take the arraylist and convert it to a string array and stor it in the previous array variable
        phraseWords = cleanPhrases.toArray(String[]::new);

        //Store the length of the phrase
        int phraseLength = phraseWords.length;

        //Create the array list thats going to be returned
        ArrayList<String> cutPhrases = new ArrayList<>();

        //If the phrase is already of the correct length
        if(phraseLength == searchLength){

            //Declare a string then loop through each word in the phrase words and concatinate them with a space between each
            String cutPhrase = "";
            for(String word:phraseWords){

                //Again ensure theres no unwanted lingering data being added
                if(!word.equals(" ") && !word.equals("")){
                    cutPhrase += word + " ";
                }
            }

            //Ensure there are no extra spaces at the end
            cutPhrase = cutPhrase.trim();

            //Once its been concatanated add the phrase to the array list
            cutPhrases.add(cutPhrase);
        }

        //Otherwise the phrase length need to be cut down 
        else{

            //Loop through the difference in the lengths
            for(int i = 0; i < (phraseLength - searchLength); i++){

                //Declare a string then loop and concatinate the phrase words to the correct length
                String cutPhrase = "";
                for(int j = i; j < (searchLength + i); j++){

                    //Again ensure theres no unwanted lingering data being added
                    if(!phraseWords[j].equals(" ") && !phraseWords[j].equals("")){
                        cutPhrase += phraseWords[j] + " ";
                    }
                }

                //Ensure there are no extra spaces at the end
                cutPhrase = cutPhrase.trim();

                //Once its been concatanated add the phrase to the array list
                cutPhrases.add(cutPhrase);
            }
        }

        //Return the arra list
        return cutPhrases;

    }

    /**
     * This calculates the Jaccard index between the phrases and search term
     * @param phrase, the string of the phrase being compared to the search term
     * @param searchTerm, the string which is the search term used to compare similiarity with
     * @return float, this is simply the jaccard similarity index calculated between the search term and phrase  
     */
    private static float calculateJaccard(String phrase, String searchTerm){
        

        //Create a hash set object
        Set<String> bigramSet = new HashSet<String>();

        //For loop to loop through the characters of the phrase to add each bigram to the set
        for (int i = 0; i < (phrase.length()-1); i++) {
            bigramSet.add(phrase.substring(i, i+2));
        }

        //Create a hash set object
        Set<String> searchBigramSet = new HashSet<String>();

        //For loop to loop through the characters of the phrase to add each bigram to the set
        for (int i = 0; i < (searchTerm.length()-1); i++) {
            searchBigramSet.add(searchTerm.substring(i, i+2));
        }

        //Declare a floating point number for the score and 2 sets used to calculate the jaccard index
        float jaccardIndex;
        Set<String> intersectionSet = new HashSet<String>();
        Set<String> unionSet = new HashSet<String>();

        //Find the intersecting bigrams by add one bigram then using the retain all function with the other
        intersectionSet.addAll(bigramSet);
        intersectionSet.retainAll(searchBigramSet);

        //Find the union bigram set by adding all the bigrams from both sets to the 1 set
        unionSet.addAll(bigramSet);
        unionSet.addAll(searchBigramSet);

        //Calculate the score by dividing the size of the intersection by the size of the union
        jaccardIndex = (float) intersectionSet.size()/unionSet.size();

        return jaccardIndex;
    }



}