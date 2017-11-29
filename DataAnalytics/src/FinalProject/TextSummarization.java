package FinalProject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TextSummarization {
	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"TextSummarization");
		j.setJarByClass(TextSummarization.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	public static class MapForWordCount extends Mapper<LongWritable, Text,
	Text, DoubleWritable>{
		public void map(LongWritable key, Text value, Context con) throws
		IOException, InterruptedException
		{
			String para = readFile(con);
			String sentence = value.toString();
			String[] filteredSentences = para.split("(?<=[.!?])\\s*");
			double sfTermWeightScore = 0, sfPropernounScore=0, sfPositionScore=0, sfCosineScore=0, sfThematicScore=0, sfTitleScore=0;
			sfTermWeightScore = termWeight(sentence,para,filteredSentences);
			sfPropernounScore = propernounCheck(sentence);
			sfPositionScore = sentencePosition(sentence,filteredSentences);
			sfCosineScore = cosineSimilarity(sentence, para, filteredSentences);
			sfThematicScore = thematicWord(sentence, para);
			sfTitleScore = titleScore(sentence,filteredSentences);
			con.write(value , new DoubleWritable(sfTermWeightScore));
			con.write(value , new DoubleWritable(sfPropernounScore));
			con.write(value , new DoubleWritable(sfPositionScore));
			con.write(value , new DoubleWritable(sfCosineScore));
			con.write(value , new DoubleWritable(sfThematicScore));
			con.write(value , new DoubleWritable(sfTitleScore));
		}
		
		double titleScore(String sentence, String[] filteredSentences){
			double sfTitleScore = 0;
			if(sentence.trim().equals(filteredSentences[0].trim())){
				return 0;
			}
			String[] titleWords = filteredSentences[0].split("\\s+");
				for(int k=0 ; k<titleWords.length ; k++){
					if(sentence.contains(titleWords[k])){
						sfTitleScore += 1;
					}
				}
				sfTitleScore /= sentence.split("\\s+").length;
			return sfTitleScore;
		}
		
		
		double thematicWord(String sentence, String para){
				double sfThematicScore = 0;
				String[] splitted = sentence.split("\\s+");
				for(int k=0 ; k<splitted.length ; k++){
					String word = splitted[k];
					int occr = 0;
					Pattern p = Pattern.compile(word);
		            Matcher m = p.matcher( para );
		            while (m.find()) {
		            	occr++;
		            }
		            if(occr > 5){
		            	sfThematicScore += 0.1;
		            }
				}
				return sfThematicScore;
		}
		
		double cosineSimilarity(String sentence, String para, String[] filteredSentences){
			double sfCosineScore=0;
				String[] splitted = sentence.split("\\s+");
				int match = 0;
				for(int j=0 ; j<filteredSentences.length ; j++){
					if(sentence.equals(filteredSentences[j])) continue;
					for(int k=0 ; k<splitted.length ; k++){
						if(filteredSentences[j].indexOf(splitted[k]) != -1){
							match++;
						}
					}
					sfCosineScore += (double)match;
				}
				sfCosineScore /= para.split("\\s+").length;
			
			return sfCosineScore;
		}
		
		double sentencePosition(String curSent, String[] filteredSentences ){
				int pos = 0;
				double sfPositionScore;
				for(int j=0 ; j<filteredSentences.length ; j++){
					if(curSent.equals(filteredSentences[j])){
						pos = j;
					}
				}
				double score = 0;
				if(filteredSentences.length-1 - pos > pos){
					score = filteredSentences.length-1 - pos;
				}
				else score = pos;
				
				sfPositionScore = score / filteredSentences.length ; 
				return sfPositionScore;
			
		}
		
		double propernounCheck(String sentence)
	    {
	        int count,totCount=0,j=0,i;
	        double sfPropernounScore;
	            count = 0;
	            String splitted[] = sentence.split("\\s+");
	            for(i=0 ; i<splitted.length ; i++){
	            	if(splitted[i].substring(0,1).equals(splitted[i].substring(0,1).toUpperCase())){
	            		count++;
	            	}
	            }
	            
	            sfPropernounScore = count;
	            sfPropernounScore /= sentence.length();
	       	    return sfPropernounScore;
	        
	    }
		
		String readFile(Context con) throws IOException{
			String para = "";
			int count = 0;
			Path pt=new Path("hdfs://localhost:9000/refinedText");
			FileSystem fs = FileSystem.get(con.getConfiguration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			try {
			  String line = br.readLine();
			  para = "";
			  while (line != null){
				para += line;
			    line = br.readLine();
			    
			  }
			} finally {
			  // you should close out the BufferedReader
			  br.close();
			}
			return para;
		}
		
		double termWeight(String sentence, String para, String[] filteredSentences)
	    {
	        double sfTermWeightScore;
	            sentence = sentence.trim();
	            String splitted[] = sentence.split("\\s+");
	            sfTermWeightScore = 0;
	            for (int start = 0; start < splitted.length; start++){
	                String word = splitted[start];
	                double sentFreq = 0;
	                int termFreq = 0;

	                Pattern p = Pattern.compile(word);
	                Matcher m = p.matcher( para );
	                while (m.find()) {
	                	termFreq++;
	                }
	                for(int k=0 ; k<filteredSentences.length ; k++)
	                {
	                    if (filteredSentences[k].indexOf(word) != -1)
	                    {
	                        sentFreq++;
	                    }
	                }
	                if (sentFreq != 0)
	                	sfTermWeightScore += termFreq * (Math.log(filteredSentences.length / sentFreq));
	                //System.out.println(sfTermWeightScore[j]);
	            }
	
	        	sfTermWeightScore = sfTermWeightScore / sentence.length();
	        	return sfTermWeightScore;

	    }
	}
	
	
	
	public static class ReduceForWordCount extends Reducer<Text,
	DoubleWritable, Text, DoubleWritable>
	{
		public void reduce(Text sentence, Iterable<DoubleWritable> values, Context
		con) throws IOException, InterruptedException
		{
			double score = 0;
			for(DoubleWritable value : values)
			{
				score += value.get();
			}
			String keySentence = sentence.toString() + " " + "~";
			con.write(new Text(keySentence), new DoubleWritable(score));
		}
	}


}
