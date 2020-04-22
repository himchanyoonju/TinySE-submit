package edu.hanyang.submit;

import java.util.List;
import edu.hanyang.indexer.Tokenizer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.tartarus.snowball.ext.PorterStemmer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TinySETokenizer implements Tokenizer {
	SimpleAnalyzer analyzer;
	PorterStemmer stemmer;
	List<String> output = new ArrayList<String>(); 

	public void setup() {
		analyzer = new SimpleAnalyzer();
		stemmer = new PorterStemmer();
	}

	public List<String> split(String text) {
		
		output.clear();
		
		try {
			
			TokenStream stream = analyzer.tokenStream(null, new StringReader(text));
			stream.reset();
			CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
			
			while (stream.incrementToken()) {
				String temp = term.toString();
				stemmer.setCurrent(term.toString());
				stemmer.stem();
				String input = stemmer.getCurrent();
				output.add(input);
			}
			stream.close();
		}catch (IOException e) {
			
			throw new RuntimeException(e);
			
		}
		System.out.print(output);
		System.out.print("");
		return output;
	}

	public void clean() {
		analyzer.close();
	}

}