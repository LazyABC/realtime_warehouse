package com.lazy.realtime.dws.traffic.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @Name: Lazy
 * @Date: 2024/1/6 15:51:58
 * @Details:  从给定的输入字符串中分割单词，并将每个单词作为表中的一个独立行进行输出
 */
public class IKUtil {

    //只能切词,没有NLP(自然语言处理)功能
    public static void main(String[] args) {
        System.out.println(splitWord("你好", false));
    }

    public static Set<String> splitWord(String str , boolean useSmart){

        Set<String> words = new HashSet<>();
        /*
             IKSegmenter : 切词器的核心api
            IKSegmenter(
                Reader input,  把字符串变为字符流
                boolean useSmart, 是否是智能切词，
                        true，智能切词
                        false, 最大化切词(切出很 无语的词 )
                 )
         */
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(str), useSmart);

        try {
            Lexeme lexeme = ikSegmenter.next();
            while (lexeme != null ){
                String lexemeText = lexeme.getLexemeText();
                //把切的内容加入到集合
                words.add(lexemeText);
                //继续切词
                lexeme = ikSegmenter.next();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return words;

    }
}
