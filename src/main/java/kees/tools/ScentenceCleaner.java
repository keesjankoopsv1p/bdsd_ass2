package kees.tools;

import java.util.Locale;

public final class ScentenceCleaner {

    private ScentenceCleaner() {

    }

    public static String cleanSentence(String originalSentence) {
        originalSentence = originalSentence.toLowerCase(Locale.ROOT);
        String cleanSentence = originalSentence.replaceAll("[^a-z ]", "*");
        return cleanSentence;
    }
}
