package searchengine.services;

import java.util.Map;

public interface IndexingPageService {
    Map<String, String> indexPage(String url);
}
