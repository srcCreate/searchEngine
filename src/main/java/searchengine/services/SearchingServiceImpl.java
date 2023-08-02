package searchengine.services;

import org.springframework.stereotype.Service;
import searchengine.dto.indexing.Lemmatizator;
import searchengine.dto.search.SearchingResponse;

import java.util.Map;

@Service
public class SearchingServiceImpl implements SearchingService {

    private Lemmatizator lemmatizator;
    @Override
    public SearchingResponse search(String query) {
        SearchingResponse response = new SearchingResponse();
        Map<String, Integer> lemmasFromQuery = lemmatizator.collectLemmas(query);

        return null;
    }
}
