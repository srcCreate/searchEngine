package searchengine.dto.search;

import lombok.Data;

@Data
public class SearchingResponse {
    private boolean result;
    private int count;
    private SearchingData data;
}
