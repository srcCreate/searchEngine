package searchengine.dto.search;

import lombok.Data;

@Data
public class SearchingResponse {
    private boolean result;
    private SearchingData searchingData;
}
