package searchengine.dto.indexing;

import java.util.Objects;

public class PageUrl {
    private final String url;

    public PageUrl(String url) {
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageUrl pageUrl = (PageUrl) o;
        return Objects.equals(url, pageUrl.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url);
    }
}
