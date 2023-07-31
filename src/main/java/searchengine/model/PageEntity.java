package searchengine.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.Set;

@Getter
@Setter
@Entity(name = "page")

public class PageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    private SiteEntity siteId;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String path;

    @Column(columnDefinition = "INT", nullable = false)
    private int code;

    @Column(name = "content", nullable = false,
            columnDefinition = "mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    private String content;

    @OneToMany(mappedBy = "pageId", cascade = CascadeType.MERGE, orphanRemoval = true)
    @Column(name = "indexes")
    private Set<IndexEntity> indexes;
}
