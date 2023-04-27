package searchengine.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Getter
@Setter
@Entity(name = "page")

public class PageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    //Без cascade работало
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    private SiteEntity siteId;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String path;

    @Column(columnDefinition = "INT", nullable = false)
    private int code;

    @Column(name = "content", nullable = false,
            columnDefinition = "mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    private String content;
}
