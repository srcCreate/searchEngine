package searchengine.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Setter
@Getter
@Entity(name = "index_table")
public class IndexEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    private PageEntity pageId;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    private LemmaEntity lemmaId;

    @Column(nullable = false)
    private float rankValue;
}
