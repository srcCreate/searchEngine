package searchengine.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.Set;

@Setter
@Getter
@Entity(name = "lemma")
public class LemmaEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    private SiteEntity siteId;

    @OneToMany(mappedBy = "lemmaId", cascade = CascadeType.MERGE, orphanRemoval = true)
    @Column(name = "indexes")
    private Set<IndexEntity> indexes;

    @Column(nullable = false)
    private String lemma;

    @Column(nullable = false)
    private int frequency;
}
