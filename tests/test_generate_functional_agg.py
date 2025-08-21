from generate_metag_functional_agg import AnnotationLine
from generate_metag_functional_agg import MetaGenomeFuncAgg


def test_AnnotationLine():
    line = "nmdc:wfmtan-11-5rqhd817.1_0000001	Prodigal v2.6.3_patched	CDS	2931	5588	340.0	+	0	ID=nmdc:wfmgan-11-5rqhd817.1_0000001_2931_5588;translation_table=11;start_type=ATG;product=O-antigen biosynthesis protein;product_source=KO:K20444;cath_funfam=3.20.20.80,3.90.550.10;cog=COG0463;ko=KO:K20444;ec_number=EC:2.4.1.-;pfam=PF00535,PF02836;superfamily=51445,53448"
    anno = AnnotationLine(line)
    assert anno
    assert len(anno.kegg) > 0
    assert anno.id == "nmdc:wfmgan-11-5rqhd817.1_0000001_2931_5588"
    assert anno.cogs == ["COG:COG0463"]
    assert anno.kegg == ["KEGG.ORTHOLOGY:K20444"]
    assert anno.pfams == ["PFAM:PF00535", "PFAM:PF02836"]


def test_functional_annotation_counts(monkeypatch):
    monkeypatch.setenv("MONGO_URL", "mongodb://db")
    mp = MetaGenomeFuncAgg()
    url = "https://portal.nersc.gov/cfs/m3408/test_data/metaT/functional_annotation.gff"
    terms = mp.get_functional_annotation_counts(url)
    assert len(terms) == 1965
    assert terms["KEGG.ORTHOLOGY:K00031"] == 1
    assert terms["COG:COG0004"] == 3
    assert terms["PFAM:PF00206"] == 2
