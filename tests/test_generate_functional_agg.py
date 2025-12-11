from generate_metag_metat_functional_agg import AnnotationLine
from generate_metag_metat_functional_agg import MetaGMetaTFuncAgg
from generate_metap_functional_agg import MetaProtAgg


def test_AnnotationLine():
    line = "nmdc:wfmtan-11-5rqhd817.1_0000001	Prodigal v2.6.3_patched	CDS	2931	5588	340.0	+	0	ID=nmdc:wfmgan-11-5rqhd817.1_0000001_2931_5588;translation_table=11;start_type=ATG;product=O-antigen biosynthesis protein;product_source=KO:K20444;cath_funfam=3.20.20.80,3.90.550.10;cog=COG0463;ko=KO:K20444;ec_number=EC:2.4.1.-;pfam=PF00535,PF02836;superfamily=51445,53448"
    anno = AnnotationLine(line)
    assert anno
    assert len(anno.kegg) > 0
    assert anno.id == "nmdc:wfmgan-11-5rqhd817.1_0000001_2931_5588"
    assert anno.cogs == ["COG:COG0463"]
    assert anno.kegg == ["KEGG.ORTHOLOGY:K20444"]
    assert anno.pfams == ["PFAM:PF00535", "PFAM:PF02836"]


def test_functional_annotation_counts():
    mp = MetaGMetaTFuncAgg()
    url = "https://portal.nersc.gov/cfs/m3408/test_data/metaT/functional_annotation.gff"
    terms = mp.get_functional_annotation_counts_from_gff_report(url)
    assert len(terms) == 2647
    assert terms["KEGG.ORTHOLOGY:K00031"] == 1
    assert terms["COG:COG0004"] == 3
    assert terms["PFAM:PF00206"] == 2

def test_functional_annotation_counts_metaproteomics():
    mp = MetaProtAgg()
    url = "https://nmdcdemo.emsl.pnnl.gov/proteomics/results/2/nmdc_dobj-11-9gcej008_nmdc_dobj-11-j5mh8584_Peptide_Report.tsv"
    terms = mp.get_functional_terms_from_peptide_report(url)
    assert len(terms) == 1943
    assert terms["KEGG.ORTHOLOGY:K00031"] == 8
    assert terms["COG:COG0004"] == 1

def test_check_for_aggregation_records():
    mp = MetaProtAgg()
    assert mp.check_for_aggregation_records("nmdc:wfmp-11-yafgh176.1")
    assert not mp.check_for_aggregation_records("nonexistent_wf_id")