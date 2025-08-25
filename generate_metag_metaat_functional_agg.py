import os
import requests
from aggregator import Aggregator

class AnnotationLine():
    """
    Class to represent a line of functional annotation from a GFF report. 
    The GFF reports are not always well-structured, and this class aims to extract relevant information from them.
    """

    def __init__(self, line, filter=None):
        self.id = None
        self.kegg = None
        self.cogs = None
        self.product = None
        self.ec_numbers = None
        self.pfams = None

        annotations = line.split("\t")[8].split(";")
        self.id = annotations[0][3:]
        if filter and self.id not in filter:
            return

        for anno in annotations:
            if anno.startswith("ko="):
                processed_kos = anno[3:].replace("KO:", "KEGG.ORTHOLOGY:")
                self.kegg = [ko.strip() for ko in processed_kos.split(',')]
            elif anno.startswith("cog="):
                self.cogs = ['COG:' + cog_id.strip() for cog_id in anno[4:].split(',')]
            elif anno.startswith("product="):
                self.product = anno[8:]
            elif anno.startswith("ec_number="):
                self.ec_numbers = anno[10:].split(",")
            elif anno.startswith("pfam="):
                self.pfams = ['PFAM:' + pfam_id.strip() for pfam_id in anno[5:].split(",")]


class MetaGMetaTFuncAgg(Aggregator):
    """
    Metagenome and Metatranscriptome Aggregator class

    Notes
    -----
    This class is used to aggregate functional annotations from metagenomics workflows in the NMDC database.
    """

    def __init__(self):
        super().__init__()
        self.aggregation_filter= '{"$or": [{"was_generated_by": {"$regex": "^nmdc:wfmgan"}},{"was_generated_by": {"$regex": "^nmdc:wfmtan"}}]}'
        self.workflow_filter = '{"$or": [{"type":"nmdc:MetagenomeAnnotation"},{"type":"nmdc:MetatranscriptomeAnnotation"}]}'

    def get_functional_annotation_counts_from_gff_report(self, url:str) -> dict:
        """
        Function to get functional annotation counts from the URL of a GFF report.
        Utilizes the AnnotationLine class to parse the GFF report and extract functional annotations.

        Parameters
        ----------
        url : str
            URL to the GFF Report

        Returns
        -------
        dict
            Dictionary of KEGG, COG, and PFAM terms with their respective counts derived from the GFF Report
        """

        s = requests.Session()
        resp = s.get(url, headers=None, stream=True)
        if not resp.ok:
            raise OSError(f"Failed to read {url}")
        lines = resp.iter_lines()

        func_count = {}
        for line in lines:
            if isinstance(line, bytes):
                line = line.decode()
            anno = AnnotationLine(line)
            if anno.kegg:
                for ko in anno.kegg:
                    if ko not in func_count:
                        func_count[ko] = 0
                    func_count[ko] += 1
            if anno.cogs:
                for cog in anno.cogs:
                    if cog not in func_count:
                        func_count[cog] = 0
                    func_count[cog] += 1
            if anno.pfams:
                for pfam in anno.pfams:
                    if pfam not in func_count:
                        func_count[pfam] = 0
                    func_count[pfam] += 1
        return func_count

    def find_gff_annotation_url(self, data_object_ids:list[str]) -> str:
        """
        Find the GFF annotation URL

        Parameters
        ----------
        data_object_ids : list[str]
            List of data object IDs

        Returns
        -------
        str
            GFF functional annotation URL
        """
        id_filter = '{"id": {"$in": ["' + '","'.join(data_object_ids) + '"]}}'
        do_recs = self.get_results(
            collection="data_object_set",
            filter=id_filter,
            max_page_size=1000,
            fields="id,data_object_type,url",
        )
        for do in do_recs:
            if do.get('data_object_type') == 'Functional Annotation GFF':
                return do.get("url")
            
        # if no GFF is found, return None
        return None
    
    def process_activity(self, act):
        """
        Function to process a metagenome workflow record

        Parameters
        ----------
        act : dict
            Metagenome workflow record to process

        Output
        ------
        dict
            Dictionary of functional annotations with their respective spectral counts
            e.g. {"KEGG.ORTHOLOGY:K00001": 100, "COG:C00001": 50, "PFAM:PF00001": 25}
        """
        # Get the URL and ID
        url = self.find_gff_annotation_url(act["has_output"])
        if not url:
            raise ValueError(f"Missing url for {act['id']}")

        # Parse the KEGG, COG, and PFAM annotations
        return self.get_functional_annotation_counts_from_gff_report(url)


if __name__ == "__main__":
    mg = MetaGMetaTFuncAgg()
    mg.sweep()
    
