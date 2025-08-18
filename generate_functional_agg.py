import os
import requests
from pymongo import MongoClient
from signal import signal, SIGINT
from aggregator import Aggregator

stop = False


def make_functional_annotation_agg_member(
    was_generated_by: str,
    gene_function_id: str,
    count: int,
) -> dict:
    r"""
    Returns a dictionary representing an instance of the `FunctionalAnnotationAggMember`
    class defined in the NMDC Schema (as of `nmdc-version` version `11.10.0`).
    Docs: https://microbiomedata.github.io/nmdc-schema/FunctionalAnnotationAggMember/

    >>> make_functional_annotation_agg_member("wgb", "gfi", 123)
    {'was_generated_by': 'wgb', 'gene_function_id': 'gfi', 'count': 123, 'type': 'nmdc:FunctionalAnnotationAggMember'}
    """
    return {
        "was_generated_by": was_generated_by,
        "gene_function_id": gene_function_id,
        "count": count,
        "type": "nmdc:FunctionalAnnotationAggMember",
    }


def sig_handler(signalnumber, frame):
    global stop
    stop = True


class AnnotationLine():

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


class MetaGenomeFuncAgg(Aggregator):
    _BASE_URL_ENV = "NMDC_BASE_URL"
    _base_url = "https://data.microbiomedata.org/data"
    _BASE_PATH_ENV = "NMDC_BASE_PATH"
    _base_dir = "/global/cfs/cdirs/m3408/results"

    def __init__(self):
        self.base_url = os.getenv("NMDC_API_URL") or self._NMDC_API_URL
        self.get_bearer_token()

        # The following attributes are set in the subclasses
        self.aggregation_filter = ""
        self.workflow_filter = ""

        url = os.environ["MONGO_URL"]
        client = MongoClient(url, directConnection=True)
        self.db = client.nmdc
        self.agg_col = self.db.functional_annotation_agg
        self.do_col = self.db.data_object_set
        self.base_url = os.environ.get(self._BASE_URL_ENV, self._base_url)
        self.base_dir = os.environ.get(self._BASE_PATH_ENV, self._base_dir)

    def get_functional_annotation_counts(self, url):
        fn = url.replace(self.base_url, self.base_dir)

        if os.path.exists(fn):
            lines = open(fn)
        else:
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

    def get_functional_terms_from_gff_report(self, url):
        """Function to get the functional terms from a URL of GFF

        Parameters
        ----------
        url : str
            URL to the GFF Report

        Returns
        -------
        dict
            Dictionary of KEGG, COG, and PFAM terms with their respective spectral counts derived from the GFF Report
        """
        # Download the GFF file
        gff_data = requests.get(url).text

        # Parse the GFF file
        functional_terms = {
            "KEGG": {},
            "COG": {},
            "PFAM": {}
        }
        for line in gff_data.splitlines():
            if line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 9:
                continue
            feature = fields[2]
            attributes = fields[8]
            if feature == "gene":
                for term in attributes.split(";"):
                    if term.startswith("KEGG"):
                        functional_terms["KEGG"][term] = functional_terms["KEGG"].get(term, 0) + 1
                    elif term.startswith("COG"):
                        functional_terms["COG"][term] = functional_terms["COG"].get(term, 0) + 1
                    elif term.startswith("PFAM"):
                        functional_terms["PFAM"][term] = functional_terms["PFAM"].get(term, 0) + 1
        return functional_terms
    
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
        return self.get_functional_terms_from_peptide_report(url)


if __name__ == "__main__":
    signal(SIGINT, sig_handler)
    mg = MetaGenomeFuncAgg()
    mg.sweep()