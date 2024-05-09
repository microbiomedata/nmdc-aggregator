import os
import requests
from pymongo import MongoClient


class AnnotationLine():
    """
    Object representation built from a GFF annotation line

    The object countains a list of annotations for KEGG,
    COG, EC, and product.  Currently on the KEGG annotations
    are used.
    """

    def __init__(self, line, filter=None):
        self.id = None
        self.kegg = None
        self.cogs = None
        self.product = None
        self.ec_numbers = None

        if line.find("ko=") > 0:
            annotations = line.split("\t")[8].split(";")
            self.id = annotations[0][3:]
            if filter and self.id not in filter:
                return

            for anno in annotations:
                if anno.startswith("ko="):
                    kos = anno[3:].replace("KO:", "KEGG.ORTHOLOGY:")
                    self.kegg = kos.rstrip().split(',')
                elif anno.startswith("cog="):
                    self.cogs = anno[4:].split(',')
                elif anno.startswith("product="):
                    self.product = anno[8:]
                elif anno.startswith("ec_number="):
                    self.ec_numbers = anno[10:].split(",")


class MetaProtAgg():
    """
    MetaP Aggregation class
    """
    _BASE_URL_ENV = "NMDC_BASE_URL"
    _base_url = "https://data.microbiomedata.org/data"
    _BASE_PATH_ENV = "NMDC_BASE_PATH"
    _base_dir = "/global/cfs/cdirs/m3408/results"

    def __init__(self):
        url = os.environ["MONGO_URL"]
        client = MongoClient(url, directConnection=True)
        self.db = client.nmdc
        self.agg_col = self.db.metap_gene_function_aggregation
        self.act_col = self.db.metaproteomics_analysis_activity_set
        self.do_col = self.db.data_object_set
        self.base_url = os.environ.get(self._BASE_URL_ENV, self._base_url)
        self.base_dir = os.environ.get(self._BASE_PATH_ENV, self._base_dir)

    def get_kegg_terms(self, url, gene_list):
        kos = {}
        fn = url.replace(self.base_url, self.base_dir)

        if os.path.exists(fn):
            lines = open(fn)
        else:
            s = requests.Session()
            resp = s.get(url, headers=None, stream=True)
            if not resp.ok:
                raise OSError(f"Failed to read {url}")
            lines = resp.iter_lines()

        for line in lines:
            if isinstance(line, bytes):
                line = line.decode()
            anno = AnnotationLine(line, gene_list)
            if anno.kegg:
                kos[anno.id] = anno.kegg
        return kos

    def find_anno(self, dos):
        """
        Find the GFF annotation URL
        We use the protein file to get the base part of the URL.
        input: list of data object IDs
        returns: GFF functional annotation URL
        """
        url = None
        for doid in dos:
            do = self.do_col.find_one({"id": doid})
            # skip over bad records
            if not do or 'data_object_type' not in do:
                continue
            if do['data_object_type'] == 'Functional Annotation GFF':
                url = do['url']
                return url
            elif do['data_object_type'] == 'Annotation Amino Acid FASTA':
                url = do['url']
                return url.replace("_proteins.faa", "_functional_annotation.gff")
        return None

    def process_activity(self, act):
        """
        Function to process an activity record.
        Input: activity record
        Output: Dictonary of KEGG records

        This currently relies on the has_peptide_quantificiations
        records in the activity record.  This may change in the future. 
        """
        # Get the URL and ID
        url = self.find_anno(act['has_input'])
        if not url:
            raise ValueError(f"Missing url for {act['id']}")
        url_id = url.split('/')[-1].replace("nmdc_", "nmdc:").split('_')[0]
        id_list = set()
        # Get the filter list
        for pep in act['has_peptide_quantifications']:
            # This check is because some activities have
            # bogus peptides
            if len(pep['all_proteins']) > 0:
                mid = pep['all_proteins'][0].split('_')[0]
                if not mid.startswith(url_id):
                    continue
            id_list.update(pep['all_proteins'])
        # get the KEGG protein terms from the annotation file
        proteins = self.get_kegg_terms(url, id_list)
        kegg_recs = {}
        for pep in act['has_peptide_quantifications']:
            # Loop through all proteins
            for prot in pep['all_proteins']:
                # ignore any proteins that don't have KEGG
                # annotations
                if prot not in proteins:
                    continue
                kos = proteins[prot]
                # Loop through the KEGG annotations for the protein
                for ko in kos:
                    # Initialize the record
                    if ko not in kegg_recs:
                        new_rec = {"metaproteomic_analysis_id": act['id'],
                                   "gene_function_id": ko,
                                   "count": 0,
                                   "best_protein": False}
                        kegg_recs[ko] = new_rec
                    # increment the count
                    kegg_recs[ko]["count"] += 1
                    # If there is a best_protein then set the flag to true
                    if prot == pep['best_protein']:
                        kegg_recs[ko]["best_protein"] = True
        return kegg_recs

    def sweep(self):
        """
        This is the main action function. This goes through all
        of the metaP activity records and finds any that are missing
        aggregations.
        """
        print("Getting list of indexed objects")
        done = self.agg_col.distinct("metaproteomic_analysis_id")

        # Iterate through all of the metaP activities
        for actrec in self.act_col.find({}):
            # New annotations should have this
            act_id = actrec['id']
            # Skip if already processed
            if act_id in done:
                continue
            try:
                rows = self.process_activity(actrec)
            except Exception as ex:
                # Continue on errors
                print(ex)
                continue
            if len(rows) > 0:
                print(f"Inserting: {len(rows)} rows for {act_id}")
                self.agg_col.insert_many(rows)
            else:
                print(f' - No rows for {act_id}')


if __name__ == "__main__":
    mp = MetaProtAgg()
    mp.sweep()
