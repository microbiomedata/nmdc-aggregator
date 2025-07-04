import os
import requests
from pymongo import MongoClient
from signal import signal, SIGINT


stop = False


def make_functional_annotation_agg_member(
    was_generated_by: str,
    gene_function_id: str,
    count: int,
) -> dict:
    r"""
    Returns a dictionary representing an instance of the `FunctionalAnnotationAggMember`
    class defined in the NMDC Schema (as of `nmdc-version` version `11.7.0`).
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
                    self.cogs = ['COG:' + cog_id for cog_id in anno[4:].split(',')]
                elif anno.startswith("product="):
                    self.product = anno[8:]
                elif anno.startswith("ec_number="):
                    self.ec_numbers = anno[10:].split(",")
                elif anno.startswith("pfam="):
                    self.pfams = ['PFAM:' + pfam_id for pfam_id in anno[5:].split(",")]


class MetaGenomeFuncAgg():
    _BASE_URL_ENV = "NMDC_BASE_URL"
    _base_url = "https://data.microbiomedata.org/data"
    _BASE_PATH_ENV = "NMDC_BASE_PATH"
    _base_dir = "/global/cfs/cdirs/m3408/results"

    def __init__(self):
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

    def find_anno(self, dos):
        """
        Find the GFF annotation URL
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
                break
        return url

    def process_workflow_execution(self, execution_record):
        url = self.find_anno(execution_record['has_output'])
        if not url:
            raise ValueError("Missing url")
        print(f"{execution_record['id']}: {url}")
        id = execution_record['id']
        cts = self.get_functional_annotation_counts(url)

        rows = []
        for func, ct in cts.items():
            rec = make_functional_annotation_agg_member(
                was_generated_by=id,
                gene_function_id=func,
                count=ct,
            )
            rows.append(rec)
        print(f' - {len(rows)} terms')
        return rows

    def sweep(self):
        print("Getting list of indexed objects")
        done = self.agg_col.distinct("was_generated_by")
        q = {"type": {
            "$in": ["nmdc:MetagenomeAnnotation", "nmdc:MetatranscriptomeAnnotation"]
        }}
        execution_records = self.db.workflow_execution_set.find(q)
        for execution_record in execution_records:
            if execution_record['id'] in done:
                continue
            try:
                rows = self.process_workflow_execution(execution_record)
            except Exception as ex:
                # Continue on errors
                print(ex)
                continue
            if len(rows) > 0:
                print(' - %s' % (str(rows[0])))
                self.agg_col.insert_many(rows)
            else:
                print(f' - No rows for {execution_record["id"]}')
            if stop:
                print("quiting")
                break


if __name__ == "__main__":
    signal(SIGINT, sig_handler)
    mg = MetaGenomeFuncAgg()
    mg.sweep()


# Schema
#
#        was_generated_by        |   gene_function_id    | count
# ---------------------------------------+-----------------------+-------
#  nmdc:006424afe19af3c36c50e2b2e68b9510 | KEGG.ORTHOLOGY:K00001 |   145
