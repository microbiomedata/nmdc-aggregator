import requests
import csv
import io
import os
from abc import ABC, abstractmethod
import logging

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class Aggregator(ABC):
    """
    Abstract class for Aggregators


    Attributes
    ----------
    base_url : str
        Base URL for the API, either production or development
        "https://api.microbiomedata.org" or "https://api-dev.microbiomedata.org"
    nmdc_api_token : str
        API bearer token to access the API
    aggregation_filter : str
        Filter to apply to the aggregation collection endpoint to get applicable records (set in subclasses)
        Note the use of the ^ character to match the beginning of the string which optimizes the query
        e.g. '{"was_generated_by":{"$regex":"^nmdc:wfmp"}}'
    workflow_filter : str
        Filter to apply to the workflow collection endpoint to get applicable records (set in subclasses)
        e.g. '{"type":"nmdc:MetaproteomicsAnalysis"}'
    """

    # Set the base URL for the API
    _NMDC_API_URL = "https://api-dev.microbiomedata.org"

    def __init__(self):
        self.base_url = os.getenv("NMDC_API_URL") or self._NMDC_API_URL
        self.get_bearer_token()

        # The following attributes are set in the subclasses
        self.aggregation_filter = ""
        self.workflow_filter = ""

    def get_bearer_token(self):
        """Function to get the bearer token from the API using the /token endpoint

        Parameters
        ----------
        None, but uses the NMDC_CLIENT_ID and NMDC_CLIENT_PW environment variables to get the token and set the nmdc_api_token attribute
        """
        token_request_body = {
            "grant_type": "client_credentials",
            "client_id": os.getenv("NMDC_CLIENT_ID"),
            "client_secret": os.getenv("NMDC_CLIENT_PW"),
        }

        rv = requests.post(self.base_url + "/token", data=token_request_body)
        token_response = rv.json()
        if "access_token" not in token_response:
            logger.error(
                f"Getting token failed: {token_response}, Status code: {rv.status_code}"
            )
            raise Exception(
                f"Getting token failed: {token_response}, Status code: {rv.status_code}"
            )
        self.nmdc_api_token = token_response["access_token"]

    def get_results(self, collection: str, filter="", max_page_size=100, fields=""):
        """General function to get results from the API using the collection endpoint with optional filter and fields

        Parameters
        ----------
        collection : str
            Collection name to query, e.g. "functional_annotation_agg"
        filter : str, optional
            Filter to apply to the query written in JSON format for MongoDB
            e.g. '{"was_generated_by":{"$regex":"wfmp"}}'
            Default is an empty string, which does not apply a filter
        max_page_size : int, optional
            Maximum number of records to return in a single page
            Default is 100
        fields : str, optional
            Fields to return in the query, separated by commas without spaces if multiple
            e.g. "id,data_object_type,url"
            Default is an empty string, which returns all fields
        """

        # Get initial results (before next_page_token is given in the results)
        result_list = []
        og_url = f"{self.base_url}/nmdcschema/{collection}?&filter={filter}&max_page_size={max_page_size}&projection={fields}"
        resp = requests.get(og_url)
        initial_data = resp.json()
        results = initial_data.get("resources", [])
        i = 0

        if results == []:
            # if no results are returned
            return result_list

        # append first page of results to an empty list
        for result in results:
            result_list.append(result)

        # if there are multiple pages of results returned
        if initial_data.get("next_page_token"):
            next_page_token = initial_data["next_page_token"]

            while True:
                i = i + max_page_size
                url = f"{self.base_url}/nmdcschema/{collection}?&filter={filter}&max_page_size={max_page_size}&page_token={next_page_token}&projection={fields}"
                response = requests.get(url)
                data_next = response.json()

                results = data_next.get("resources", [])
                result_list.extend(results)
                next_page_token = data_next.get("next_page_token")

                if not next_page_token:
                    break

        return result_list

    def get_previously_aggregated_workflow_ids(self):
        """Function to return all ids of workflow execution ids that have already been aggregated.

        Uses the aggregation_filter attribute to filter the results for subclasses.

        Returns
        -------
        list
            List of workflow ids that have already been aggregated
        """
        agg_col = self.get_results(
            collection="functional_annotation_agg",
            filter=self.aggregation_filter,
            max_page_size=10000,
            fields="was_generated_by",
        )
        ids = list(set([x["was_generated_by"] for x in agg_col]))
        return ids

    def get_workflow_records(self):
        """Function to return full workflow execution records in the database

        Returns
        -------
        list of dict
            List of workflow execution records, each represented as a dictionary
        """
        act_col = self.get_results(
            collection="workflow_execution_set",
            filter=self.workflow_filter,
            max_page_size=500,
            fields="",
        )
        return act_col

    def submit_json_records(self, json_records):
        """Function to submit records to the database using the post /metadata/json:submit endpoint

        Parameters
        ----------
        json_records : list
            List of dictionaries where each dictionary represents a record to be submitted to the database

        Returns
        -------
        int
            HTTP status code of the response
        """
        url = f"{self.base_url}/metadata/json:submit"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.nmdc_api_token}",
            "Content-Type": "application/json",
        }

        response = requests.post(url, headers=headers, json=json_records)

        return response.status_code

    def read_url_tsv(self, url):
        """Function to read a TSV file's content from a URL and convert it to a list of dictionaries

        Parameters
        ----------
        url : str
            URL to the TSV file

        Returns
        -------
        list
            List of dictionaries where each dictionary represents a row in the TSV file
        """
        response = requests.get(url)

        # Read the TSV content
        tsv_content = response.content.decode("utf-8")

        # Use csv.reader to parse the TSV content
        tsv_reader = csv.reader(io.StringIO(tsv_content), delimiter="\t")

        # Convert the TSV content to a list of dictionaries
        tsv_data = []
        headers = next(tsv_reader)  # Get the headers from the first row
        for row in tsv_reader:
            tsv_data.append(dict(zip(headers, row)))

        return tsv_data

    def sweep(self):
        """This is the main action function for the Aggregator class.

        It performs the following steps:
        1. Get list of workflow IDs that have already been added to the functional_annotation_agg collection
        2. Get list of all applicable workflow in the database, as defined by the workflow_filter attribute
        3. For each workflow that is not in the list of previously aggregated records:
            a. Process the activity according to the process_activity method in the subclass
            b. Prepare a json record for the database with the annotations and counts
            c. Submit json to the database using the post /metadata/json endpoint

        Returns
        -------
        None
        """
        # Get list of workflow IDs that have already been processed
        mp_wf_in_agg = self.get_previously_aggregated_workflow_ids()

        # Get list of all workflow records
        mp_wf_recs = self.get_workflow_records()

        # Iterate through all of the workflow records
        for mp_wf_rec in mp_wf_recs:
            if mp_wf_rec["id"] in mp_wf_in_agg:
                continue
            try:
                functional_agg_dict = self.process_activity(mp_wf_rec)
            except Exception as ex:
                # Log the error and continue to the next record
                logger.error(f"Error processing activity {mp_wf_rec['id']}: {ex}")
                continue

            # Prepare a  json record for the database
            json_records = []
            for k, v in functional_agg_dict.items():
                json_records.append(
                    {
                        "was_generated_by": mp_wf_rec["id"],
                        "gene_function_id": k,
                        "count": v,
                        "type": "nmdc:FunctionalAnnotationAggMember",
                    }
                )
            json_record_full = {"functional_annotation_agg": json_records}

            response = self.submit_json_records(json_record_full)
            if response != 200:
                logger.error(
                    f"Error submitting the aggregation records for the workflow: {mp_wf_rec['id']}, Response code: {response}"
                )
            if response == 200:
                print(
                    "Submitted aggregation records for the workflow: ", mp_wf_rec["id"]
                )

    def sweep_success(self):
        """Function to check the results of the sweep and ensure that the records were added to the database

        Returns
        -------
        bool
            True if all records were added to the functional_annotation_agg collection, False otherwise
        """
        # Get list of workflow IDs that have already been processed
        mp_wf_in_agg = self.get_previously_aggregated_workflow_ids()

        # Get list of all workflow records
        mp_wf_recs = self.get_workflow_records()

        # If there are any records that were not processed, return FALSE
        check = [x["id"] in mp_wf_in_agg for x in mp_wf_recs]
        if all(check):
            return True
        else:
            return False

    @abstractmethod
    def process_activity(self, act):
        """
        Abstract method to process an activity record.  This method must be implemented in the subclass.

        Parameters
        ----------
        act : dict
            Activity record to process

        Returns
        -------
        dict
            Dictionary of functional annotations with their respective counts
        """
        pass


class MetaProtAgg(Aggregator):
    """
    Metaproteomics Aggregator class

    Parameters
    ----------
    dev : bool
        Flag to indicate if production or development API should be used
        Default is True, which uses the development API

    Notes
    -----
    This class is used to aggregate functional annotations from metaproteomics workflows in the NMDC database.
    """

    def __init__(self):
        super().__init__()
        self.aggregation_filter = '{"was_generated_by":{"$regex":"^nmdc:wfmp"}}'
        self.workflow_filter = '{"type":"nmdc:MetaproteomicsAnalysis"}'

    def get_functional_terms_from_protein_report(self, url):
        """Function to get the functional terms from a URL of a Protein Report

        Parameters
        ----------
        url : str
            URL to the Protein Report

        Returns
        -------
        dict
            Dictionary of KEGG, COG, and PFAM terms with their respective spectral counts derived from the Protein Report
        """
        fxns = {}

        content = self.read_url_tsv(url)

        # Parse the Protein Report content into KO, COG, and Pfam terms
        for line in content:
            # Add ko terms to the dictionary
            ko = line.get("KO")
            if ko != "" and ko is not None:
                # Replace KO: with KEGG.ORTHOLOGY:
                ko_clean = ko.replace("KO:", "KEGG.ORTHOLOGY:")
                if ko_clean not in fxns.keys():
                    fxns[ko_clean] = int(float(line.get("SummedSpectraCounts")))
                else:
                    fxns[ko_clean] += int(float(line.get("SummedSpectraCounts")))

            # Add cog terms to the dictionary
            cog = line.get("COG")
            if cog != "" and cog is not None:
                cog_clean = "COG:" + cog
                if cog_clean not in fxns.keys():
                    fxns[cog_clean] = int(float(line.get("SummedSpectraCounts")))
                else:
                    fxns[cog_clean] += int(float(line.get("SummedSpectraCounts")))

            # Add pfam terms to the dictionary
            pfam = line.get("pfam")
            if pfam != "" and pfam is not None:
                pfam_clean = "PFAM:" + pfam
                if pfam_clean not in fxns.keys():
                    fxns[pfam_clean] = int(float(line.get("SummedSpectraCounts")))
                else:
                    fxns[pfam_clean] += int(float(line.get("SummedSpectraCounts")))

        # For all, loop through keys and separate into multiple keys if there are multiple pfams
        new_fxns = {}
        for k, v in fxns.items():
            if "," in k:
                for pfam in k.split(","):
                    # Check if pfam is already "PFAM:" prefixed
                    if not pfam.startswith("PFAM:"):
                        pfam = "PFAM:" + pfam
                    if pfam not in new_fxns.keys():
                        new_fxns[pfam] = v
                    else:
                        new_fxns[pfam] += v
            else:
                new_fxns[k] = v

        return new_fxns

    def find_protein_report_url(self, dos):
        """Find the URL for the protein report from a list of data object IDs

        Parameters
        ----------
        dos : list
            List of data object IDs

        Returns
        -------
        str
            URL for the Protein Report data object if found
        """
        url = None

        # Get all the data object records
        id_filter = '{"id": {"$in": ["' + '","'.join(dos) + '"]}}'
        do_recs = self.get_results(
            collection="data_object_set",
            filter=id_filter,
            max_page_size=1000,
            fields="id,data_object_type,url",
        )

        # Find the Protein Report data object and return the URL to access it
        for do in do_recs:
            if do.get("data_object_type") == "Protein Report":
                url = do.get("url")
                return url

        # If no Protein Report data object is found, return None
        return None

    def process_activity(self, act):
        """
        Function to process a metaproteomics workflow record

        Parameters
        ----------
        act : dict
            Metaproteomics workflow record to process

        Output
        ------
        dict
            Dictionary of functional annotations with their respective spectral counts
            e.g. {"KEGG.ORTHOLOGY:K00001": 100, "COG:C00001": 50, "PFAM:PF00001": 25}
        """
        # Get the URL and ID
        url = self.find_protein_report_url(act["has_output"])
        if not url:
            raise ValueError(f"Missing url for {act['id']}")

        # Parse the KEGG, COG, and PFAM annotations
        return self.get_functional_terms_from_protein_report(url)


if __name__ == "__main__":
    mp = MetaProtAgg()
    mp.sweep()
