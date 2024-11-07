import requests
import csv
import io
import os
import time

# TODO KRH: Change metagmetagenome_anlaysis_id to was_generated_by throughout after https://github.com/microbiomedata/nmdc-schema/pull/2203 has been merged

class MetaProtAgg:
    """
    MetaP Aggregation class

    Parameters
    ----------
    dev : bool
        Flag to indicate if production or development API should be used
        Default is True, which uses the development API

    Attributes
    ----------
    base_url : str
        Base URL for the API
    nmdc_api_token : str
        API token to access the API
    
    Notes
    -----
    This class is used to aggregate functional annotations from metaproteomics activities in the NMDC database.
    There must be an environment variable called NMDC_API_TOKEN that contains the API token to access the API.
    """

    def __init__(self, dev=True):
        if dev:
            self.base_url = "https://api-dev.microbiomedata.org"
            self.nmdc_api_token = os.getenv("NMDC_API_DEV_BEARER_TOKEN")
        else:
            self.base_url = "https://api.microbiomedata.org"
            self.nmdc_api_token = os.getenv("NMDC_API_BEARER_TOKEN")
        
    def get_results(
        self, collection: str, filter="", max_page_size=100, fields=""
    ):
        """General function to get results from the API using the collection endpoint with optional filter and fields
        
        Parameters
        ----------
        collection : str
            Collection name to query, e.g. "functional_annotation_agg"
        filter : str, optional
            Filter to apply to the query written in JSON format for MongoDB
            e.g. '{"metagenome_annotation_id":{"$regex":"wfmp"}}'
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
                url = f"{self.base_url}/nmdcschema/{collection}?&filter={filter}&max_page_size={max_page_size}&next_page_token={next_page_token}&projection={fields}"
                response = requests.get(url)
                data_next = response.json()

                results = data_next.get("resources", [])
                result_list.extend(results)
                next_page_token = data_next.get("next_page_token")

                if not next_page_token:
                    break

        return result_list

    def get_previously_aggregated_records(self):
        """
        Function to return all ids of metaproteomics activity records that have already been aggregated

        Returns
        -------
        list
            List of metagenome_annotation_ids that have already been aggregated
        """
        agg_col = self.get_results(
            collection="functional_annotation_agg",
            filter='{"metagenome_annotation_id":{"$regex":"wfmp"}}',
            max_page_size=1000,
            fields="metagenome_annotation_id",
        )
        ids = list(set([x["metagenome_annotation_id"] for x in agg_col]))
        return ids

    def get_activity_records(self):
        """
        Function to return full metaproteomics activity records in the database

        Returns
        -------
        list
            List of metaproteomics activity records
        """
        act_col = self.get_results(
            collection="workflow_execution_set", 
            filter='{"type":"nmdc:MetaproteomicsAnalysis"}', 
            max_page_size=1000, 
            fields=""
        )
        return act_col

    def submit_json_records(self, json_records):
        """
        Function to submit aggregation records to the database

        Parameters
        ----------
        json_records : list
            List of dictionaries where each dictionary represents a record to be submitted to the database
        """
        url = f"{self.base_url}/metadata/json:submit"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.nmdc_api_token}",
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, json=json_records)

        return response.status_code
    
    def read_url_tsv(self, url):
        """
        Function to read a URL that points to a TSV file

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

    def get_functional_terms(self, url):
        """
        Function to get the functional terms from a URL of a Protein Report

        Parameters
        ----------
        url : str
            URL to the Protein Report

        Returns
        -------
        dict
            Dictionary of KO, COG, and pfam terms with their respective spectral counts
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
                    fxns[ko_clean] = int(line.get("SummedSpectraCounts"))
                else:
                    fxns[ko_clean] += int(line.get("SummedSpectraCounts"))
            
            # Add cog terms to the dictionary
            cog = line.get("COG")
            if cog != "" and cog is not None:
                cog_clean = "COG:" + cog
                if cog_clean not in fxns.keys():
                    fxns[cog_clean] = int(line.get("SummedSpectraCounts"))
                else:
                    fxns[cog_clean] += int(line.get("SummedSpectraCounts"))
            
            # Add pfam terms to the dictionary
            pfam = line.get("pfam")
            if pfam != "" and pfam is not None:
                pfam_clean = "PFAM:" + pfam
                if pfam_clean not in fxns.keys():
                    fxns[pfam_clean] = int(line.get("SummedSpectraCounts"))
                else:
                    fxns[pfam_clean] += int(line.get("SummedSpectraCounts"))
        

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

    def find_anno_url(self, dos):
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
        Function to process an activity record.
        Input: activity record
        Output: Dictonary of KEGG records

        This currently relies on the has_peptide_quantificiations
        records in the activity record.  This may change in the future.
        """
        # Get the URL and ID
        url = self.find_anno_url(act["has_output"])
        if not url:
            raise ValueError(f"Missing url for {act['id']}")

        # Parse the KEGG, COG, and PFAM annotations
        return self.get_functional_terms(url)

    def sweep(self):
        """
        This is the main action function. 
        
        Steps:
        1. Get list of workflow IDs that have already been added to the functional_annotation_agg collection
        2. Get list of all metaproteomics activities in the database
        3. For each activity that is not in the list of previously aggregated records, process the activity:
            a. Find the Protein Report URL
            b. From the Protein Report URL, extract the KEGG, COG, and PFAM annotations and associated counts
            c. Prepare a json record for the database with the annotations and counts
            d. Validate the json record using the post /metadata/json:validate endpoint
            e. If the json record is valid, submit it to the database using the post /metadata/json endpoint
        """
        # Get list of workflow IDs that have already been processed
        mp_wf_in_agg = self.get_previously_aggregated_records()

        # Get list of all metaproteomics activities
        mp_wf_recs = self.get_activity_records()

        # Records to add to the aggregation
        agg_records = {}

        # Iterate through all of the metaP activities
        for mp_wf_rec in mp_wf_recs:
            if mp_wf_rec["id"] in mp_wf_in_agg:
                continue
            try:
                agg_records[mp_wf_rec["id"]] = self.process_activity(mp_wf_rec)
            except Exception as ex:
                # Continue on errors
                print(ex)
                continue

            # Prepare a  json record for the database
            json_records = []
            for key, value in agg_records.items():
                for k, v in value.items():
                    json_records.append(
                        {"metagenome_annotation_id": key, "gene_function_id": k, "count": v}
                    )
            json_record_full = {"functional_annotation_agg": json_records}

            # Validate the json record using the post /metadata/json:validate endpoint
            url = f"{self.base_url}/metadata/json:validate"
            response = requests.post(url, json=json_record_full)

            # If the json record is valid, submit it to the database using the post /metadata/json endpoint
            if response.status_code == 200:
                response = self.submit_json_records(json_records)
                if response != 200:
                    print("Error submitting the aggregation records for the workflow: ", mp_wf_rec["id"])
                if response == 200:
                    print("Submitted aggregation records for the workflow: ", mp_wf_rec["id"])


    def sweep_success(self):
        """Function to check the results of the sweep and ensure that the records were added to the database
        
        Returns
        -------
        bool
            True if all records were added to the functional_annotation_agg collection, False otherwise
        """
        # Get list of workflow IDs that have already been processed
        mp_wf_in_agg = self.get_previously_aggregated_records()

        # Get list of all metaproteomics activities
        mp_wf_recs = self.get_activity_records()

        # If there are any records that were not processed, return FALSE
        check = [x for x in mp_wf_recs if x["id"] in mp_wf_in_agg]
        if all(check):
            return True
        else:
            return False    

if __name__ == "__main__":
    mp_dev = MetaProtAgg()
    mp_dev.sweep()

    # Wait for the records to be added to the database before running check (5 minutes)
    time.sleep(300)
    success_check = mp_dev.sweep_success()

"""
# This is commented out until script is ready for production
    if success_check:
        # Reprocess in the production API
        mp_prod = MetaProtAgg(dev=False)
        mp_prod.sweep()

        # Wait for the records to be added to the database before running check (5 minutes)
        time.sleep(300)
        success_check = mp_prod.sweep_success()
"""