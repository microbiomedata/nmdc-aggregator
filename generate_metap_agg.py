from aggregator import Aggregator

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
    
    def get_functional_terms_from_peptide_report(self, url):
        """Function to get the functional terms from a URL of a Peptide Report

        Parameters
        ----------
        url : str
            URL to the Peptide Report

        Returns
        -------
        dict
            Dictionary of KEGG, COG, and PFAM terms with their respective spectral counts derived from the Peptide Report
        """
        # Parse the Peptide Report content
        content_pep = self.read_url_tsv(url)

        # Initialize the dictionary to store the peptide annotations for each peptide sequence
        pep_dict = {}

        # Loop through the Peptide Report content and populate the dictionary for each peptide sequence
        for line in content_pep:
            peptide_sequence = line.get("peptide_sequence")
            
            # Add the peptide sequence and spectral count to the dictionary and initialize the annotations list
            if peptide_sequence not in pep_dict:
                pep_dict[peptide_sequence] = {}
                pep_dict[peptide_sequence]["spectral_counts"] = int(float(line.get("peptide_spectral_count")))
                pep_dict[peptide_sequence]["annotations"] = []
            
            # Get the annotations for the peptide sequence
            annotations = []

            # Add ko terms to annotations list
            ko = line.get("KO")
            if ko != "" and ko is not None:
                for ko_term in ko.split(","):
                    # Replace KO: with KEGG.ORTHOLOGY:
                    ko_clean = ko_term.replace("KO:", "KEGG.ORTHOLOGY:").strip()
                    annotations.append(ko_clean)

            # Add cog terms to annotations list
            cog = line.get("COG")
            if cog != "" and cog is not None:
                for cog_term in cog.split(","):
                    cog_clean = "COG:" + cog_term.strip()
                    annotations.append(cog_clean)

            # Add pfam terms to annotations list
            pfam = line.get("pfam")
            if pfam != "" and pfam is not None:
                for pfam_term in pfam.split(","):
                    pfam_clean = "PFAM:" + pfam_term.strip()
                    annotations.append(pfam_clean)
            
            # Add the annotations to the peptide sequence dictionary
            pep_dict[peptide_sequence]["annotations"] = list(set(pep_dict[peptide_sequence]["annotations"] + annotations))
        
        # Collapse the peptide annotations to the functional annotation level
        pep_fxns = {}
        # loop through the peptides and add the annotations and spectral counts to the functional annotations dictionary
        for pep_seq, pep_single_dict in pep_dict.items():
            for annotation in pep_single_dict["annotations"]:
                pep_fxns = self.add_to_dict(pep_fxns, annotation, pep_single_dict["spectral_counts"])

        return pep_fxns

    def find_peptide_report_url(self, dos):
        """Find the URL for the peptide report from a list of data object IDs

        Parameters
        ----------
        dos : list
            List of data object IDs

        Returns
        -------
        str
            URL for the Peptide Report data object if found
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

        # Find the Peptide Report data object and return the URL to access it
        for do in do_recs:
            if do.get("data_object_type") == "Peptide Report":
                url = do.get("url")
                return url

        # If no Peptide Report data object is found, return None
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
        url = self.find_peptide_report_url(act["has_output"])
        if not url:
            raise ValueError(f"Missing url for {act['id']}")

        # Parse the KEGG, COG, and PFAM annotations
        return self.get_functional_terms_from_peptide_report(url)


if __name__ == "__main__":
    mp = MetaProtAgg()
    mp.sweep()
