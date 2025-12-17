import requests
# from nmdc_api_utilities.functional_annotation_agg_search import FunctionalAnnotationAggSearch

def get_results(collection: str, filter="", max_page_size=100, fields="", return_all=True):
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
    return_all : bool, optional
        If True, returns all pages of results. If False, returns only the first page.
        Default is True
    """

    # Get initial results (before next_page_token is given in the results)
    result_list = []
    og_url = f"https://api.microbiomedata.org/nmdcschema/{collection}?&filter={filter}&max_page_size={max_page_size}&projection={fields}"
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

    # If return_all is False, return only the first page
    if not return_all:
        return result_list

    # if there are multiple pages of results returned
    if initial_data.get("next_page_token"):
        next_page_token = initial_data["next_page_token"]

        while True:
            i = i + max_page_size
            url = og_url + f"&page_token={next_page_token}"
            response = requests.get(url)
            data_next = response.json()

            results = data_next.get("resources", [])
            result_list.extend(results)
            next_page_token = data_next.get("next_page_token")

            if not next_page_token:
                break

    return result_list

def _get_all_pages(
    self,
    response: requests.models.Response,
    filter: str = "",
    max_page_size: int = 100,
    fields: str = "",
):
    """
    Get all pages of data from the NMDC API. This is a helper function to get all pages of data from the NMDC API.

    Parameters
    ----------
    response: requests.models.Response
        The response object from the API request.
    filter: str
        The filter to apply to the query. Default is an empty string.
    max_page_size: int
        The maximum number of items to return per page. Default is 100.
    fields: str
        The fields to return. Default is all fields.

    Returns
    -------
    list[dict]
        A list of dictionaries containing the records.

    Raises
    ------
    RuntimeError
        If the API request fails.

    """

    results = response.json()

    while True:
        if response.json().get("next_page_token"):
            next_page_token = response.json()["next_page_token"]
        else:
            break
        url = f"https://api.microbiomedata.org/nmdcschema/functional_annotation_agg?filter={filter}&max_page_size={max_page_size}&projection={fields}&page_token={next_page_token}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise RuntimeError("Failed to get collection from NMDC API") from e
        results = {"resources": results["resources"] + response.json()["resources"]}
    return results

def get_results_with_api_utilities():
    """Function to get results using the API utilities library"""
    def get_records(
        filter: str = "",
        max_page_size: int = 100,
        fields: str = "",
        all_pages: bool = False,
    ) -> list[dict]:
        """
        Get a collection of data from the NMDC API. Generic function to get a collection of data from the NMDC API. Can provide a specific filter if desired.

        Parameters
        ----------
        filter: str
            The filter to apply to the query. Default is an empty string.
        max_page_size: int
            The maximum number of items to return per page. Default is 100.
        fields: str
            The fields to return. Default is all fields.
        all_pages: bool
            True to return all pages. False to return the first page. Default is False.

        Returns
        -------
        list[dict]
            A list of dictionaries containing the records.

        Raises
        ------
        RuntimeError
            If the API request fails.

        """
        # filter = urllib.parse.quote(filter)
        url = f"https://api.microbiomedata.org/nmdcschema/functional_annotation_agg?&filter={filter}&max_page_size={max_page_size}&projection={fields}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise RuntimeError("Failed to get collection from NMDC API") from e

        results = response.json()["resources"]
        # otherwise, get all pages
        if all_pages:
            results = _get_all_pages(response, filter, max_page_size, fields)[
                "resources"
            ]

        return results
    filter = '{"was_generated_by":{"$regex":"^nmdc:wfmp"}}'
    resp = get_records(filter=filter, fields="was_generated_by", max_page_size=1000, all_pages=True)
    return resp

agg = get_results(
            collection="functional_annotation_agg",
            filter='{"was_generated_by":{"$regex":"^nmdc:wfmp"}}',
            max_page_size=1000,
            fields="was_generated_by",
        )
print()
# api = get_results_with_api_utilities()
print()