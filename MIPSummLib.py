#!/bin/env python
# -*- coding: utf-8 -*-

"""
MIPSummLib - Reusable functions for CMIP citation analysis

Created: November 16, 2024
Author: Paul J. Durack @durack1

Provides functionality for:
- Web of Science (WoS) API queries and citation reports
- Google Scholar citation retrieval via SerpAPI
- Citation data processing and padding to current year

Version History:
- 2024-11-16: Initial creation
- 2024-11-17: Updated SerpAPI to use cluster (doi) query
- 2024-11-18: add updateLineColours
- 2025-01-22: pullstats updated to deal with int64 string mapping (cfmip, omip2)
- 2025-01-23: pullstats updated to track citeStart, pub and end yrs
- 2025-01-24: add padCitationCounts
- 2025-01-28: Enhanced for gsch DOI or cluster ID flexibility (author=researchgate.net)
- 2026-05-23: Refactored for cleaner code practices; map gsch doi/cluster input
"""

import copy
import datetime
import logging
import re
import requests
import numpy as np


# API Constants
WOS_API_URL = "https://wos-api.clarivate.com/api/wos"
WOS_STARTER_API_URL = "https://api.clarivate.com/apis/wos-starter/v1"
SERPAPI_URL = "https://serpapi.com/search.json"


# ============================================================================
# API Key Management
# ============================================================================

def _load_api_key(filename):
    """Load API key from file (last whitespace-delimited token)."""
    try:
        with open(filename, "r") as f:
            return f.read().split()[-1]
    except FileNotFoundError:
        logging.error(f"API key file not found: {filename}")
        raise


def apiKeyW():
    """Get WoS API key from WoSKey.txt."""
    return _load_api_key("WoSKey.txt")


def apiKeyG():
    """Get SerpAPI key from SerpKey.txt."""
    return _load_api_key("SerpKey.txt")


# ============================================================================
# Helper Functions
# ============================================================================

def convertToFloat(inList):
    """Convert list elements to float type."""
    return [float(x) for x in inList]


def _make_wos_headers():
    """Build WoS API headers with authentication."""
    return {"Accept": "application/json", "X-ApiKey": apiKeyW()}


def _extract_author_info(pub_info):
    """
    Extract author count and first author name from publication info.

    Returns:
        tuple: (author_count, first_author_last_name, et_al_str)
    """
    if "authors" in pub_info:
        authors = pub_info["authors"]
        author_count = len(authors)
        first_author_last_name = authors[0]["name"]
        et_al = "et al." if author_count > 1 else ""
        return author_count, first_author_last_name, et_al

    # Fallback: extract from summary (e.g., ResearchGate)
    summary = pub_info.get("summary", "Unknown")
    if summary == "researchgate.net":
        return 0, "researchgate.net", ""

    first_author = summary.split("-")[0].strip()
    return 0, first_author, ""


def _is_doi_format(query_id):
    """Check if query_id appears to be a DOI (contains slashes)."""
    return "/" in str(query_id)


# ============================================================================
# WoS API Functions
# ============================================================================

def grabQueryId(query, params=None):
    """
    Send API call to get query ID.

    Args:
        query (str): WoS query string
        params (dict): Additional query parameters

    Returns:
        str: Query ID for subsequent API calls

    Raises:
        Exception: If API call fails
    """
    if params is None:
        params = {}

    query_obj = {
        "databaseId": "WOS",
        "usrQuery": query,
        "count": 0,
        "firstRecord": 1
    }
    query_obj.update(params)

    try:
        r = requests.get(WOS_API_URL, params=query_obj,
                         headers=_make_wos_headers(), timeout=10)
        rj = r.json()
        logging.debug(f"grabQueryId response: {rj}")
        return rj["QueryResult"]["QueryID"]
    except Exception as e:
        logging.exception(f"Failed to get query ID for: {query}")
        raise


def grabQueryReport(queryId, params=None):
    """
    Fetch query results using a query ID.

    Args:
        queryId (str): Query ID from grabQueryId()
        params (dict): Additional parameters

    Returns:
        dict: Query results JSON

    Raises:
        Exception: If API call fails
    """
    if params is None:
        params = {}

    try:
        r = requests.get(
            f"{WOS_API_URL}/query/{queryId}",
            params=params,
            headers=_make_wos_headers(),
            timeout=10
        )
        rj = r.json()
        logging.debug(f"grabQueryReport response: {rj}")
        return rj
    except Exception as e:
        logging.exception(
            f"Failed to retrieve query report for queryId: {queryId}")
        raise


def grabCitationReport(queryId, params=None):
    """
    Fetch citation report for a query ID.

    Args:
        queryId (str): Query ID from grabQueryId()
        params (dict): Additional parameters (e.g., reportLevel)

    Returns:
        dict: Citation report JSON

    Raises:
        Exception: If API call fails
    """
    if params is None:
        params = {}

    try:
        r = requests.get(
            f"{WOS_API_URL}/citation-report/{queryId}",
            params=params,
            headers=_make_wos_headers(),
            timeout=10
        )
        rj = r.json()
        logging.debug(f"grabCitationReport response: {rj}")
        return rj
    except Exception as e:
        logging.exception(
            f"Failed to retrieve citation report for queryId: {queryId}")
        raise


# ============================================================================
# Google Scholar Functions
# ============================================================================

def grabGoogleScholarCites(query_id):
    """
    Retrieve citation data from Google Scholar via SerpAPI.

    Accepts either a DOI or Google Scholar cluster ID. Auto-detects format:
    - DOI: Contains "/" (e.g., "10.1175/JCLI-D-18-0823.1")
    - Cluster ID: Numeric value (e.g., "1234567890")

    Args:
        query_id (str): DOI or Google Scholar cluster ID

    Returns:
        int or None: Citation count, or None if query fails

    Prints:
        Author name, "et al." if applicable, publication year, citation count
    """
    # Auto-detect input type
    query_param = "doi" if _is_doi_format(query_id) else "cluster"

    params = {
        "api_key": apiKeyG(),
        "engine": "google_scholar",
        query_param: query_id,
        "hl": "en",
    }

    try:
        r = requests.get(SERPAPI_URL, params=params, timeout=10)
        rj = r.json()
        logging.debug(f"grabGoogleScholarCites response: {rj}")

        # Check if API quota exceeded
        if "organic_results" not in rj:
            print("Processing GS: API allocation exceeded")
            return None

        result = rj["organic_results"][0]

        # Extract citation count
        cite_count = None
        if "inline_links" in result and "cited_by" in result["inline_links"]:
            cite_count = result["inline_links"]["cited_by"]["total"]

        # Extract publication info
        pub_info = result.get("publication_info", {})
        author_count, first_author, et_al = _extract_author_info(pub_info)

        # Extract publication year from summary
        pub_year = ""
        if "summary" in pub_info:
            parts = pub_info["summary"].split("-")
            if len(parts) > 1:
                pub_year = parts[-1].split(",")[-1].strip()

        print(f"Processing GS: {first_author} {et_al} {pub_year} {cite_count}")
        return cite_count

    except Exception as e:
        logging.exception(
            f"Failed to retrieve Google Scholar data for: {query_id}")
        raise


# ============================================================================
# Citation Data Processing
# ============================================================================

def padCiteCounts(citeDict, pubYr):
    """
    Process WoS citation data: aggregate pre-publication citations, fill gaps to current year.

    Args:
        citeDict (dict): Citation data from WoS report with 'CitingYears' key
        pubYr (int): Publication year

    Returns:
        tuple: (citingYrs, citingCounts, citingYrsComplete, citeCountsComplete)
               Complete year and count arrays from pubYr to current year
    """
    currentYr = datetime.date.today().year

    # Extract and convert years/counts
    citingYrs = list(map(int, citeDict["CitingYears"].keys()))
    citingCounts = list(map(int, citeDict["CitingYears"].values()))
    citeStartYr = citingYrs[0]

    # Handle citations before publication year
    startInd = citeStartYr - pubYr
    if startInd < 0:
        print("**case citeStartYr < pubYr")
        # Sum all pre-publication citations into publication year
        newInd = abs(startInd) + 1
        citingCounts = [np.sum(citingCounts[:newInd])] + citingCounts[newInd:]
        citingYrs = citingYrs[abs(startInd):]

    # Create complete year range with padding
    citingYrsComplete = np.arange(pubYr, currentYr + 1, dtype="int16").tolist()
    citingCountsComplete = np.zeros(
        len(citingYrsComplete), dtype="int16").tolist()

    # Fill in observed years
    for count, yr in enumerate(citingYrs):
        if yr == currentYr:
            print(
                f"Current year: {currentYr}, total citations: {citingCounts[count]}")
        idx = citingYrsComplete.index(yr)
        citingCountsComplete[idx] = citingCounts[count]

    return citingYrs, citingCounts, citingYrsComplete, citingCountsComplete


def pullStats(wosId, doi, padArray):
    """
    Extract publication and citation statistics from WoS.

    Args:
        wosId (str): Web of Science ID
        doi (str): DOI (for reference, currently unused in query)
        padArray (list): Pre-allocated array for citation counts

    Returns:
        tuple: (pubYr, timesCited, citingYrs, citingCountsCompletePad,
                citingYrsDict, citeStartYr, citeEndYr)
    """
    # Query WoS for publication record
    queryId = grabQueryId(f"UT={wosId}")
    query = grabQueryReport(queryId)

    # Extract publication info
    rec = query["Records"]["records"]["REC"][0]["static_data"]["summary"]
    pubYr = rec["pub_info"]["pubyear"]

    # Extract author info
    names = rec["names"]
    author_count = names["count"]

    if author_count > 1:
        et_al = "et al."
        first_author = names["name"][0]["last_name"]
    else:
        et_al = ""
        first_author = names["name"]["last_name"]

    print(f"Processing WoS: {first_author} {et_al} {pubYr}")

    # Get citation report
    cite_report = grabCitationReport(queryId, {"reportLevel": "WOS"})

    # Process citations
    citingCountsCompletePad = copy.deepcopy(padArray)
    citingYrs, citingCounts, citingYrsComplete, citeCountsComplete = padCiteCounts(
        cite_report[0], pubYr
    )

    # Fill padded array
    citingCountsCompletePad[0:len(citeCountsComplete)] = citeCountsComplete

    # Extract metadata
    citingYrsDict = cite_report[0]["CitingYears"]
    timesCited = cite_report[0]["TimesCited"]
    citeStartYr = citingYrs[0]
    citeEndYr = citingYrs[-1]

    # Convert to float for JSON serialization
    citingYrs = convertToFloat(citingYrs)
    citingCountsCompletePad = convertToFloat(citingCountsCompletePad)

    return (
        pubYr,
        timesCited,
        citingYrs,
        citingCountsCompletePad,
        citingYrsDict,
        citeStartYr,
        citeEndYr,
    )


# ============================================================================
# Plotting Utilities
# ============================================================================

def updateLineColours(ax, cm):
    """
    Recolor line plot using provided colormap.

    Args:
        ax: Matplotlib axis object
        cm: Colormap to apply

    Reference:
        https://stackoverflow.com/questions/20040597/matplotlib-change-colormap-after-the-fact
    """
    lines = ax.lines
    colours = cm(np.linspace(0, 1, len(lines)))
    for line, c in zip(lines, colours):
        line.set_color(c)
