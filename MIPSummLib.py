#!/bin/env python
# -*- coding: utf-8 -*-

# %% imports

import copy
import logging
import requests
import numpy as np

# import pdb

# %% notes
"""
Created on Sat Nov 16 06:34:21 2024

Paul J. Durack 16 November 2024

This python library holds a number of reusable functions
being used in this repo

2024-
PJD 16 Nov 2024 - started
PJD 17 Nov 2024 - updated serpapi call to use cluster vs doi query
PJD 18 Nov 2024 - add updateLineColours func
PJD 22 Jan 2025 - tweak pullstats to deal with int64 string mapping (cfmip, omip2)
                  citation before publication

@author: durack1
"""

# %% function defs

# API Expanded
WOS_API_URL = "https://wos-api.clarivate.com/api/wos"
# https://api.clarivate.com/swagger-ui/?apikey=none&url=https%3A%2F%2Fdeveloper.clarivate.com%2Fapis%2Fwos%2Fswagger
# Starter API
WoSStarter_API_URL = "https://api.clarivate.com/apis/wos-starter/v1"
# https://api.clarivate.com/swagger-ui/?apikey=none&url=https%3A%2F%2Fdeveloper.clarivate.com%2Fapis%2Fwos-starter%2Fswagger


def apiKeyW():
    """
    Read WoS API key from local store
    """
    with open("WoSKey.txt", "r") as f:
        tmp = f.read()
        key = tmp.split()[-1]

    return key


def apiKeyG():
    """
    Read SerpAPI key from local store
    """
    with open("SerpKey.txt", "r") as f:
        tmp = f.read()
        key = tmp.split()[-1]

    return key


def grabCitationReport(queryId, params={}):
    """
    Use queryId to grab json output - every query counts as 1 against quota
    """
    headers = {"Accept": "application/json", "X-ApiKey": apiKeyW()}
    r = requests.get(
        WOS_API_URL + "/citation-report/" + str(queryId),
        params=params,
        headers=headers,
        timeout=10,
    )
    try:
        rj = r.json()
        logging.debug("API response: {}".format(rj))
        return rj
    except Exception:
        logging.exception("Citation report for queryId {} failed".format(queryId))
        raise


def grabGoogleScholarCites(doi):
    """
    User SerpAPI to scour citation counts from Google Scholar
    """
    # params = {"api_key": apiKeyG(), "engine": "google_scholar", "q": doi, "hl": "en"}
    params = {
        "api_key": apiKeyG(),
        "engine": "google_scholar",
        "cluster": doi,
        "hl": "en",
    }
    queryUrl = "https://serpapi.com/search.json?"
    r = requests.get(queryUrl, params=params, timeout=10)
    # catch case of allocation time out
    if "organic_results" not in r.json().keys():
        print("Processing GS: API allocation exceeded")
        googleScholCites = None
    else:
        try:
            rj = r.json()
            logging.debug("SerpAPI response: {}".format(rj))
            googleScholCites = rj["organic_results"][0]["inline_links"]["cited_by"][
                "total"
            ]
            if "authors" in rj["organic_results"][0]["publication_info"].keys():
                authorCount = len(
                    rj["organic_results"][0]["publication_info"]["authors"]
                )
                firstAuthorLastName = rj["organic_results"][0]["publication_info"][
                    "authors"
                ][0]["name"]
            else:
                authorCount = 0
                firstAuthorLastName = (
                    rj["organic_results"][0]["publication_info"]["summary"]
                    .split("-")[0]
                    .strip()
                )
            if authorCount > 1:
                etal = "et al."
            else:
                etal = ""
            pubYr = (
                rj["organic_results"][0]["publication_info"]["summary"]
                .split("-")[1]
                .split(",")[-1]
                .strip()
            )
            print("Processing GS:", firstAuthorLastName, etal, pubYr, googleScholCites)
        except Exception:
            logging.exception(doi)
            raise

    return googleScholCites


def grabQueryId(query, params={}):
    """
    Send API dummy call - ping to get query ID, start connection
    """
    query = {"databaseId": "WOS", "usrQuery": query, "count": 0, "firstRecord": 1}
    query.update(params)
    headers = {"Accept": "application/json", "X-ApiKey": apiKeyW()}
    # logging.info('Query parameters: {}'.format(query))
    # print(query)
    r = requests.get(WOS_API_URL, params=query, headers=headers, timeout=10)
    try:
        # print(r.text)
        rj = r.json()
        # print(rj)
        logging.debug("API response: {}".format(rj))
        queryId = rj["QueryResult"]["QueryID"]
        return queryId
    except Exception:
        logging.exception(query)
        raise


def grabQueryReport(queryId, params={}):
    """
    Use queryId to grab json output - every query counts as 1 against quota
    """
    headers = {"Accept": "application/json", "X-ApiKey": apiKeyW()}
    r = requests.get(
        WOS_API_URL + "/query/" + str(queryId),
        params=params,
        headers=headers,
        timeout=10,
    )
    try:
        rj = r.json()
        logging.debug("API response: {}".format(rj))
        return rj
    except Exception:
        logging.exception("Citation report for queryId {} failed".format(queryId))
        raise


def pullStats(wosId, doi, padArray):
    """
    From WoS Expanded API DOI object extract time history of citations
    along with total citation count and pubYr
    """
    # construct per call arguments and send to API
    params = "UT={}".format(wosId)
    # if doi != "":
    #    params = "&".join([params, "DO={}".format(doi)])
    queryId = grabQueryId(params)
    # query
    query = grabQueryReport(queryId)
    pubYr = query["Records"]["records"]["REC"][0]["static_data"]["summary"]["pub_info"][
        "pubyear"
    ]
    authorCount = query["Records"]["records"]["REC"][0]["static_data"]["summary"][
        "names"
    ][
        "count"
    ]  # Author count
    if authorCount > 1:
        etal = "et al."
        firstAuthorLastName = query["Records"]["records"]["REC"][0]["static_data"][
            "summary"
        ]["names"]["name"][0]["last_name"]
    else:
        etal = ""
        firstAuthorLastName = query["Records"]["records"]["REC"][0]["static_data"][
            "summary"
        ]["names"]["name"]["last_name"]
    print("Processing WoS:", firstAuthorLastName, etal, pubYr)
    # citation-report
    crParams = {"reportLevel": "WOS"}
    crData = grabCitationReport(queryId, crParams)
    # pull entries out of data object
    citingYrsDict = crData[0]["CitingYears"]
    citingYrs = list(map(int, citingYrsDict.values()))
    timesCited = crData[0]["TimesCited"]

    # ascertain length; generate padded citingYears - check pubYr against citeStartYr
    citeStartYr = int(list(citingYrsDict.keys())[0])
    startInd = citeStartYr - pubYr  # 0 if cited same year published
    # print("startInd:", startInd)
    # print("len(citingYrsPad):", len(citingYrsPad))
    indEnd = len(citingYrs)

    # padArray copy.deepcopy()
    citingYrsPad = copy.deepcopy(padArray)

    if startInd == -1:
        # count first two entries as one
        tmp = list(map(int, citingYrs))
        tmp1 = [np.sum(tmp[:2])]
        tmp1.extend(citingYrs[2:])
        citingYrs = list(map(int, tmp1))  # catch issue with omip2 wos first entry "6"
        del (tmp, tmp1)
        citingYrsPad[0 : indEnd - 1] = list(map(int, citingYrs))
    elif startInd == 1:
        citingYrsPad[0] = 0
        citingYrsPad[startInd : indEnd + 1] = list(map(int, citingYrs))
    elif startInd > 1:
        citingYrsPad[0:startInd] = list(map(int, np.zeros(startInd)))
        citingYrsPad[startInd : indEnd + startInd] = list(map(int, citingYrs))
    else:
        citingYrsPad[0:indEnd] = list(map(int, citingYrs))
    # print("len(citingYrsPad):", len(citingYrsPad))

    return pubYr, timesCited, citingYrs, citingYrsPad, citingYrsDict


def updateLineColours(ax, cm):
    """
    For line plot, take provided colourmap and recolour lines
    https://stackoverflow.com/questions/20040597/matplotlib-change-colormap-after-the-fact
    """
    lines = ax.lines
    colours = cm(np.linspace(0, 1, len(lines)))
    for line, c in zip(lines, colours):
        line.set_color(c)


# %%
