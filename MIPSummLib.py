#!/bin/env python
# -*- coding: utf-8 -*-

# %% imports

import copy
import datetime
import logging
import requests
import numpy as np

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
PJD 23 Jan 2025 - augmented pullstats to track citeStart, pub and end yrs
PJD 24 Jan 2025 - add padCitationCounts

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


def convertToFloat(inList):
    """
    Convert all list integers to float type
    """
    return [float(x) for x in inList]


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


def padCiteCounts(citeDict, pubYr):
    """
    Take WoS citation year:count, sum earlier citations to pubYr, fill missing
    years and expand to current year, even if not citations to fill
    """
    currentYr = datetime.date.today().year
    # targetYr = currentYr - 1
    # print("currentYr:", currentYr, "targetYr:", targetYr)

    # ascertain cite start year build list
    citingYrs = list(map(int, citeDict["CitingYears"].keys()))
    citingCounts = list(map(int, citeDict["CitingYears"].values()))
    citeStartYr = citingYrs[0]
    # print("citingYrs:", citingYrs)
    # print("citingCounts:", citingCounts)

    # if citeStartYr < pubYr sum first entries
    startInd = citeStartYr - pubYr  # 0 if cited same year published
    startIndAbs = abs(startInd)
    # print("startInd:", startInd)
    if startInd < 0:  # cfmip, omip2 = -1; = 0; ar1 = 1; dynvarmip = 2
        print("**case citeStartYr < pubYr")
        # sum entries before publication yr into pubYr
        tmp = copy.deepcopy(citingCounts)
        # print("citingCounts:", citingCounts)
        # print("tmp:       ", tmp)
        newInd = abs(startInd) + 1
        tmp1 = [np.sum(tmp[:newInd])]
        tmp1.extend(citingCounts[newInd:])
        # print("tmp1.ext:", tmp1)
        citingCounts = tmp1  # list(map(int, tmp1))
        citingYrs = citingYrs[startIndAbs:]
        del (tmp, tmp1)

    # preallocate target - ar1 has holes
    citingYrsComplete = np.arange(pubYr, currentYr + 1, dtype="int16").tolist()
    citingCountsComplete = np.zeros(len(citingYrsComplete), dtype="int16").tolist()

    # iterate and fill - ignoring current year
    for count, yr in enumerate(citingYrs):
        # report currentYr partial counts
        if yr == currentYr:  # fangio, cmip3, ar4, cmip5, cmip6, 250124
            print("Current year:", currentYr, "total citations:", citingCounts[count])
        ind = citingYrsComplete.index(yr)
        citingCountsComplete[ind] = citingCounts[count]

    # print("citingYrsComplete:", len(citingYrsComplete), citingYrsComplete)
    # print("citingCountsComplete:", len(citingCountsComplete), citingCountsComplete)

    return citingYrs, citingCounts, citingYrsComplete, citingCountsComplete


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

    #### drop json to file
    # with open("query-dynvarmip.json", "w") as f:
    #    json.dump(
    #        query, f, ensure_ascii=True, sort_keys=True, indent=4, separators=(",", ":")
    #    )

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

    #### drop json to file
    # with open("crData-cordex.json", "w") as f:
    #    json.dump(
    #        crData,
    #        f,
    #        ensure_ascii=True,
    #        sort_keys=True,
    #        indent=4,
    #        separators=(",", ":"),
    #    )

    # pull entries out of data object
    # citingYrsDict = crData[0]["CitingYears"]
    # citingYrs = list(map(int, citingYrsDict.values()))
    # timesCited = crData[0]["TimesCited"]

    # ascertain length; generate padded citingYears - check pubYr against citeStartYr
    # citeStartYr = int(list(citingYrsDict.keys())[0])
    # citeEndYr = int(list(citingYrsDict.keys())[-1])
    # startInd = citeStartYr - pubYr  # 0 if cited same year published
    # print("startInd:", startInd)
    # print("len(citingYrsPad):", len(citingYrsPad))
    # indEnd = len(citingYrs)

    # padArray copy.deepcopy() NaN length array
    citingCountsCompletePad = copy.deepcopy(padArray)
    del padArray

    # print(
    #    "citingCountsCompletePad 1:",
    #    len(citingCountsCompletePad),
    #    citingCountsCompletePad,
    # )

    # pass info to padCiteCounts providing a time complete entry to currentYr-1
    citingYrs, citingCounts, citingYrsComplete, citeCountsComplete = padCiteCounts(
        crData[0], pubYr
    )
    indEnd = len(citingYrsComplete)  # stop prior to currentYr, index in zero space
    # print("indEnd:", indEnd, "len(citingYrsComplete):", len(citingYrsComplete))
    citingCountsCompletePad[0:indEnd] = citeCountsComplete

    # print(
    #    "citingCountsCompletePad 2:",
    #    len(citingCountsCompletePad),
    #    citingCountsCompletePad,
    # )
    # print("**********")

    citingYrsDict = crData[0]["CitingYears"]
    timesCited = crData[0]["TimesCited"]
    citeStartYr = citingYrs[0]
    citeEndYr = citingYrs[-1]

    # old and delete-able
    # if startInd == -1:
    #    # count first two entries as one
    #    tmp = list(map(int, citingYrs))
    #    tmp1 = [np.sum(tmp[:2])]
    #    tmp1.extend(citingYrs[2:])
    #    citingYrs = list(map(int, tmp1))  # catch issue with omip2 wos first entry "6"
    #    del (tmp, tmp1)
    #    citingYrsPad[0 : indEnd - 1] = list(map(int, citingYrs))
    # elif startInd == 1:
    #    citingYrsPad[0] = 0
    #    citingYrsPad[startInd : indEnd + 1] = list(map(int, citingYrs))
    # elif startInd > 1:
    #    citingYrsPad[0:startInd] = list(map(int, np.zeros(startInd)))
    #    citingYrsPad[startInd : indEnd + startInd] = list(map(int, citingYrs))
    # else:
    #    citingYrsPad[0:indEnd] = list(map(int, citingYrs))
    # print("len(citingYrsPad):", len(citingYrsPad))

    # print("type(pubYr):", type(pubYr))
    # print("type(timesCited):", type(timesCited))
    # print("type(citingYrs):", type(citingYrs))
    # print("type(citingCountsCompletePad):", type(citingCountsCompletePad))
    # print("type(citingYrsDict):", type(citingYrsDict))
    # print("type(citeStartYr):", type(citeStartYr))
    # print("type(citeEndYr):", type(citeEndYr))

    # explicitly convert int64 to int16 - json.dump can't write it
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


def updateLineColours(ax, cm):
    """
    For line plot, take provided colourmap and recolour lines
    https://stackoverflow.com/questions/20040597/matplotlib-change-colormap-after-the-fact
    """
    lines = ax.lines
    colours = cm(np.linspace(0, 1, len(lines)))
    for line, c in zip(lines, colours):
        line.set_color(c)
