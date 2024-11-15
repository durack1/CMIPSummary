#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 05:57:34 2024

PJD 28 Aug 2024     - Started
PJD 28 Aug 2024     - Working version for CMIP3, 5 and 6
PJD 28 Aug 2024     - Updated to count tables as well
PJD 28 Aug 2024     - Removed "CMIP6_grids.json"; added tableFiles.sort()
PJD 28 Aug 2024     - Added datetime for reporting
PJD 28 Aug 2024     - Added mip-cmor-tables
PJD 12 Nov 2024     - Updated to deal with CMIP3 A5 table specifics;
                      explicitly highlight droppedVariables
PJD 13 Nov 2024     - Augmented cmipCoords with more CMIP5-era coords to
                      exclude
PJD 14 Nov 2024     - Augmented for all existing CMOR tables
                    TODO: Determine variables written to CMIP3,
                     5 and 6 ESGF archives

@author: durack1
"""

# %% imports
import datetime
import glob
import hashlib
import json
import os

# %% function defs


def readJsonTable(tableFilePath) -> dict:
    with open(tableFilePath, "r") as f:
        aDic = json.load(f)

    return aDic


def readTxtTable(tableFilePath) -> dict:
    """
    function lifted from the CMOR2.8 library, see
    https://github.com/PCMDI/cmor/blob/CMOR-2.8.0/Lib/check_CMOR_compliant.py#L119-L199
    """

    lists_kw = [
        "requested",
        "bounds_requested",
        "z_factors",
        "z_bounds_requested",
        "dimensions",
        "required",
        "ignored",
        "optional",
    ]
    f = open(tableFilePath, "r", encoding="utf-8")
    blob = f.read().encode("utf-8")
    m5 = hashlib.md5(blob)
    m5 = m5.hexdigest()
    f.seek(0)
    ln = f.readlines()
    f.close()
    header = 1
    gen_attributes = {"actual_md5": m5}
    while header:
        l = ln.pop(0)[:-1]
        l = l.strip()
        if l == "" or l[0] == "!":
            continue
        sp = l.split("_entry")
        if len(sp) > 1:
            ln.insert(0, l + "\n")
            header = 0
            continue
        sp = l.split(":")
        kw = sp[0]
        st = "".join(sp[1:])
        st = st.split("!")[0].strip()
        if st[0] == "":
            st = st[1:-1]
        if kw in gen_attributes:
            if isinstance(gen_attributes[kw], str):
                gen_attributes[kw] = [gen_attributes[kw], st]
            else:
                gen_attributes[kw].append(st)
        else:
            gen_attributes[kw] = st
    e = {}  # entries dictionnary
    while len(ln) > 0:
        l = ln.pop(0)
        sp = l.split("_entry:")
        entry_type = sp[0]
        entry = sp[1].strip()
        if not entry_type in e:
            e[entry_type] = {}
        e[entry_type][entry] = e[entry_type].get(entry, {})

        # print(e[entry_type][entry])
        cont = 1
        while cont:
            l = ln.pop(0)[:-1]
            l = l.strip()
            if l == "" or l[0] == "!":
                if len(ln) == 0:
                    cont = 0
                continue
            sp = l.split("_entry:")
            if len(sp) > 1:
                ln.insert(0, l + "\n")
                cont = 0
            sp = l.split(":")
            kw = sp[0].strip()
            val = ":".join(sp[1:]).split("!")[0].strip()
            # print("dic is:", e[entry_type][entry])
            if kw in e[entry_type][entry]:
                if kw in lists_kw:
                    e[entry_type][entry][kw] = "".join(e[entry_type][entry][kw])
                e[entry_type][entry][kw] += " " + val
            else:
                e[entry_type][entry][kw] = val
                # print("After:", e[entry_type][entry][kw])
            if kw in lists_kw:
                # print("splitting:", kw, e[entry_type][entry][kw].split())
                e[entry_type][entry][kw] = e[entry_type][entry][kw].split()
            if len(ln) == 0:
                cont = 0
    e["general"] = gen_attributes

    return e


def reportMipEra(tablePath, mipId, exclusionList=[]) -> None:
    print("Processing:", mipId)
    tableFiles = glob.glob(os.path.join(tablePath))
    tableFiles.sort()  # add sort before processing
    # catch non-Table files
    nonTable = [
        "CMIP5_grids",  # CMIP5
        "CMIP6_coordinate.json",
        "CMIP6_formula_terms.json",
        "CMIP6_grids.json",
        "CMIP6_input_example.json",
        "CMIP6_CV.json",
        "CORDEX-CMIP6_coordinate.json",  # CORDEX (CMIP6)
        "CORDEX-CMIP6_CV.json",
        "CORDEX-CMIP6_formula_terms.json",
        "CORDEX-CMIP6_grids.json",
        "CORDEX-CMIP6_remo_example.json",
        "md5s",  # CMIP5
        "CORDEX_grids",  # CORDEX (CMIP5)
        "GeoMIP_grids",  # GeoMIP (CMIP5)
        "LUCID_grids",  # LUCID (CMIP5)
    ]
    # deal with IPCC_table_A5
    specialTable = [
        "IPCC_table_A5",
    ]

    varCount, tableCount = [0 for _ in range(2)]
    for table in tableFiles:
        # check for non-Table files
        if table.split("/")[-1] in nonTable:
            print("skipping:", table)
            print("-----")
            continue
        print("table:", trimPath(table))
        tableCount = tableCount + 1
        if mipId in ["CMIP6", "CMIP6Plus", "cordex-cmip6"]:
            aDic = readJsonTable(table)
            key = "variable_entry"
        else:
            aDic = readTxtTable(table)
            key = "variable"
        if table.split("/")[-1] in specialTable:
            print("CMIP3 Table_A5")
            cnt = trimReportVar(aDic, key, exclusionList)
        else:
            cnt = trimReportVar(aDic, key)
        varCount = varCount + cnt
        print("-----")
    print("total", mipId, "tables:", tableCount, "vars:", varCount)


def trimPath(filePath):
    """
    trim local path
    """
    filePath = filePath.replace("/Users/durack1/sync/git/", "")

    return filePath


def trimReportVar(tableDict, key, exclusionList=[]) -> list:
    """
    Take a table dictionary, parse the variable subDict and report
    """
    cmipCoords = [
        "a",
        "a_bnds",
        "ap",
        "ap_bnds",
        "az",
        "az_bnds",
        "b",
        "b_bnds",
        "bz",
        "bz_bnds",
        "depth",
        "depth_c",
        "eta",
        "href",
        "k_c",
        "nsigma",
        "p0",
        "ptop",
        "sigma",
        "sigma_bnds",
        "z1",
        "z2",
        # "zfull",  # fixed field CMIP6_fx.json
        # "zhalf", # fixed field CMIP6_CF3hr.json
        "zlev",
        "zlev_bnds",
    ]
    varKeys = list(tableDict[key].keys())
    # trim out coord vars
    varList = [x for x in varKeys if x not in cmipCoords]
    lenVarList = len(varList)
    print("len(varList):", lenVarList)
    # if exclusionList not none - hack for IPCC_table_A5
    if exclusionList:
        varList = [x for x in varKeys if x not in exclusionList]
        varList.extend(["rsf", "rsfcs", "rlf", "rlfcs"])
        lenVarList = len(varList)
        print("len(varList):", lenVarList)
    # varList.sort()  # add sort before processing
    print("varList:", varList)
    # print dropped vars
    droppedVarList = list(set(varKeys) - set(varList))
    print("droppedVarList:", droppedVarList)

    return lenVarList


# %% define table_A5 exclusion list
varListA5 = [
    "rlftoaa_a",
    "rlftoaa_bc",
    "rlftoaa_co2",
    "rlftoaa_lo",
    "rlftoaa_n",
    "rlftoaa_o",
    "rlftoaa_s",
    "rlftoaa_sd",
    "rlftoaa_si",
    "rlftoaa_so",
    "rlftoaa_sun",
    "rlftoaa_to",
    "rlftoaa_v",
    "rlftoaacs_a",
    "rlftoaacs_bc",
    "rlftoaacs_co2",
    "rlftoaacs_lo",
    "rlftoaacs_n",
    "rlftoaacs_o",
    "rlftoaacs_s",
    "rlftoaacs_sd",
    "rlftoaacs_si",
    "rlftoaacs_so",
    "rlftoaacs_sun",
    "rlftoaacs_to",
    "rlftoaacs_v",
    "rlftoai_a",
    "rlftoai_bc",
    "rlftoai_co2",
    "rlftoai_lo",
    "rlftoai_n",
    "rlftoai_o",
    "rlftoai_s",
    "rlftoai_sd",
    "rlftoai_si",
    "rlftoai_so",
    "rlftoai_sun",
    "rlftoai_to",
    "rlftoai_v",
    "rlftoaics_a",
    "rlftoaics_bc",
    "rlftoaics_co2",
    "rlftoaics_lo",
    "rlftoaics_n",
    "rlftoaics_o",
    "rlftoaics_s",
    "rlftoaics_sd",
    "rlftoaics_si",
    "rlftoaics_so",
    "rlftoaics_sun",
    "rlftoaics_to",
    "rlftoaics_v",
    "rlftropa_a",
    "rlftropa_bc",
    "rlftropa_co2",
    "rlftropa_g",
    "rlftropa_lo",
    "rlftropa_n",
    "rlftropa_o",
    "rlftropa_s",
    "rlftropa_sd",
    "rlftropa_si",
    "rlftropa_so",
    "rlftropa_sun",
    "rlftropa_to",
    "rlftropa_v",
    "rlftropacs_a",
    "rlftropacs_bc",
    "rlftropacs_co2",
    "rlftropacs_g",
    "rlftropacs_lo",
    "rlftropacs_n",
    "rlftropacs_o",
    "rlftropacs_s",
    "rlftropacs_sd",
    "rlftropacs_si",
    "rlftropacs_so",
    "rlftropacs_sun",
    "rlftropacs_to",
    "rlftropacs_v",
    "rlftropi_a",
    "rlftropi_bc",
    "rlftropi_co2",
    "rlftropi_g",
    "rlftropi_lo",
    "rlftropi_n",
    "rlftropi_o",
    "rlftropi_s",
    "rlftropi_sd",
    "rlftropi_si",
    "rlftropi_so",
    "rlftropi_sun",
    "rlftropi_to",
    "rlftropi_v",
    "rlftropics_a",
    "rlftropics_bc",
    "rlftropics_co2",
    "rlftropics_g",
    "rlftropics_lo",
    "rlftropics_n",
    "rlftropics_o",
    "rlftropics_s",
    "rlftropics_sd",
    "rlftropics_si",
    "rlftropics_so",
    "rlftropics_sun",
    "rlftropics_to",
    "rlftropics_v",
    "rlfttoaa_g",
    "rlfttoaacs_g",
    "rlfttoai_g",
    "rlfttoaics_g",
    "rsftoaa_a",
    "rsftoaa_bc",
    "rsftoaa_co2",
    "rsftoaa_lo",
    "rsftoaa_n",
    "rsftoaa_o",
    "rsftoaa_s",
    "rsftoaa_sd",
    "rsftoaa_si",
    "rsftoaa_so",
    "rsftoaa_sun",
    "rsftoaa_to",
    "rsftoaa_v",
    "rsftoaacs_a",
    "rsftoaacs_bc",
    "rsftoaacs_co2",
    "rsftoaacs_lo",
    "rsftoaacs_n",
    "rsftoaacs_o",
    "rsftoaacs_s",
    "rsftoaacs_sd",
    "rsftoaacs_si",
    "rsftoaacs_so",
    "rsftoaacs_sun",
    "rsftoaacs_to",
    "rsftoaacs_v",
    "rsftoai_a",
    "rsftoai_bc",
    "rsftoai_co2",
    "rsftoai_lo",
    "rsftoai_n",
    "rsftoai_o",
    "rsftoai_s",
    "rsftoai_sd",
    "rsftoai_si",
    "rsftoai_so",
    "rsftoai_sun",
    "rsftoai_to",
    "rsftoai_v",
    "rsftoaics_a",
    "rsftoaics_bc",
    "rsftoaics_co2",
    "rsftoaics_lo",
    "rsftoaics_n",
    "rsftoaics_o",
    "rsftoaics_s",
    "rsftoaics_sd",
    "rsftoaics_si",
    "rsftoaics_so",
    "rsftoaics_sun",
    "rsftoaics_to",
    "rsftoaics_v",
    "rsftropa_a",
    "rsftropa_bc",
    "rsftropa_co2",
    "rsftropa_g",
    "rsftropa_lo",
    "rsftropa_n",
    "rsftropa_o",
    "rsftropa_s",
    "rsftropa_sd",
    "rsftropa_si",
    "rsftropa_so",
    "rsftropa_sun",
    "rsftropa_to",
    "rsftropa_v",
    "rsftropacs_a",
    "rsftropacs_bc",
    "rsftropacs_co2",
    "rsftropacs_g",
    "rsftropacs_lo",
    "rsftropacs_n",
    "rsftropacs_o",
    "rsftropacs_s",
    "rsftropacs_sd",
    "rsftropacs_si",
    "rsftropacs_so",
    "rsftropacs_sun",
    "rsftropacs_to",
    "rsftropacs_v",
    "rsftropi_a",
    "rsftropi_bc",
    "rsftropi_co2",
    "rsftropi_g",
    "rsftropi_lo",
    "rsftropi_n",
    "rsftropi_o",
    "rsftropi_s",
    "rsftropi_sd",
    "rsftropi_si",
    "rsftropi_so",
    "rsftropi_sun",
    "rsftropi_to",
    "rsftropi_v",
    "rsftropics_a",
    "rsftropics_bc",
    "rsftropics_co2",
    "rsftropics_g",
    "rsftropics_lo",
    "rsftropics_n",
    "rsftropics_o",
    "rsftropics_s",
    "rsftropics_sd",
    "rsftropics_si",
    "rsftropics_so",
    "rsftropics_sun",
    "rsftropics_to",
    "rsftropics_v",
    "rsfttoaa_g",
    "rsfttoai_g",
    "rsfttoaics_g",
]

# %% start to iterate over tables
timeNow = datetime.datetime.now()
timeFormat = timeNow.strftime("%y%m%d_%H%M%S")
print("-----")
print("Process time:", timeFormat)
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/cmip3-cmor-tables/Tables/*", "CMIP3", varListA5
)  # good 143
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/cmip5-cmor-tables/Tables/*", "CMIP5"
)  # good 986 (zfull, zhalf added back in)
print("-----")
print()
print("-----")
reportMipEra("/Users/durack1/sync/git/cmip6-cmor-tables/Tables/*", "CMIP6")  # good 2062
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/mip-cmor-tables/Tables/*", "CMIP6Plus"
)  # good 2049
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/cfmip1-cmor-tables/Tables/*", "cfmip1"
)  # good 149
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/c-lamp1-cmor-tables/Tables/*", "c-lamp1"
)  # good 88
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/iaemip1-cmor-tables/Tables/*", "iaemip1"
)  # good 146
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/cordex-cmor-tables/Tables/*", "cordex"
)  # good 207
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/geomip-cmor-tables/Tables/*", "geomip"
)  # good 1142
print("-----")
print()
print("-----")
reportMipEra("/Users/durack1/sync/git/lucid-cmor-tables/Tables/*", "lucid")  # good 979
print("-----")
print()
print("-----")
reportMipEra("/Users/durack1/sync/git/pmip3-cmor-tables/Tables/*", "pmip3")  # good 810
print("-----")
print()
print("-----")
reportMipEra(
    "/Users/durack1/sync/git/cordex-cmip6-cmor-tables/Tables/*",
    "cordex-cmip6",
)  # good 565
