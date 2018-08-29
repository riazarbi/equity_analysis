TODO:

1. Bloomberg scraper script to become currency-aware  
2. Migrate dimensions into a parameters file; build in functionality for  other sources. Note - each source will have unique index codes, metadtaa, fundamental and market data fields, but not unique date fields. Do we need date, ISIN dimensions? ISINs is computed at runtime and date is defined by start and end.
3. Document the features of the datalog for point-in-time datasets, total eraseablity and portability


# Technical Document: A fully replicable Equity backtesting workflow

_Created by Riaz Arbi in fulfilment of the dissertation requirements for an MSc in Data Science at the University of Cape Town_

__This codebase is being heavily modified weekly. You should assume that the structure and functionality will vary significantly in the immediate future.__  

# Project Purpose
This paper provides a working example of an equity backtest that has been conducted in R according to the principles of transparency and reproducability in research. It provides working code to conduct the full analysis chain of equity backtesting in a way that is totally automated and modifiable. The companion git repository to this paper can be cloned and, with minimal modifications, new backtests can be created that avoid the pitfalls of common statistical biases. It is hoped that this work will make quality research into the cross section of equity returns more accessible to practitioners.

## Principles
This project favours transparency and customizability over ease of use. For a sophisticated, easy to use backtesting environment see [Zipline](https://github.com/quantopian/zipline) or [QSTrader](https://github.com/mhallsmoore/qstrader). 

This project aims to be -  
- Totally transparent in the flow and transformation of data  
- Low-level in terms of dependencies  
- Highly customizable  
- Easy to set up in any environment  

## Intended Audience

This project should be useful to:

-  Finance students at all levels wanting to conduct statistically rigorous equity backtests
-  Post graduates and academics looking to conduct research on a common platform to facilitate replication and peer review
-  Research professionals looking to build out an in-house backtesting environment for proprietary equity investment strategies
-  Equity researchers looking for a bridge between Excel-based research and `R` or `python`.

## Included Data
This repository __does not include any equity data__, but it does include working scripts to automatically extract data from data vendors, and to save that data on a well-formed way. It is assumed that the user will acquire data using the scripts which have been included in this repository. This will only be possible if a user has access to the relevant data services - namely Bloomberg, DataStream or iNet.

### The dimensions directory

The dimensions directory contains dimensions relevant to the backtest. These dimensions are organized on a per-file basis. For instance, the `fundamental_fields.csv` file contains a list of fundamental Bloomberg fields salient to the backtest.

# Replication and Extension to other use cases

To replicate the results of this paper, simply clone this git repository to your computer, and run each script in sequence. 

To replicate the results of this paper on another equity index, change the 'indexes.csv' dimension in the `dimensions` directory to another Bloomberg index code.

To alter the algorithm..... TBC.

# Code flow
The scripts in this repository are written in a procedural manner. That is, it is intended that parameters are set prior to execution and that the scripts are executed noninteractively. The code logic flows down each script in a linear manner wherever possible. This style facilitates the auditing of how data is manipulated as it flows through the code logic.

It should be possible to modify these scripts to run in a scheduled, automated fashion with the assistance of a scheduling daemon such as `cron` or `anacron`.

## Limiting the number of code files
Wherever possible, the code is kept in a single file. Code is split between several files when the contents of the files do not naturally run as the same execution batch. For instance, in an academic setting, data is often collected from vendors through the use of shared terminals, often situated in a library or laboratory. It does not make sense to include the data collection code with downstream processing code because downstream processing can be done on a another machine at a different time, freeing up the terminal for another user. 

## Chunking
Researchers are often limited by their computing reosurces. Most of the time, research is conducted on consumer hardware with limited amounts of RAM. To accommodate this, workloads are chunked or run sequentially to limit RAM consumption. The tradeoff is that the code is quite slow, because there are more disk reads and writes, and parallelization is not utilized fully.

## Sequential list of procedures

1. Index, ticker market, fundamental and meta data is collected from Bloomberg via the R Rblpapi package and saved to a log directory.
2. The logfiles are transformed into three datasets -  
  a. Ticker market and fundamental data  
  b. Ticker metadata  
  c. Monthly index constituent lists  
3. Backtest parameters are defined  
  a. Target index  
  b. Date range  
  c. Salient ticker metrics  
  d. Rebalancing periodicity  
4. A portfolio constituent weighting criterion is defined  
  a. For instance, "this portfolio will weight the constituents of the index according to their Price to Book ratio."  
  b. This weighting can be any mathematical function, and can contain binary statements such as "weight the constituents equally of their price to book raito is greate than 2, otherwise weight them as zero."  
4. A master dataset is loaded into memory containing the data relevant only to the backtest parameters  
5. The code steps through the backtest date range in increments related to the rebalancing periodicity, computing the portfolio weights for each period and appending these weights to a two dimensional `holdings matrix` with the dimensions `ticker(i -> n)` , `date(j -> m)`, where `entry(i,j)` corresponds to the weight of `ticker(i)` in the portfolio on `date(j)`.  
6. A `return matrix` is computed with the same dimensions as the `holdings matrix`, but with `entry(i,j)` corresponding to the total return of ticker(i) at time (j+1) / total return index of ticker(i) at time (j). That is, each `entry(i,j)` tells us the total return that ticker(i) would have in the next period.  
7. The `holdings matrix` and `return matrix` are multipied together to compute the `periodic portfolio return` (summed column-wise), the `lifetime ticker contribution to portfolio return` (multiplied row-wise) and the `lifetime portfolio return` (sumproduct).  
8. These returns are used to compute various risk measures, Sharpe Ratio chief among them.  
9. The risk and return results and various matrices and backtest parameters are bundled into an .RData object that can be passed to a report generator for automated reporting.

# Data flow
The data query script saves four kinds of dataset to the `datalog` folder. These are -  

1. Index constituents for a certain date  
2. Ticker metadata for an arbitrary list of tickers  
3. Fundamental data for a ticker for an arbitrary date range  
4. Market data for a ticker for an arbitrary date range  

The files in the `datalog` directory can be identified for their filename. The `filename` contains the following substrings, where each substring is separated by two underscores (ie `__`)  

* substring1: timestamp (integer-represented UNIX epoch time)  
* substring2:  source (eg. BLOOMBERG, REUTERS)  
* substring3: data type (eg. constituent list data, ticker market data, ticker fundamental data, metadata array)  
* substring4: data label (eg. 20120101_TOP40; ANG SJ) 

A sample filename is `1029384859940__BLOOMBERG__constituent_list_data__20121001_TOP40.csv`

This file naming convention allows the datalog to be searchable by a conbination of substrings. This is leveraged by the dataset builder to transform the datalog into well-formed datasets.

# Development Status

## Versioning and Support
This project will continually change. A user is advised to compare between commits on github to see how the code changes over time. It is anticipated that this codebase will be altered significantly and rapidly until August 2018; thereafter development will slow. A good way to check the development status is look at the github commit logs.

It is advised that a user of this codebase fork the repository in order to stabilize their codebase at a particular point in time. When sharing your work, you can facilitate peer review and replication by including access to your forked repository.

There is no guaranteed support for this project past August 2018, but users are free to fork and develop it on their own. Wherever possible, the code is written in R, python and bash to ensure compatibility with generally avaiailable computer software for the forseeable future.

###  Project Personnel, Reporting and Responsibility
This codebase is being built out by Riaz Arbi, who can be contacted via riazarbi.github.io I take no responsibility for any errors contained in this code. A user of this code should verify each line of the codebase to ensure that it operates as expected.

### Intellectual Property Statement
This is the intellectual property of Riaz Arbi. This code will be released under a strong copyleft license. A license is included in the repository.


### Milestones and Project Deliverables
The end goal of this project is to have a complete equity backtesting workflow by August 2018. This project is being built solely by Riaz Arbi.

| Date | Milestone | Status |
|--------------|----------------------------------|-----------|
| October 2017 | Set up server with necessary dependencies | Complete | 
| November 2017 |Build Bloomberg Excel VBA Workbook to scrape data | Complete |
| January 2018 |Scrape Bloomberg terminal for data | Complete |
| January 2018 |Merge raw files into single csv files | Complete |
| February 2018 |Clean, join and interpolate data | In Progress |
| February 2018 |Transform raw data into Sqlite file | In Progress |
| March 2018 | Build out code to control for backtesting biases (look-ahead, survivorship etc) |  Not Started |
| April 2018 | Perform a case study backtest | Not Started | 
| May 2018 | Debug, refactor, refine | Not Started |
| June 2018 | Add additional data sources: iNet, Datastream | Not Started |
| July 2018 | Replicate case study backtest on alternative data and compare differential results | Not Started |
| August 2018 | Create second backtest and document workflow steps and benchmark timing | Not Started |

## Requirements Specification
1. Hardware
  - My development machine is a modern generic x86 workstation running Ubuntu 16.04 LTS. 
  - It is conneced to the internet
  - It is recommended that you have at least 4gb of RAM for data transformation; large datasets will require significantly more.

2. Software
  - At a minimum, you need to have python 3.5, R version 3.4.3 running on a modern x86 linux distribution.
  - A complete browser-based software suite can be set up automatically using this repo https://github.com/riazarbi/serversetup. The scripts contained in that repo will, among other things, set up RStudio Server, JupyterHub with Python 3.5, NextCloud and secure the server with approproate firewall rules. It can also set up a Tor hidden ssh service, private key access and automated standard user creation.

3. Data 
  - This project presupposes that a user has access to a data vendor subscription. It contains VBA enabled Excel workbooks that can automatically extract relevant data from the Bloomberg Excel Add-In. It is anticipated that support will be added for similar automated VBA-based extraction of DataStream and iNet sources.

4. Maintenance
  - The user will have to periodically upload new data if this environment is to be converted into a production trading system. The Excel scrapers can easily be modified to perform incremental additons to `raw` directories but a produciton system should probably talk directly to an API using a native data vendor API.


