---
layout: post
title: "Analyzing the Medicare Cost Report Data: Preparation"
date: 2020-01-25T10:35:56-06:00
categories: Analytics
tags: ["Python", "Medicare", "Dask"]
published: true
---

# About the Data

[Cost report data](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports) from the U.S. Centers for Medicare and Medicaid Services (CMS) are often used for healthcare research.  A significant issue with the data is that it is not released in a manner that is ready for comprehension and discovery.  Through this series of articles, I hope to help others with that journey.  These data provide valuable insight into the U.S. healthcare system, and CMS has broad influence on the behavior and practices of hospitals and clinicians providing care.

![RPT, ALPHA, NMRC and ROLLUP are the four major table types.](/analysis/media/mcr.png#floatright "MCR Table Types")

In its shared form, the public use file data take a lot of work to use for analysis and research, particularly if you are trying to piece together a broad view of data across the report.  You can't use Microsoft Excel with these files because they are too big, and if you can only open one at a time, good luck understanding all of the values, and matching them to specific hospitals!

Within the archives you may encounter up to four different types of csv file. For this article, we will work with three of them: ALPHA, NMRC and RPT. ALPHA contains all entries into the cost report forms that are alphanumeric, while NMRC contains only the numeric values from the forms.  The RPT file contains information about the report such as the provider and submission dates.  The RPT data is identified by a unique report record number which is used to link to all of the individual values in the ALPHA and NMRC files.

# Downloading Files

The downloadable public use files (PUFs) for hospitals go back to 1996, and are updated on a quarterly basis.  The files are available without the need for special accounts or licensing from the Healthcare Cost Report Information System (HCRIS) website.  They are delivered in zip files containing comma separated value (CSV) text files stored by year. Within each archive you will find the three kinds of files we are looking for.

There are two hospital cost report forms used to report data to CMS. CMS-2552-96 was used until full transition to the updated CMS-2552-10 form.  The transitional years of 2010-2011 result in a set of files using the older form, as well as the newer form.  During this time frame, both files are needed to piece together all of the information applicable to each year.  Since the two forms are different, this presents a concern when matching specific fields to a database schema.  Fields may be added, removed or changed, which can cause data quality issues that prevent dependable analysis.

While data are available back to 1996, for our initial prototype work, we are only going to use the last two years.  As you add years you will run into other challenges, and once you add in data that uses the older form, you will add to those challenges.  For now, try to stay in the range of 2012 and later.  

Start by downloading the following two files:

- [2019 Public Use File](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Cost-Reports-by-Fiscal-Year-Items/HOSPITAL10-DL-2019)
- [2018 Public Use File](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Cost-Reports-by-Fiscal-Year-Items/HOSPITAL10-DL-2018)

Save these files to a folder called "data" in your script directory.

# Planning the Approach

Before trying to implement a solution, particularly if you are inclined to first start writing code, I recommend pausing and writing the general steps that you need to try to accomplish with your solution.  As we go through the general steps, our code should be solving the problems or needs that we've identified.  Focus on one problem at a time!

## Rough Sketch

![My sketch of a solution.](/analysis/media/mcr-unit01-preparation-approach.jpeg "Approaching the HCRIS Data")

## Capturing the Steps

For this approach, we are going to take care of getting the files for the script.  The next version of our script might take care of the download part.

Our script will need to:
- Unzip the files
- Read in the data
- Identify the fields using the metadata file
- Join the different file types together
- Save to a friendlier format for analysis

# Development Needs

{{< tech/tool/vscode >}}
{{< tech/lang/python >}}
{{< tech/lib/numpy >}}
{{< tech/lib/dask >}}

![Use the run cell feature in VS Code to prototype a solution.](/analysis/media/runcell.png#floatright "Using iPython in VS Code")

As you develop with VS Code, you can use the interactive Python features to help build a full script.  This allows running a small chunk of code at a time using the "Run Cell" feature that appears at any location where the ```%##``` text is found.  Rather than having to run the entire script each time you make a mistake or need to adjust something, iPython will allow you to rerun the chunk that needed adjustment.  

This is great when you have a very large dataset in memory and you do not want to have to reload everything to test simple output, or explore an aspect of a dataframe.  Sometimes you will need to figure out how to get to a piece of data, and this will be a huge time saver.

# Time to Code!

{{< exercise >}}
First, we need to import the libraries that are required.
{{< /exercise >}}

The `os` library will allow obtaining a list of files from the input directory.  The `zipfile` library will allow Python to open the zip files and extract the NMRC, ALPHA and RPT files from the archives.  The `dask` library is the tool that will allow importing the csv file into Python, and performing actions on the records. The `numpy` library will help assign data types to certain fields so that numeric values will maintain the intended format.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Import Libraries
import os
import zipfile
import dask.dataframe as dd
import numpy as np
{{< / highlight >}}

{{< exercise >}}
2.) Create directories, and set the values.
{{< /exercise >}}

The code below expects a folder `data` that has the zipped files in it.  It also will expect an `output` folder as the destination for the prepared data files to be used by an analysis script.  Please make sure these folders exist in the location you are building the script.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Set Paths
input_folder = "data"
output_folder = "output"
{{< / highlight >}}

{{< exercise >}}
3.) Loop through each zip file in the output directory, and extract the csv files.
{{< /exercise >}}

This step doesn't require a lot of explanation.  The code grabs the list of files from the input directory, and then unzips them. You could have many years in this directory, and the script would handle them.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Unzip Archives
for file in os.listdir(input_folder):
    filename = os.path.join(input_folder,file)
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(os.path.abspath(filename),"r") as zipped:
            zipped.extractall(input_folder)
{{< / highlight >}}

{{< exercise >}}
4.) Set up the field names expected from each type of csv file.
{{< /exercise >}}

This is where you specify the field names used when importing the CSV data.  I used the standard ones that I've used in the past, but you could make these more user friendly.  Just be aware that you will need to make the same adjustments to field names below, particularly on the identifier ("key") fields.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Setup Columns
colRPT = ['RPT_REC_NUM','PRVDR_CTRL_TYPE_CD','PRVDR_NUM','NPI','RPT_STUS_CD', \
            'FY_BGN_DT','FY_END_DT','PROC_DT','INITL_RPT_SW','LAST_RPT_SW', \
            'TRNSMTL_NUM','FI_NUM','ADR_VNDR_CD','FI_CREAT_DT','UTIL_CD','NPR_DT', \
            'SPEC_IND','FI_RCPT_DT']
colNMRC = ['RPT_REC_NUM','WKSHT_CD','LINE_NUM','CLMN_NUM','ITM_VAL']
colALPHA = ['RPT_REC_NUM','WKSHT_CD','LINE_NUM','CLMN_NUM','ITM_VAL']
{{< / highlight >}}

{{< exercise >}}
5.) Read each type of csv file into a dataframe.
{{< /exercise >}}

When reading in the data into the dataframe, the file path accepts the `*` symbol as a wildcard.  This allows you to match a group of files rather than specifying each file, or looping through the load.

This particular match with work on files from 2010 forward.  If you have additional years, you can adjust.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Read data into Dask dataframe (201X Files)
dfRPT = dd.read_csv(input_folder + '/*_201*_RPT.CSV', header=None, names=colRPT, dtype='str')
dfNMRC = dd.read_csv(input_folder + '/*_201*_NMRC.CSV', header=None, names=colNMRC, dtype='str')
dfALPHA = dd.read_csv(input_folder + '/*_201*_ALPHA.CSV', header=None, names=colALPHA, dtype='str')
{{< / highlight >}}

{{< exercise >}}
6.) Gather the metadata that will help identify fields in the HCRIS data.
{{< /exercise >}}

I have prepared a field list that contains metadata on the cost report fields.  This is shared through the [OHANA Project](https://ohanaproject.org), and updates are welcome.  One of the significant burdens of the cost report data is having a reliable way to identify all of the fields, and crosswalk fields from the 1996 version of the form to the 2010.  It's not perfect, but is better that where I started a few years ago.

In future articles I will discuss approaches to working with data from the older version of the form.  To start, I recommend starting with 2012, and you will only have data from the CMS-2552-10 form.  Remember, 2010 and 2011 were transition years where both forms were accepted.  This means your results may be skewed for those years since some data are reported using the older format.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Load field descriptors
dfFIELDS = dd.read_csv('https://raw.githubusercontent.com/ohana-project/HCRISFields/master/fields.csv', \
                dtype='str')
{{< / highlight >}}

{{< exercise >}}
7.) Force important numeric data to a string data type.
{{< /exercise >}}

When using dataframes, you need to be aware that sometimes your data will be altered.  It may or may not be important to you, but if you have numeric strings with leading zeros, those fields will likely get converted to an integer type. This means that those zeros will disappear.  For actual numbers, this isn't a concern.  

In our case, the leading zeros are important, so you will want to force these key fields to a string type to preserve the format.  For example, LINE_NUM will appear as a six character string (`00100`) and the zeros are VERY important.  In this case, `00100` refers to line `1` of the form, but when converted to an integer this would instead refer to line `100`!

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Match field data types
dtypes = {
    'WKSHT_CD':np.str,
    'LINE_NUM':np.str,
    'CLMN_NUM':np.str
}
dfNMRC = dfNMRC.astype(dtypes)
dfALPHA = dfALPHA.astype(dtypes)
dfFIELDS = dfFIELDS.astype(dtypes)
{{< / highlight >}}

{{< exercise >}}
8.) Join the numeric data to the field metadata.  Do the same for alphanumeric dataframe.
{{< /exercise >}}

Now it is time to start linking the field metadata to the dataframes containing the form data.  This may take significant time depending on the number of years that you are processing.  We keep it moving by only working on two years for the example.

{{< highlight python3 "linenos=table,hl_lines=1 5,linenostart=1" >}}
#%% Merge Field Information with Numeric Data
dfNMRC = dfNMRC.merge(dfFIELDS, left_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'], \
    right_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'])

#%% Merge Field Information with Alpha Data
dfALPHA = dfALPHA.merge(dfFIELDS, left_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'], \
    right_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'])
{{< / highlight >}}

{{< exercise >}}
9.) Join the report metadata to the numeric and alphanumeric dataframes.
{{< /exercise >}}

The `RPT` table contains the overall form information, so we will join this to each set of values. This essentially creates a denormalized record that supplies the form information and field metadata for each value.  While it is true that this isn't how we would typically store information in a database, this format makes it FAR easier to query for analysis using a simple script.

This will let you concentrate on the analysis rather than the relationships, performance and linking of data in your script.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Merge Alpha and Numeric
dfNMRC = dfNMRC.merge(dfRPT, left_on='RPT_REC_NUM', right_on='RPT_REC_NUM')
dfALPHA = dfALPHA.merge(dfRPT, left_on='RPT_REC_NUM', right_on='RPT_REC_NUM')
{{< / highlight >}}

{{< exercise >}}
10.) Merge the numeric and alphanumeric dataframes into one.
{{< /exercise >}}

Now that we have flattened the data for all values, we can merge them all into one common dataframe.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Combine to Final Dataframe
dfFINAL = dfALPHA.append(dfNMRC)
{{< / highlight >}}

{{< exercise >}}
11.) Export the merged data for easier access by analysis scripts.
{{< /exercise >}}

Lastly, we export the results to compressed parquet files, and in the next article, we will read in these files and start analyzing.  If you save these files, you can reuse them without going through all of the preparation steps.

When the new yearly files are released, you will need to rebuild the files.  There are techniques to be able to merge updates so you don't have to start from scratch, but that is a more advanced task so we can explore that in a later article.

{{< highlight python3 "linenos=table,hl_lines=1,linenostart=1" >}}
#%% Exporting to Parquet
dfFINAL.to_parquet(output_folder+"mcr", compression={"name": "gzip", "values": "snappy"})
{{< / highlight >}}

# Review

While I encourage you to build the script as you go through the article, the full, finished script is linked below.  There is a lot you could do to make this script more robust, and that is an opportunity for you to develop your coding skills.  For example, the pieces are here to be able to download the 2018 and 2019 zip files directly from the CMS website.

You do need to be aware that repeatedly downloading from any site is not really being a good neighbor on the internet.  If you do set up an automatic download, make sure to check to see if your file already exists in the directly before attempting the download.  This way you only download what is missing, and you save some time, bandwidth and money in the process.

# Next Steps

In the next article we will start exploring, cleaning and performing some initial analysis.  We will check for problems with the data, generate summaries of the data, and see if there are some initial areas of interest for deeper analysis.  Follow me on Twitter for updates!

# Source Code

- [Github project for this article.](https://github.com/dcinformatics/HCRIS-Preparation)
- [Direct link to finished script.](https://raw.githubusercontent.com/dcinformatics/HCRIS-Preparation/master/preparation.py)

# References

- [Centers for Medicare &amp; Medicaid Services Cost Reports](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports)
- [Open Health Analytics Project, HCRIS Field Metadata](https://github.com/ohana-project/HCRISFields)