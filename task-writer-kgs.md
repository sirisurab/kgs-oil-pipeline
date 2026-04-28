<project>kgs</project>

<datasets>
	<dataset>
		<name>oil_leases_2020_present.txt</name>
		<file-path>./data/external/oil_leases_2020_present.txt</file-path>
		<data-dictionary>./references/kgs_archives_data_dictionary.csv</data-dictionary>
		<description>All oil production from Kansas Oil and Gas Leases from 2020 through Sep 2025</description>
		<instructions>
			<instruction>The file contains all production leases with a URL (column="URL") for
			each lease. Extracting oil production data for all wells in each lease will require
			the design of a download workflow to be included as part of the data-pipeline
			component for data acquisition. (this workflow must be executed in parallel using dask).
			Steps for the download workflow are as follows:
			 1. Extract the lease ID from the URL. For example, the URL for lease "1001135839"
			    is "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839" and the
			    lease ID is "1001135839".
			 2. Make an HTTP GET request to the MonthSave page for the lease:
			    "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc=1001135839"
			 3. Parse the HTML response using BeautifulSoup to find the download link — it is
			    an anchor tag whose href contains "anon_blobber.download". For example:
			    "https://chasm.kgs.ku.edu/ords/qualified.anon_blobber.download?p_file_name=lp564.txt"
			 4. Make a second HTTP GET request to that download URL and save the response
			    content to project_root/data/raw using the filename from the p_file_name
			    parameter (e.g. lp564.txt).
			 5. The data file is in .txt format, but it can be treated as .csv format. The data
			    dictionary for the data file is available in this file
			    ./references/kgs_monthly_data_dictionary.csv
			</instruction>
			<instruction>Before constructing the list of lease URLs to download, filter the lease
			index to leases with MONTH-YEAR >= 1-2024 (i.e. year component >= 2024). The
			MONTH-YEAR column format is "M-YYYY" (e.g. "1-2024"). Extract the year by splitting
			on "-" and taking the last element. Deduplicate by URL after filtering — the index
			has one row per month per lease, not one row per lease.
			</instruction>
			<instruction>Rate-limit parallel downloads to a maximum of 5 concurrent workers via
			Dask. Add a 0.5 second sleep per download worker to avoid overloading the KGS server.
			</instruction>
		</instructions>
	</dataset>
</datasets>

<data-filtering>
	<constraint>The ingest stage must filter rows to the target date range
	(year >= 2024) after reading raw files. Raw lease files downloaded from KGS
	contain full production history going back to the 1960s regardless of the
	acquire filter. Extract year from MONTH-YEAR column (format "M-YYYY", split
	on "-", take last element). Drop rows where year &lt; 2024 and rows where
	the year component is not numeric (e.g. "-1-1965", "0-1966").</constraint>
</data-filtering>

<deduplication>
  <rule>
  KGS lease files do not contain a revision column. If duplicate rows exist for
  the same (entity, production_date) after set_index, keep the first occurrence
  per (entity, production_date) — no tie-breaking by a revision field is needed.
  A bare drop_duplicates() with no subset must not be used.
  </rule>
</deduplication>

<data-dictionaries>
  <file>references/kgs_monthly_data_dictionary.csv</file>
  <file>references/kgs_archives_data_dictionary.csv</file>
  <description>Use kgs_monthly_data_dictionary.csv as the authoritative data-type source for all
  ingest, transform, and features task specs; data-type and nullable values must be
  mapped to pandas types per the Data dictionary ADR in ADRs.md
  </description>
</data-dictionaries>

<context>
The pipeline package is named `kgs_pipeline`.
All test files must be under `tests/`.
  Data files:
    - data/external/oil_leases_2020_present.txt  <- lease index
  Reference files in references/:
    - kgs_archives_data_dictionary.csv
    - kgs_monthly_data_dictionary.csv
</context>
