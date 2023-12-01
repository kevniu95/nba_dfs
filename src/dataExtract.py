import os
import pathlib
from .dataImport import initYearDict
import pandas as pd
from typing import Dict, Any, Set
from util.constants import *
from util.logger_config import setup_logger
import re
import glob

logger = setup_logger(__name__)

# ===========
# Extract and save functions
# ===========
def getConcatDateFrame(yearDict : Dict[Any, Any], dataSource : DataSource, dfsSite : DfsSite) -> pd.DataFrame:
    """
    From yearDict, extract the dataframe for the given dataSource and dfsSite, 
        and concatenate all dates together into one dataframe.
    """
    frame_all = []
    for date, v in yearDict.items():
        curr_df = v[dataSource.value][dfsSite.value]
        curr_df['Date'] = date
        frame_all.append(curr_df)
    return pd.concat(frame_all)

def extractFramesByDataSource(yearDict : Dict[Any, Any], dfsSite : DfsSite) -> Dict[str, pd.DataFrame]:
    """
    Returned:
        key: data source
        value: single dataframe for this year with all dates concatenated together
    """
    frames = {}
    for dataSource in [j for j in DataSource]:
        frames[dataSource.value] = getConcatDateFrame(yearDict, dataSource, dfsSite)
    return frames

def extractAndSaveYear(year : int) -> None:
    """
    'Extract' from pickle saved in dataImport module
    'Save' all dates into one csv for each year in data/import folder
    """
    yearDict = initYearDict(year)
    logger.info(f"Extracting data for {year}...")
    frames = extractFramesByDataSource(yearDict, DfsSite.DRAFTKINGS) # key is DataSource
    logger.info(f"Saving data for {year}...")
    for key, v in frames.items():
        v.to_csv(f'../data/import/{key}_{year}.csv', index=False)

# ===========
# Load Functions
# ===========
# Note save happens on conditional basis within load functions
def loadFrames(target_years : Set[int]) -> Dict[DataSource, pd.DataFrame]:
    frames = {}
    for dataSource in DataSource:
        saved_years = {int(re.search(r'_(\d{4})\.csv', i).group(1)) for i in glob.glob(f'../data/import/{dataSource.value}_*.csv')}
        missing_years = target_years - saved_years
        for year in missing_years:
            logger.info(f"Extracting and saving out data for {year}...")
            extractAndSaveYear(year)
        frames[dataSource.value] = loadSource(dataSource)
    return frames

def loadSource(dataSource : DataSource) -> pd.DataFrame:
    frames = {}
    for i in glob.glob(f'../data/import/{dataSource.value}_*.csv'):
        year = int(re.search(r'_(\d{4})\.csv', i).group(1))
        df = pd.read_csv(i)
        df['Season'] = year
        frames[year] = df
    return pd.concat(frames.values())
    
def standardizeNames(dfs : pd.DataFrame, ref : pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    dfs['Name'] = dfs['Name'].str.replace(r' [A-Z]$', '', regex=True)
    dfs['Name'] = dfs['Name'].str.replace(r'^(\S+\s\S+).*', r'\1', regex=True)

    ref['Player'] = ref['Player'].str.replace(r'^(\S+\s\S+).*', r'\1', regex=True)
    
    dfs['Team'] = convertTeamList(dfs['Team'], DataSource.DFS.value)
    ref.rename(columns={'Player' : 'Name', 'Tm' : 'Team'}, inplace=True)
    return dfs, ref

# ===========
# Export functions
# ===========
def exportToMakeLookup(df : pd.DataFrame, name_field : str, team_field : str, dataSource : DataSource) -> None:
    names = df[[name_field, team_field]].drop_duplicates()
    names[['FirstName', 'LastName']] = names[name_field].str.split(' ', expand=True)
    names.sort_values(by=['LastName','FirstName', team_field], inplace=True)
    names.to_csv(f'../data/lookup/names_{dataSource.value}.csv', index=False)

def exportLookupHelpers(frames : Dict[DataSource, pd.DataFrame]) -> None:
    for dataSource in DataSource:
        if not os.path.exists(f'../data/lookup/names_{dataSource.value}.csv'):
            exportToMakeLookup(frames[dataSource.value], 'Name', 'Team', dataSource)

# ===========
# Finalize loaded data
# ===========
def finalizeCombinedSet(frames : Dict[DataSource, pd.DataFrame]) -> pd.DataFrame:
    # Split dfs, and convert dates
    dfs, ref = frames['dfs'], frames['ref']
    dfs['Date'] = pd.to_datetime(dfs['Date'])
    ref['Date'] = pd.to_datetime(ref['Date'])

    # Get lookup names onto dfs
    lookup = pd.read_csv(f'../data/lookup/name_lookup.csv')
    dfs = dfs.merge(lookup, left_on = ['Team','Name'], right_on = ['Team_dfs','Name_dfs'], how = 'outer', indicator = True)
    dfs[['Name','Team']] = dfs[['Player_ref','Team_ref']]
    # Every dfs player has a ref match
        #print(dfs['_merge'].value_counts())
        #print(dfs.head())

    # Merge ref and dfs, return inner join results
    big = ref.merge(dfs, left_on = ['Team', 'Name', 'Date'], right_on = ['Team_ref', 'Player_ref', 'Date'], how = 'outer', indicator = '_merge2')
    # 9,000 of 50,000 ref data observations don't have match in dfs
        #print(big['_merge2'].value_counts())
        #print(ref.shape)
    # 20,000 of 50,000 dfs data observations don't have match in ref
    # Most are injuries / scratches

    both = big[big['_merge2'] == 'both'].copy()
    return both

def main():
    year_start = 2022
    year_end = 2023
    target_years = set(range(year_start, year_end+1))

    # Extract and save yearly data if it doesn't exist
    frames = loadFrames(target_years)
    frames['dfs'], frames['ref'] = standardizeNames(frames['dfs'], frames['ref'])

    # Export lookup table helpers if they don't exist
    exportLookupHelpers(frames)

    bigFrame = finalizeCombinedSet(frames)
    print(bigFrame.head())
    print(bigFrame.shape)

    
if __name__ == '__main__':
    path = pathlib.Path(__file__).parent.resolve()
    os.chdir(path)
    main()