import os
import pathlib
from .dataExtract import *


def initFrame(bothOnly : bool = True) -> pd.DataFrame:
    year_start = 2022
    year_end = 2023
    target_years = set(range(year_start, year_end+1))

    # Extract and save yearly data if it doesn't exist
    frames = loadFrames(target_years)
    frames['dfs'], frames['ref'] = standardizeNames(frames['dfs'], frames['ref'])
    return finalizeCombinedSet(frames, bothOnly)

def main():
    bothFrame = initFrame()
    print(bothFrame.columns)

    bigFrame = initFrame(False)
    print(bigFrame.shape)
    print(bigFrame.head())

    a = bigFrame.merge(bothFrame[['Name','Team','Date']], how = 'left', on = ['Name','Team','Date'], indicator = '_merge2')
    print(a['_merge'].value_counts())
    print(bothFrame.shape)
    sub = a[a['_merge2'] == 'both'].copy()
    dups = sub.duplicated(['Name','Team','Date'], keep = False)
    print(sub[dups].sort_values(['Name','Team','Date']))
    
    
    # b = bigFrame.drop_duplicates(['Name','Team','Date'])
    # print(bigFrame[bigFrame['_merge'] == 'both'].shape)
    # print(b.shape)
    
    


    # Assumptions made
    # 1. ok that dfs-no-ref guys are excluded
    #   # Explanation: Can easily identify injuries on day of game
    # 2. ok that ref-no-dfs guys are excluded
        # Explanation if dfs hasn't identified, them, they're not an option in the first place

# Do regression of proj on points

    # Do regression of proj + all others on points

    # Come up with way to select best possible team

    

if __name__ == '__main__':
    path = pathlib.Path(__file__).parent.resolve()
    os.chdir(path)
    
    main()