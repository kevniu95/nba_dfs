import os
import time
import pathlib
from datetime import datetime, timedelta
import requests
import pickle
from abc import ABC, abstractmethod
import pandas as pd 
from bs4 import BeautifulSoup
from util.constants import *
from util.logger_config import setup_logger

logger = setup_logger(__name__)
MODBY = 19

DATES_DICT = {
    2022 : 'October 19, 2021 – April 10, 2022',
    2023 : 'October 18, 2022 – April 9, 2023'
    # 2024 : 'October 24, 2023 – April 14, 2024',
}

def prepData(getData_func):
    def wrapper(self, *args, **kwargs):
        result = getData_func(self, *args, **kwargs)
        if len(result) == 0 or result is None:
            logger.warning(f"Empty dataframe returned for {self.__class__.__name__} on {args[0]}")
            return result
        return self._prepData(result)
    return wrapper

class DataGenerator(ABC):
    def __init__(self, url_pattern : str):
        self.url_pattern = url_pattern

    @abstractmethod
    def getLink(self, date : str) -> str:
        pass

    def _prepData(self, df : pd.DataFrame):
        return df

    @prepData
    def getData(self, date : str) -> pd.DataFrame:
        link = self.getLink(date)
        req = requests.get(link)
        soup = BeautifulSoup(req.text, 'html.parser')
        if len(soup.find_all('table')) == 0:
            logger.warning(f"Empty dataframe returned for {self.__class__.__name__} on {date}")
            return pd.DataFrame([])
        else:
            table = soup.find_all('table')[0]
            return pd.read_html(str(table))[0]
    
class RefDataGenerator(DataGenerator):
    def __init__(self, dfsSite : DfsSite):
        super().__init__('https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}')
        self.dfsSite = dfsSite
        
    def getLink(self, date : str) -> str:
        date = datetime.strptime(date, '%B %d, %Y')
        return self.url_pattern.format(date.month, date.day, date.year)
        
    def _prepData(self, df : pd.DataFrame) -> pd.DataFrame:
        df = df[df['Rk'] != 'Rk'].copy()
        pts_converter = POINTS_CONVERTER[self.dfsSite]
        df[['PTS', '3P', 'TRB', 'AST', 'STL', 'BLK', 'TOV']] = df[['PTS', '3P', 'TRB', 'AST', 'STL', 'BLK', 'TOV']].apply(pd.to_numeric)
        df['Count'] = (df[['PTS', 'TRB', 'AST', 'STL', 'BLK']] >= 10).sum(axis=1)
        df['Double-Double'] = df['Count'] >= 2
        df['Triple-Double'] = df['Count'] >= 3
        
        df_copy = df.copy()
        for column, rule in pts_converter.items():
            df_copy[column] = df_copy[column] * rule
        
        df['Total Points'] = df_copy[['PTS', '3P', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'Double-Double', 'Triple-Double']].sum(axis = 1)
        return df

class DailyFantasyDataGenerator(DataGenerator):
    def __init__(self, dfsSite : DfsSite):
        super().__init__('https://www.dailyfantasyfuel.com/nba/projections/{}/{}-{}-{}')
        self.dfsSite = dfsSite # if None, get both sites

    def getLink(self, date : str) -> str:
        date = datetime.strptime(date, '%B %d, %Y')
        return self.url_pattern.format(self.dfsSite.value, date.year, date.month, date.day)
    
    @prepData
    def getData(self, date : str) -> pd.DataFrame:
        getBoth : bool = False
        if not self.dfsSite:
            getBoth = True
            self.dfsSite = DfsSite.DRAFTKINGS
        link = self.getLink(date)
        df = self._getDailyFantasyData(link)

        if getBoth:
            self.dfsSite = DfsSite.FANDUEL
            link1 = self.getLink(date)
            df1 = self._getDailyFantasyData(link1, DfsSite.FANDUEL)
            return self._mergeDfsSources(df, df1)
        return df
    
    def _getDailyFantasyData(self, link : str) -> pd.DataFrame:
        req = requests.get(link)
        try:
            soup = BeautifulSoup(req.text, 'html.parser')    
            tbody = soup.find('tbody')
            rows = tbody.find_all('tr')
        except AttributeError as e:
            logger.warning(f"Failed to parse HTML: {e}")
            return pd.DataFrame()

        if len(rows) == 0:
            logger.warning(f"Empty dataframe returned for {self.__class__.__name__} on {link}")
            return pd.DataFrame()

        # Continue processing rows...
        table = []
        for row in rows:
            cols = row.find_all('td')
            cols = [col.text.strip() for col in cols]
            table.append(cols)
        df = pd.DataFrame(table)
        df.drop(columns = [0, 4, 5], inplace = True, axis = 1)
        df.columns = ['Position', 'Name', 'Salary', 'Team', 'Opponent', 'DvP', 'Proj', 'Value', 'L5Min', 'L5Avg', 'L5Max', 'OU', 'TmPts']
        df['Salary'] = df['Salary'].str.replace('$', '').str.replace('k', '').astype(float)
        return df
    

    def _mergeDfsSources(self, df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
        logger.error("Merge of DFS sources is not implemented yet")
        raise NotImplementedError("Merge of DFS sources is not implemented yet")
        
def initYearDict(year : int, saveTo : str = None) -> dict:
    if not saveTo:
        saveTo = f'../data/import/import_{year}.pkl'
        print(saveTo)
        print(os.path.exists(saveTo))
    if os.path.exists(saveTo):
        with open(saveTo, 'rb') as f:
            return pickle.load(f)
    else:
        return {}
    
def processYear(year : int, dfsSite : DfsSite = DfsSite.DRAFTKINGS):
    saveTo = f'../data/import/import_{year}.pkl'
    yearDict = initYearDict(year, saveTo)

    print(yearDict)
    yr = DATES_DICT[year]
    start_date = datetime.strptime(yr.split(' – ')[0], '%B %d, %Y')
    end_date = datetime.strptime(yr.split(' – ')[1], '%B %d, %Y')
    
    ctr = 0
    for i in range((end_date - start_date).days + 1):
        print(f'{i}: {start_date + timedelta(days=i)}')
        date = start_date + timedelta(days=i)
        date_string = date.strftime('%B %d, %Y')
        if date_string in yearDict:
            logger.info(f"Skipping {date_string} because it already exists in {saveTo}...")
            continue

        rdg = RefDataGenerator(dfsSite)
        dfdg = DailyFantasyDataGenerator(dfsSite)
        yearDict[date_string] = {'ref' : {dfsSite.value : rdg.getData(date_string)}, 
                                 'dfs' : {dfsSite.value : dfdg.getData(date_string)}}
        
        ctr += 1
        if ctr % MODBY == 0:
            with open(saveTo, 'wb') as f:
                logger.info(f"Saving data to {saveTo}...")
                pickle.dump(yearDict, f)
            logger.info(f"Sleeping for 60 seconds then scraping gamelog data for new set of {MODBY} teams...")
            time.sleep(61)    

if __name__ == '__main__':
    path = pathlib.Path(__file__).parent.resolve()
    os.chdir(path)
    processYear(2022)
    processYear(2023)