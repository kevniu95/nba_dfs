from datetime import datetime, timedelta
import pandas as pd 
from bs4 import BeautifulSoup
import requests
from enum import Enum


datesDict = {
    2022 : 'October 19, 2021 – April 10, 2022',
    2023 : 'October 18, 2022 – April 9, 2023',
    2024 : 'October 24, 2023 – April 14, 2024',
}

class Site(Enum):
    FANDUEL = "fanduel"
    DRAFTKINGS = "draftkings"


def getRefLink(date : str) -> str:
    date = datetime.strptime(date, '%B %d, %Y')
    return f'https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={date.month}&day={date.day}&year={date.year}'

def getRefData(date : str) -> pd.DataFrame:
    refLink = getRefLink(date)
    req = requests.get(refLink)
    soup = BeautifulSoup(req.text, 'html.parser')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))[0]
    return df

def getDailyFantasyLink(date : str, site : Site) -> str:
    date = datetime.strptime(date, '%B %d, %Y')
    return f'https://www.dailyfantasyfuel.com/nba/projections/{site.value}/{date.year}-{date.month}-{date.day}'


def mergeDfsSources(df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
    # not implemented
    return pd.DataFrame()
    
def getDailyFantasyDataByDate(date : str, site : Site = Site.DRAFTKINGS) -> pd.DataFrame:
    link = getDailyFantasyLink(date, site)
    df = getDailyFantasyData(link, site)

    if not site:
        link1 = getDailyFantasyLink(date, Site.FANDUEL)
        df1 = getDailyFantasyData(link1, Site.FANDUEL)
        return mergeDfsSources(df, df1)
    return df

def getDailyFantasyData(dailyFantasyLink : str, site : Site) -> pd.DataFrame:
    req = requests.get(dailyFantasyLink)
    soup = BeautifulSoup(req.text, 'html.parser')    
    tbody = soup.find('tbody')
    rows = tbody.find_all('tr')

    table = []
    for row in rows:
        cols = row.find_all('td')
        cols = [col.text.strip() for col in cols]
        table.append(cols)
    df = pd.DataFrame(table)
    df.drop(columns = [0, 4, 5], inplace = True, axis = 1)
    df.columns = ['Position', 'Name', 'Salary', 'Team', 'Opponent', 'DvP', 'Proj', 'Value', 'L5Min', 'L5Avg', 'L5Max', 'OU', 'TmPts']
    return df
    
def processYear(year : int):
    yr = datesDict[year]
    start_date = datetime.strptime(yr.split(' – ')[0], '%B %d, %Y')
    end_date = datetime.strptime(yr.split(' – ')[1], '%B %d, %Y')
    for i in range((end_date - start_date).days + 1)[:1]:
        date = start_date + timedelta(days=i)
        date_string = date.strftime('%B %d, %Y')
        ref_df = getRefData(date_string)
        dfs_df = getDailyFantasyDataByDate(date_string)
        print(ref_df.head())
        print(dfs_df.head())


if __name__ == '__main__':
    processYear(2022)

# https://www.basketball-reference.com/friv/dailyleaders.fcgi?month=10&day=19&year=2021

# https://www.dailyfantasyfuel.com/nba/projections/draftkings/2021-10-19

