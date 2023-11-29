from enum import Enum
from typing import List, Any
from util.logger_config import setup_logger
import pandas as pd

logger = setup_logger(__name__)

class DfsSite(Enum):
    FANDUEL = "fanduel"
    DRAFTKINGS = "draftkings"
    # None means process both sites

class DataSource(Enum):
    DFS = "dfs"
    REF = "ref" 

POINTS_CONVERTER = {
    DfsSite.DRAFTKINGS :  {'PTS': 1, '3P': 0.5, 'TRB': 1.25, 'AST': 1.5, 'STL': 2, 'BLK': 2, 'TOV': -0.5, 'Double-Double': 1.5, 'Triple-Double': 3},
    DfsSite.FANDUEL :  {}
}

DFS_TEAM_LIST = ['ATL', 'BKN','BOS', 'CHA','CHI','CLE','DAL','DEN','DET','GSW','HOU','IND',
                 'LAC','LAL','MEM','MIA','MIL','MIN','NO','NY','OKC','ORL','PHI','PHO',
                 'POR','SA','SAC','TOR','UTA','WAS']

REF_TEAM_LIST = ['ATL','BRK','BOS','CHO','CHI','CLE','DAL','DEN','DET','GSW','HOU','IND',
                 'LAC','LAL','MEM','MIA','MIL','MIN','NOP','NYK','OKC','ORL','PHI','PHO',
                 'POR','SAS','SAC','TOR','UTA','WAS']

def getTeamList(dataSource : DataSource) -> List[str]:
    if dataSource == DataSource.DFS:
        return DFS_TEAM_LIST
    elif dataSource == DataSource.REF:
        return REF_TEAM_LIST
    else:
        logger.error(f"Invalid data source {dataSource}")
        raise ValueError(f"Invalid data source {dataSource}")

def convertTeamList(column : pd.Series, convertFrom : DataSource) -> Any:
    if convertFrom == DataSource.DFS.value:
        return column.replace(DFS_TEAM_LIST, REF_TEAM_LIST)
    elif convertFrom == DataSource.REF.value:
        return column.replace(REF_TEAM_LIST, DFS_TEAM_LIST)
    else:
        logger.error(f"Invalid data source {convertFrom}")
        raise ValueError(f"Invalid data source {convertFrom}")