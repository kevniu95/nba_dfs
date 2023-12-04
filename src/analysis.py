import os
import pathlib
from .dataExtract import *
import pulp
from .lp import *


def initFrame(bothOnly : bool = True) -> pd.DataFrame:
    year_start = 2022
    year_end = 2023
    target_years = set(range(year_start, year_end+1))

    # Extract and save yearly data if it doesn't exist
    frames = loadFrames(target_years)
    frames['dfs'], frames['ref'] = standardizeNames(frames['dfs'], frames['ref'])
    return finalizeCombinedSet(frames, bothOnly)

def split_dates(df : pd.DataFrame) -> Dict[str, pd.DataFrame]:
    date_dict = {}
    for date in df['Date'].unique():
        date_dict[date] = df[df['Date'] == date].copy()
    return date_dict

# def processFrame(df : pd.DataFrame):
#     prob = pulp.LpProblem("MaximalTeam", pulp.LpMaximize)
#     player_names = df['Name'].unique()
#     player_vars = pulp.LpVariable.dicts("Players", player_names, cat='Binary')
#     prob += pulp.lpSum([player_vars[player] * row['Proj_dfds'] for player, row in df.iterrows()]), "TotalProjectedPoints"
#     prob += pulp.lpSum([player_vars[player] * row['Salary_dfs'] for player, row in df.iterrows()]) <= 50000, "TotalSalary"
#     positions = ['PG', 'SG', 'SF', 'PF', 'C', 'G', 'F', 'UTIL']
#     for pos in positions:
#         prob += pulp.lpSum([player_vars[player] for player, row in players_df.iterrows() if pos in row['Position']]) >= 1, f"Position_{pos}"

#     # Solve the problem
#     prob.solve()

#     # Check if a valid solution was found
#     if prob.status == 1:
#         selected_players = [player for player in player_vars if player_vars[player].varValue > 0]
#     else:
#         selected_players = "No valid team could be formed within the budget constraints."

#     selected_players





def main():
    bigFrame = initFrame(False)
    print(bigFrame.shape)
    dfs_sub = bigFrame[bigFrame['_merge'].isin(['dfs_only','both'])].copy()
    print(bigFrame['_merge'].value_counts())

    date_dict = split_dates(dfs_sub)
    ctr = 0
    for date, frame in date_dict.items():
        ctr += 1
        print(date)
        print(frame)
        print(frame.columns)
        print(doLp(frame, 'Position_dfs').sort_values('selectedPosition').head(8)[['selectedPosition','Name','Team','Opp_ref','Proj_dfs','Salary_dfs','Value_dfs','Total Points_ref']])
        if ctr > 0: 
            break
        



    # [print(k, v.shape) for k, v in date_dict.items()]
    # print(date_dict[pd.to_datetime('2021-10-20')].sort_values('Proj_dfs',ascending = False))
    # date_dict[pd.to_datetime('2021-10-20')].sort_values('Proj_dfs',ascending = False).to_csv('../data/2021-10-20.csv')
    
    
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