import pulp
import pandas as pd
import random
import itertools
from typing import Dict

POSITIONS = ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL"]

def getPlayerPositionVars(df : pd.DataFrame) -> Dict[str, pulp.LpVariable]:
    player_position_vars = {}
    for player, row in df.iterrows():
        eligible_positions = row['Position'] + ['UTIL']  # Adding 'UTIL' to eligible positions
        if 'PG' in row['Position'] or 'SG' in row['Position']:
            eligible_positions.append('G')  # Adding 'G' for guards
        if 'SF' in row['Position'] or 'PF' in row['Position']:
            eligible_positions.append('F')  # Adding 'F' for forwards
        
        df.at[player, 'Position'] = eligible_positions  # Updating the dataframe
        
        row['Position'] = eligible_positions  # Updating the dataframe
        for pos in set(eligible_positions):  # Using set to avoid duplicates
            var_name = f"{row['Name']}_{pos}"
            player_position_vars[var_name] = pulp.LpVariable(var_name, cat='Binary')
    return player_position_vars

def doLp(df : pd.DataFrame, positionVar : str):
    # Updating the model to handle multiple eligible positions for each player
    prob = pulp.LpProblem("AdvancedFantasyBasketballTeam", pulp.LpMaximize)
    
    # Decision variables: For each player and each position they can fulfill
    df['Position'] = df[positionVar].str.split('/')
    player_position_vars = getPlayerPositionVars(df)
    print(player_position_vars)
    
    # Objective Function: Maximize the sum of projected points
    a = [player_position_vars[f"{row['Name']}_{pos}"] * row['Proj_dfs'] 
                        for player, row in df.iterrows() 
                        for pos in row['Position']]
    prob += pulp.lpSum(a), "TotalProjectedPoints"

    # Constraint: Total salary must be less than or equal to $50,000
    b = []
    for k, v in player_position_vars.items():
        b.append(df.loc[df['Name'] == k.split('_')[0], 'Salary_dfs'].values[0] * v)
    prob += pulp.lpSum(b) <= 50, "TotalSalary"

    # Constraints for positions (including generic positions)
    player_names = df['Name'].unique()
    for pos in POSITIONS:  # Includes both specific and generic positions
        e = [player_position_vars[f"{player}_{pos}"] for player in player_names if f"{player}_{pos}" in player_position_vars]
        prob += pulp.lpSum(e) == 1, f"ExactlyOne_{pos}"

    # Constraint: Each player can be selected for at most one position
    for player in player_names:
        prob += pulp.lpSum([player_position_vars[var] for var in player_position_vars if var.startswith(f"{player}_")]) <= 1, f"OnePosPerPlayer_{player}"

    # Solve the problem
    prob.solve()
    print(prob.status)
    lineup = [i for i in prob.variables() if i .varValue > 0]

    for var in lineup:
        name, position = " ".join(var.name.split('_')[:-1]), var.name.split('_')[-1]
        df.loc[df['Name'] == name, 'selectedPosition'] = position
    return df
