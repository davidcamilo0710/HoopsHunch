from IPython.display import HTML, display
import requests
import time
from datetime import datetime
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from nba_api.stats.endpoints.leaguedashteamstats import LeagueDashTeamStats
from nba_api.stats.endpoints.leaguedashplayerstats import LeagueDashPlayerStats
from datetime import timedelta
import hoopshunch.templates as templates


def trade_deadline(season: int) -> datetime:
    if season < 2012 or season > 2022:
        raise ValueError(
            "Season must be between 2012 and 2022 inclusive. If it is a later season, please update the function in util.py."
        )
    if season == 2012:
        return datetime(season + 1, 2, 21)
    elif season == 2013:
        return datetime(season + 1, 2, 20)
    elif season == 2014:
        return datetime(season + 1, 2, 19)
    elif season == 2015:
        return datetime(season + 1, 2, 18)
    elif season == 2016:
        return datetime(season + 1, 2, 23)
    elif season == 2017:
        return datetime(season + 1, 2, 8)
    elif season == 2018:
        return datetime(season + 1, 2, 8)
    elif season == 2019:
        return datetime(season + 1, 2, 6)
    elif season == 2020:
        return datetime(season + 1, 3, 25)
    elif season == 2021:
        return datetime(season + 1, 2, 10)
    else:
        return datetime(season + 1, 2, 9)  # season 2022-23


def print_df(df):
    display(HTML(df.to_html(index=False)))


def season_str(season):
    return str(season) + "-" + str(season + 1)[-2:]


def labels_to_drop(column_names, list_of_strings):
    return [
        col for col in column_names if any([x for x in list_of_strings if x in col])
    ]


def select_columns(data, attributes, columns):
    return data[
        :,
        [
            index
            for index, col in enumerate(columns)
            if any(name in col for name in attributes)
        ],
    ]


def WinningV1(df, count, window):
    try:
        win = df["HOME_WL"].value_counts()[count]
        win = win / window
    except:
        win = 0
    return win

# this function has the advanced stats unable, beacause the api is very slow
def getLast5Stats(season, date_from, date_to, team_id, sleep, real_date, real_game_id, conn, temp=False):
    first_sleep = sleep
    # second_sleep = sleep
    table = pd.DataFrame()
    # table2 = pd.DataFrame()
    for attempt in range(7):
        try:
            if temp == False:
                table = LeagueDashTeamStats(
                    season=season_str(season),
                    team_id_nullable=team_id,
                    date_from_nullable=date_from.strftime("%m/%d/%Y"),
                    date_to_nullable=date_to.strftime("%m/%d/%Y"),
                    per_mode_detailed="PerGame",
                    pace_adjust="N",
                ).get_data_frames()[0]
            else:
                table = LeagueDashTeamStats(
                    season=season_str(season),
                    team_id_nullable=team_id,
                    date_to_nullable=date_to.strftime("%m/%d/%Y"),
                    per_mode_detailed="PerGame",
                    pace_adjust="N",
                ).get_data_frames()[0]
            time.sleep(first_sleep)
            if temp == False:
                labels = ["CF", "NAME", "RANK"]
            else:
                labels = ["CF", "NAME"]
            table.drop(labels_to_drop(table.columns, labels), axis=1, inplace=True)
            table = table.drop(["TEAM_ID"], axis=1)
        except:
            first_sleep += 8
            continue
        else:
            break
    else:
        print(
            "Issue! imposible to fix in date "
            + str(real_date.strftime("%Y-%m-%d"))
            + ", for team "
            + str(team_id)
            + ", game id "
            + str(real_game_id)
        )
        data = [(season, season_str(season), real_game_id, team_id, str(date_from.strftime("%m/%d/%Y")), str(date_to.strftime("%m/%d/%Y")))]
        issues = pd.DataFrame(data, columns=["SEASON", "SEASON_STR", "GAME_ID", "TEAM_ID", "DATE_FROM", "DATE_TO"])
        if temp == False:
            issues.to_sql("issues", conn, if_exists="append", index=False)
        else:
            issues.to_sql("issues_season_record", conn, if_exists="append", index=False)
        
    return table


# this function is used to get the last 5 games stats of a team, and make 8 requests to the NBA API
def GetLast5Games(games, date, homeID, awayID, sleep, gamesH2H, game_id, conn, window=5):
    games = games.sort_values(by="GAME_DATE", ascending=False)
    gamesH2H = gamesH2H.sort_values(by="GAME_DATE", ascending=False)
    lastGamesHome = games[
        (games["GAME_DATE"] < date)
        & ((games["HOME_TEAM_ID"] == homeID) | (games["AWAY_TEAM_ID"] == homeID))
    ]
    lastGamesHome = lastGamesHome.iloc[:window]
    lastGamesHH = lastGamesHome[lastGamesHome["HOME_TEAM_ID"] == homeID]
    lastGamesHA = lastGamesHome[lastGamesHome["AWAY_TEAM_ID"] == homeID]
    winLastGamesHome = WinningV1(lastGamesHH, "W", len(lastGamesHome)) + WinningV1(
        lastGamesHA, "L", len(lastGamesHome)
    )

    lastGamesAway = games[
        (games["GAME_DATE"] < date)
        & ((games["HOME_TEAM_ID"] == awayID) | (games["AWAY_TEAM_ID"] == awayID))
    ]
    lastGamesAway = lastGamesAway.iloc[:window]
    lastGamesAH = lastGamesAway[lastGamesAway["HOME_TEAM_ID"] == awayID]
    lastGamesAA = lastGamesAway[lastGamesAway["AWAY_TEAM_ID"] == awayID]
    winLastGamesAway = WinningV1(lastGamesAH, "W", len(lastGamesAway)) + WinningV1(
        lastGamesAA, "L", len(lastGamesAway)
    )

    lastHomeHome = games[
        (games["GAME_DATE"] < date) & (games["HOME_TEAM_ID"] == homeID)
    ]
    lastHomeHome = lastHomeHome.iloc[:window]
    winLastHomeHome = WinningV1(lastHomeHome, "W", len(lastHomeHome))

    lastAwayAway = games[
        (games["GAME_DATE"] < date) & (games["AWAY_TEAM_ID"] == awayID)
    ]
    lastAwayAway = lastAwayAway.iloc[:window]
    winLastAwayAway = WinningV1(lastAwayAway, "L", len(lastAwayAway))

    lastH2H = gamesH2H[
        (gamesH2H["GAME_DATE"] < date)
        & (
            ((gamesH2H["HOME_TEAM_ID"] == homeID) & (gamesH2H["AWAY_TEAM_ID"] == awayID))
            | ((gamesH2H["HOME_TEAM_ID"] == awayID) & (gamesH2H["AWAY_TEAM_ID"] == homeID))
        )
    ]
    lastH2H = lastH2H.iloc[:window]
    lastH2H_HH = lastH2H[lastH2H["HOME_TEAM_ID"] == homeID]
    lastH2H_HA = lastH2H[lastH2H["AWAY_TEAM_ID"] == homeID]
    winHomelastH2H = WinningV1(lastH2H_HH, "W", len(lastH2H)) + WinningV1(
        lastH2H_HA, "L", len(lastH2H)
    )
    template = templates.create_stats_table()

    DATESlastGamesHome = lastGamesHome[["SEASON", "GAME_ID", "GAME_DATE"]]
    DATESlastGamesHome = DATESlastGamesHome.reset_index(drop=True)

    DATESlastGamesAway = lastGamesAway[["SEASON", "GAME_ID", "GAME_DATE"]]
    DATESlastGamesAway = DATESlastGamesAway.reset_index(drop=True)
    try:
        date_to = DATESlastGamesHome.at[0, "GAME_DATE"]
        date_from = DATESlastGamesHome.at[DATESlastGamesHome.index[-1], "GAME_DATE"]
        season = DATESlastGamesHome.at[0, "SEASON"]
        new_columns_home = getLast5Stats(
            season, date_from, date_to, homeID, sleep, date, game_id, conn
        )
        cols = new_columns_home.columns
        new_cols = ["LAST5_HOME_" + col for col in cols]
        new_columns_home.columns = new_cols
    except:
        new_columns_home = pd.DataFrame()
    try:
        date_to = DATESlastGamesAway.at[0, "GAME_DATE"]
        date_from = DATESlastGamesAway.at[DATESlastGamesAway.index[-1], "GAME_DATE"]
        season = DATESlastGamesAway.at[0, "SEASON"]
        new_columns_away = getLast5Stats(
            season, date_from, date_to, awayID, sleep, date, game_id, conn
        )
        cols = new_columns_away.columns
        new_cols = ["LAST5_AWAY_" + col for col in cols]
        new_columns_away.columns = new_cols
    except:
        new_columns_away = pd.DataFrame()
    result = pd.concat(
        [
            new_columns_home,
            new_columns_away,
        ],
        axis=1,
    )
    
    result = result.reindex(columns=template.columns, fill_value=np.nan)
    result["LAST5_HOME_GAMES"] = winLastGamesHome
    result["LAST5_AWAY_GAMES"] = winLastGamesAway
    result["LAST5_HOME_AS_HOME"] = winLastHomeHome
    result["LAST5_AWAY_AS_AWAY"] = winLastAwayAway
    result["LAST5_HOME_H2H"] = winHomelastH2H
    if result.empty:
        result = pd.DataFrame({'LAST5_HOME_GAMES': [winLastGamesHome], 'LAST5_AWAY_GAMES': [winLastGamesAway], 'LAST5_HOME_AS_HOME': [winLastHomeHome], 'LAST5_AWAY_AS_AWAY': [winLastAwayAway], 'LAST5_HOME_H2H': [winHomelastH2H]})
        result = result.reindex(columns=template.columns, fill_value=np.nan)
    return result

def GetLast5GamesV2(games, date, homeID, awayID, sleep, game_id, conn, window=5):
    games = games.sort_values(by="GAME_DATE", ascending=False)
    lastGamesHome = games[
        (games["GAME_DATE"] < date)
        & ((games["HOME_TEAM_ID"] == homeID) | (games["AWAY_TEAM_ID"] == homeID))
    ]
    lastGamesHome = lastGamesHome.iloc[:window]
    lastGamesAway = games[
        (games["GAME_DATE"] < date)
        & ((games["HOME_TEAM_ID"] == awayID) | (games["AWAY_TEAM_ID"] == awayID))
    ]
    lastGamesAway = lastGamesAway.iloc[:window]
    template = templates.create_season_record_table()

    DATESlastGamesHome = lastGamesHome[["SEASON", "GAME_ID", "GAME_DATE"]]
    DATESlastGamesHome = DATESlastGamesHome.reset_index(drop=True)

    DATESlastGamesAway = lastGamesAway[["SEASON", "GAME_ID", "GAME_DATE"]]
    DATESlastGamesAway = DATESlastGamesAway.reset_index(drop=True)
    try:
        date_to = DATESlastGamesHome.at[0, "GAME_DATE"]
        date_from = DATESlastGamesHome.at[DATESlastGamesHome.index[-1], "GAME_DATE"]
        season = DATESlastGamesHome.at[0, "SEASON"]
        new_columns_home = getLast5Stats(
            season, date_from, date_to, homeID, sleep, date, game_id, conn, temp=True
        )
        cols = new_columns_home.columns
        new_cols = ["SEASON_HOME_" + col for col in cols]
        new_columns_home.columns = new_cols
    except:
        new_columns_home = pd.DataFrame()
    try:
        date_to = DATESlastGamesAway.at[0, "GAME_DATE"]
        date_from = DATESlastGamesAway.at[DATESlastGamesAway.index[-1], "GAME_DATE"]
        season = DATESlastGamesAway.at[0, "SEASON"]
        new_columns_away = getLast5Stats(
            season, date_from, date_to, awayID, sleep, date, game_id, conn, temp=True
        )
        cols = new_columns_away.columns
        new_cols = ["SEASON_AWAY_" + col for col in cols]
        new_columns_away.columns = new_cols
    except:
        new_columns_away = pd.DataFrame()
    result = pd.concat(
        [
            new_columns_home,
            new_columns_away,
        ],
        axis=1,
    )
    
    result = result.reindex(columns=template.columns, fill_value=np.nan)
    return result


def get_restdays(row, home):
    today_game = row["GAME_DATE"]
    last_game = (
        row["HOME_LAST_GAME_DATE"] if home == "1" else row["AWAY_LAST_GAME_DATE"]
    )
    last2_game = (
        row["HOME_2_LAST_GAME_DATE"] if home == "1" else row["AWAY_2_LAST_GAME_DATE"]
    )
    last3_game = (
        row["HOME_3_LAST_GAME_DATE"] if home == "1" else row["AWAY_3_LAST_GAME_DATE"]
    )
    two_days = timedelta(days=2)
    three_days = timedelta(days=3)
    four_days = timedelta(days=4)
    five_days = timedelta(days=5)
    if (today_game - five_days) < last3_game:
        return "4IN5-B2B"
    elif (today_game - two_days) < last_game:
        if (today_game - four_days) < last2_game:
            return "3IN4-B2B"
        else:
            return "B2B"

    elif (today_game - three_days) < last_game:
        if (today_game - four_days) < last2_game:
            return "3IN4"
        else:
            return "1"
    elif (today_game - four_days) < last_game:
        return "2"
    else:
        return "3+"


def addRestDays(games):
    games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
    games = games.sort_values(by="GAME_DATE")
    home_games = games[["HOME_TEAM_ID", "GAME_DATE"]].rename(
        columns={"HOME_TEAM_ID": "TEAM"}
    )
    away_games = games[["AWAY_TEAM_ID", "GAME_DATE"]].rename(
        columns={"AWAY_TEAM_ID": "TEAM"}
    )
    all_games = pd.concat([home_games, away_games])
    grouped = all_games.groupby(["TEAM", "GAME_DATE"]).first().reset_index()
    grouped["LAST_GAME_DATE"] = grouped.groupby("TEAM")["GAME_DATE"].shift(1)
    grouped["2_LAST_GAME_DATE"] = grouped.groupby("TEAM")["GAME_DATE"].shift(2)
    grouped["3_LAST_GAME_DATE"] = grouped.groupby("TEAM")["GAME_DATE"].shift(3)
    games = games.merge(
        grouped,
        left_on=["HOME_TEAM_ID", "GAME_DATE"],
        right_on=["TEAM", "GAME_DATE"],
        suffixes=("_HOME", "_AWAY"),
    )
    games = games.merge(
        grouped,
        left_on=["AWAY_TEAM_ID", "GAME_DATE"],
        right_on=["TEAM", "GAME_DATE"],
        suffixes=("_HOME", "_AWAY"),
    )
    col_rename = {
        "LAST_GAME_DATE_AWAY": "AWAY_LAST_GAME_DATE",
        "LAST_GAME_DATE_HOME": "HOME_LAST_GAME_DATE",
        "2_LAST_GAME_DATE_AWAY": "AWAY_2_LAST_GAME_DATE",
        "2_LAST_GAME_DATE_HOME": "HOME_2_LAST_GAME_DATE",
        "3_LAST_GAME_DATE_AWAY": "AWAY_3_LAST_GAME_DATE",
        "3_LAST_GAME_DATE_HOME": "HOME_3_LAST_GAME_DATE",
    }
    games = games.rename(columns=col_rename)
    games = games.drop(["TEAM_HOME", "TEAM_AWAY"], axis=1)
    five_days = timedelta(days=5)
    games["AWAY_LAST_GAME_DATE"].fillna(games["GAME_DATE"] - five_days, inplace=True)
    games["HOME_LAST_GAME_DATE"].fillna(games["GAME_DATE"] - five_days, inplace=True)

    games["AWAY_2_LAST_GAME_DATE"].fillna(
        games["AWAY_LAST_GAME_DATE"] - five_days, inplace=True
    )
    games["HOME_2_LAST_GAME_DATE"].fillna(
        games["HOME_LAST_GAME_DATE"] - five_days, inplace=True
    )

    games["AWAY_3_LAST_GAME_DATE"].fillna(
        games["AWAY_2_LAST_GAME_DATE"] - five_days, inplace=True
    )
    games["HOME_3_LAST_GAME_DATE"].fillna(
        games["HOME_2_LAST_GAME_DATE"] - five_days, inplace=True
    )

    games["HOME_REST_DAYS"] = games.apply(get_restdays, axis=1, args=("1"))
    games["AWAY_REST_DAYS"] = games.apply(get_restdays, axis=1, args=("0"))
    games = games.drop(
        [
            "HOME_LAST_GAME_DATE",
            "AWAY_LAST_GAME_DATE",
            "HOME_2_LAST_GAME_DATE",
            "AWAY_2_LAST_GAME_DATE",
            "HOME_3_LAST_GAME_DATE",
            "AWAY_3_LAST_GAME_DATE",
        ],
        axis=1,
    )
    return games


def injuries():
    list_of_subdomains = [
        "nba-health-and-safety-protocols",
        "illness",
        "sprained-left-ankle",
        "sprained-right-ankle",
        "sore-left-knee",
        "sore-right-knee",
        "concussion",
        "sore-left-ankle",
        "left-knee-injury",
        "sore-lower-back",
    ]
    all_data = []

    for subdomain in list_of_subdomains:
        url = f"https://hashtagbasketball.com/injury/{subdomain}"
        r = requests.get(url)
        soup = BeautifulSoup(r.content, "html.parser")

        table = soup.find("table")
        table_rows = table.find_all("tr")

        for tr in table_rows:
            td = tr.find_all("td")
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                all_data.append(row)

    df = pd.DataFrame(
        all_data, columns=["PLAYER", "TEAM", "INJURED_ON", "RETURNED", "DAYS_MISSED"]
    )
    return df


def getBest3(df, df2):
    df["SEASON"] = df["SEASON"].astype(int)
    df["PTS"] = df["PTS"].astype(float)
    df = df.sort_values(by=["SEASON", "TEAM_ID", "PTS"], ascending=[True, True, False])
    grouped = df.groupby(["SEASON", "TEAM_ID"])
    result = grouped.head(3).reset_index(drop=True)
    result = result[["SEASON", "PLAYER_ID", "TEAM_ID", "PTS"]]
    df2 = df2.rename(columns={"ID": "PLAYER_ID", "NAME": "PLAYER_NAME"})
    df_temp = pd.merge(result, df2, on="PLAYER_ID")
    return df_temp


def getBest3_v2(df, df2):
    df["SEASON"] = df["SEASON"].astype(int)
    df["PTS"] = df["PTS"].astype(float)
    df = df.sort_values(by=["SEASON", "PTS"], ascending=[True, False])
    grouped = df.groupby(["SEASON"])
    result = grouped.head(3).reset_index(drop=True)
    result = result[["SEASON", "PLAYER_ID", "TEAM_ID", "PTS"]]
    df2 = df2.rename(columns={"ID": "PLAYER_ID", "NAME": "PLAYER_NAME"})
    df_temp = pd.merge(result, df2, on="PLAYER_ID")
    return df_temp


def get_players_stats_per_date(season, date, team_id, matchup, sleep_time=1):
    table = pd.DataFrame()
    for attempt in range(7):
        try:
            table = LeagueDashPlayerStats(
                season=season_str(season),
                per_mode_detailed="PerGame",
                date_to_nullable=date.strftime("%m/%d/%Y"),
                team_id_nullable=team_id,
            ).get_data_frames()[0]
            time.sleep(sleep_time)
            labels = ["ABBREV", "CF", "NAME", "NBA_FANTASY_PTS", "RANK"]
            table.drop(labels_to_drop(table.columns, labels), axis=1, inplace=True)
            table.dropna(
                axis=0, how="any", subset=["PLAYER_ID", "TEAM_ID"], inplace=True
            )
        except:
            sleep_time += 8
            continue
        else:
            break
    else:
        print(
            "Issue imposible to fix! in date "
            + str(date.strftime("%Y-%m-%d"))
            + ", match up "
            + matchup
            + ", for team "
            + str(team_id)
        )
    return table


def getNames(df_temp, season, team_id):
    df_temp = df_temp[(df_temp["SEASON"] == season) & (df_temp["TEAM_ID"] == team_id)]
    df_temp = df_temp.reset_index(drop=True)
    p1 = df_temp.at[0, "PLAYER_NAME"]
    p2 = df_temp.at[1, "PLAYER_NAME"]
    p3 = df_temp.at[2, "PLAYER_NAME"]
    return p1, p2, p3


def getNames_v2(df_temp):
    df_temp = df_temp.reset_index(drop=True)
    p1 = df_temp.at[0, "PLAYER_NAME"]
    p2 = df_temp.at[1, "PLAYER_NAME"]
    p3 = df_temp.at[2, "PLAYER_NAME"]
    return p1, p2, p3


def injuredOrNot(df, star, teamID, game_date, conn):
    df_player = df[df["PLAYER"] == star].copy()
    df_player["INJURED"] = (df_player["INJURED_ON"] <= game_date) & (
        df_player["RETURNED"] > game_date
    )

    if df_player["INJURED"].any():
        df_player = df_player[df_player["INJURED"] == True]
        team_short_name = df_player["TEAM"].iloc[0]
        if team_short_name == "Blazers":
            team_short_name = "Trail Blazers"
        cur = conn.cursor()
        cur.execute(f'SELECT ID FROM teams WHERE MASCOT IS "{team_short_name}"')
        team_id = cur.fetchone()[0]
        if int(team_id) == int(teamID):
            return 1
        else:
            cur.execute(f'SELECT MASCOT FROM teams WHERE ID IS "{teamID}"')
            mascot = cur.fetchone()[0]
            print(
                "in date "
                + str(game_date)
                + " "
                + star
                + " end the season in "
                + mascot
                + " but was injured in "
                + team_short_name
            )
            return 99  # 99 is a flag to indicate that the player was injured in another team (Issue)
    else:
        return 0
    
def get_player_name(row, conn):
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT NAME FROM players WHERE ID IS "{row["PLAYER_ID"]}"')
        name = cur.fetchone()[0]
    except:
        name = "Unknown"
    return name

def get_home_team_id(row, conn):
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT ID FROM teams WHERE NAME IS "{row["HOME"]}"')
        team_id = cur.fetchone()[0]
    except:
        print("issue in home team name " + row["HOME"])
        team_id = "Unknown"
    return team_id

def get_away_team_id(row, conn):
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT ID FROM teams WHERE NAME IS "{row["AWAY"]}"')
        team_id = cur.fetchone()[0]
    except:
        print("issue in away team name " + row["AWAY"])
        team_id = "Unknown"
    return team_id




