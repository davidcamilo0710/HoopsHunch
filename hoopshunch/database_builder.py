import json
import sqlite3
import time
import pandas as pd
from nba_api.stats.endpoints.leaguedashplayerstats import LeagueDashPlayerStats
from nba_api.stats.endpoints.leaguedashteamstats import LeagueDashTeamStats
from nba_api.stats.endpoints.leaguegamelog import LeagueGameLog
from nba_api.stats.static import teams as TEAMS
import hoopshunch.util as util
import hoopshunch.templates as templates
import warnings

warnings.filterwarnings("ignore")


def labels_to_drop(column_names, list_of_strings):
    return [
        col for col in column_names if any([x for x in list_of_strings if x in col])
    ]


def season_str(season):
    return str(season) + "-" + str(season + 1)[-2:]


def add_player_game_stats(conn, start_season, end_season, if_exists="append", sleep=1):
    table_name = "player_game_stats"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("DROP TABLE IF EXISTS players")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (PLAYER_ID INTEGER, TEAM_ID INTEGER, GAME_ID TEXT, MIN REAL,
        FGM REAL, FGA REAL, FG3M REAL, FG3A REAL, FTM REAL, FTA REAL, OREB REAL, DREB REAL,
        REB REAL, AST REAL, STL REAL, BLK REAL, TOV REAL, PF REAL, PTS REAL, PLUS_MINUS REAL, FANTASY_PTS REAL)""".format(
            table_name
        )
    )

    conn.execute("CREATE TABLE IF NOT EXISTS players (ID INTEGER, NAME TEXT)")

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " player game stats")
        table = LeagueGameLog(
            season=season_str(season), player_or_team_abbreviation="P"
        ).get_data_frames()[0]
        table.to_sql("temp", conn, if_exists="append", index=False)
        labels = ["ABBREV", "DATE", "MATCHUP", "NAME", "PCT", "SEASON", "VIDEO", "WL"]
        table.drop(labels_to_drop(table.columns, labels), axis=1, inplace=True)
        table.dropna(
            axis=0, how="any", subset=["GAME_ID", "PLAYER_ID", "TEAM_ID"], inplace=True
        )
        table.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)

    query = "SELECT DISTINCT PLAYER_ID AS ID, PLAYER_NAME AS NAME FROM temp ORDER BY ID"
    pd.read_sql(query, conn).to_sql("players", conn, if_exists="append", index=False)
    conn.execute("DROP TABLE temp")
    conn.execute("VACUUM")


def add_player_season_stats(
    conn, start_season, end_season, if_exists="append", sleep=1
):
    table_name = "player_season_stats"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (SEASON INTEGER, PLAYER_ID INTEGER, TEAM_ID INTEGER, AGE REAL,
        GP INTEGER, W INTEGER, L INTEGER, W_PCT REAL, MIN REAL, FGM REAL, FGA REAL, FG_PCT REAL, FG3M REAL, FG3A REAL,
        FG3_PCT REAL, FTM REAL, FTA REAL, FT_PCT REAL, OREB REAL, DREB REAL, REB REAL, AST REAL, TOV REAL, STL REAL,
        BLK REAL, BLKA REAL, PF REAL, PFD REAL, PTS REAL, PLUS_MINUS REAL, DD2 INTEGER, TD3 INTEGER)""".format(
            table_name
        )
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " player season stats")
        table = LeagueDashPlayerStats(
            season=season_str(season), per_mode_detailed="PerGame"
        ).get_data_frames()[0]
        labels = ["ABBREV", "CF", "NAME", "NBA_FANTASY_PTS", "RANK"]
        table.drop(labels_to_drop(table.columns, labels), axis=1, inplace=True)
        table.dropna(axis=0, how="any", subset=["PLAYER_ID", "TEAM_ID"], inplace=True)
        table["SEASON"] = season
        table.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def add_teams(conn):
    print("Reading team information")
    conn.execute("DROP TABLE IF EXISTS teams")
    conn.execute("VACUUM")
    conn.execute(
        "CREATE TABLE teams (ID INTEGER, ABBREVIATION TEXT, MASCOT TEXT, NAME TEXT, CITY TEXT, STATE TEXT, YEAR INTEGER)"
    )
    teams = TEAMS.get_teams()
    teams = json.dumps(teams)
    teams = pd.read_json(teams)
    teams.rename(
        columns={"full_name": "NAME", "nickname": "MASCOT", "year_founded": "YEAR"},
        inplace=True,
    )
    teams.to_sql("teams", conn, if_exists="append", index=False)


def add_team_game_stats(conn, start_season, end_season, if_exists="append", sleep=1):
    table_name = "team_game_stats"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("DROP TABLE IF EXISTS games")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (TEAM_ID INTEGER, GAME_ID TEXT, MIN INTEGER, FGM INTEGER, FGA INTEGER,
        FG3M INTEGER, FG3A INTEGER, FTM INTEGER, FTA INTEGER, OREB INTEGER, DREB INTEGER, REB INTEGER, AST INTEGER,
        STL INTEGER, BLK INTEGER, TOV INTEGER, PF INTEGER, PTS INTEGER, PLUS_MINUS INTEGER)""".format(
            table_name
        )
    )

    conn.execute(
        """CREATE TABLE IF NOT EXISTS games (SEASON INTEGER, ID TEXT, HOME_TEAM_ID INTEGER,
        AWAY_TEAM_ID INTEGER, GAME_DATE TEXT, MATCHUP TEXT, HOME_WL TEXT)"""
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " team game stats")
        table = LeagueGameLog(
            season=season_str(season),
            player_or_team_abbreviation="T",
            date_to_nullable="11/25/2022",
        ).get_data_frames()[0]
        table["SEASON"] = season
        table.to_sql("temp", conn, if_exists="append", index=False)
        labels = ["ABBREV", "DATE", "MATCHUP", "NAME", "PCT", "SEASON", "VIDEO", "WL"]
        table.drop(labels_to_drop(table.columns, labels), axis=1, inplace=True)
        table.dropna(axis=0, how="any", subset=["GAME_ID", "TEAM_ID"], inplace=True)
        table.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)

    query = """
    SELECT SEASON,
        ID,
        HOME_TEAM_ID,
        AWAY_TEAM_ID,
        GAME_DATE,
        MATCHUP,
        HOME_WL
    FROM
        (SELECT TEAM_ID AS HOME_TEAM_ID,
                GAME_ID AS ID,
                SEASON,
                GAME_DATE,
                MATCHUP,
                WL AS HOME_WL
            FROM temp
            WHERE MATCHUP LIKE '%vs%') AS home,
        (SELECT TEAM_ID AS AWAY_TEAM_ID,
                GAME_ID
            FROM temp
            WHERE MATCHUP LIKE '%@%') AS away
    WHERE home.ID = away.GAME_ID
    """
    pd.read_sql(query, conn).to_sql("games", conn, if_exists="append", index=False)
    conn.execute("DROP TABLE temp")
    conn.execute("VACUUM")


def add_team_season_stats(conn, start_season, end_season, if_exists="append", sleep=1):
    table_name = "team_season_stats"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (SEASON INTEGER, TEAM_ID INTEGER, GP INTEGER, W INTEGER, L INTEGER,
        W_PCT REAL, MIN REAL, FGM REAL, FGA REAL, FG_PCT REAL, FG3M REAL, FG3A REAL, FG3_PCT REAL, FTM REAL, FTA REAL,
        FT_PCT REAL, OREB REAL, DREB REAL, REB REAL, AST REAL, TOV REAL, STL REAL, BLK REAL, BLKA REAL, PF REAL,
        PFD REAL, PTS REAL, PLUS_MINUS REAL)""".format(
            table_name
        )
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " team season stats")
        table = LeagueDashTeamStats(season=season_str(season)).get_data_frames()[0]
        labels = ["CF", "NAME", "RANK"]
        table.drop(labels_to_drop(table.columns, labels), axis=1, inplace=True)
        table.dropna(axis=0, how="any", subset=["TEAM_ID"], inplace=True)
        table["SEASON"] = season
        table.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def add_betting(conn, if_exists="append", sleep=1):
    table_name = "betting"
    # we ever drop the table, because the csv is static, the same thing with the injuries
    conn.execute("DROP TABLE IF EXISTS " + table_name)
    conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, HOME_SPREAD REAL, AWAY_SPREAD REAL, OVER_UNDER REAL, HOME_MONEYLINE REAL, AWAY_MONEYLINE REAL)""".format(
            table_name
        )
    )
    cur = conn.cursor()
    print("Reading betting data")
    historical_odds = pd.read_csv("../data/raw/historical_odds.csv")
    historical_odds["GAME_DATE"] = pd.to_datetime(
        historical_odds["GAME_DATE"], format="%d/%m/%y"
    )
    historical_odds["GAME_DATE"] = historical_odds["GAME_DATE"].dt.strftime("%Y-%m-%d")
    for index, row in historical_odds.iterrows():
        home_team = row["HOME"]
        away_team = row["AWAY"]
        cur.execute(f'SELECT ID FROM teams WHERE NAME IS "{home_team}"')
        home_id = cur.fetchone()[0]
        cur.execute(f'SELECT ID FROM teams WHERE NAME IS "{away_team}"')
        away_id = cur.fetchone()[0]
        date = row["GAME_DATE"]
        cur.execute(
            f'SELECT ID FROM games WHERE HOME_TEAM_ID == {home_id} AND AWAY_TEAM_ID == {away_id} AND GAME_DATE IS "{date}"'
        )
        game_id = cur.fetchone()
        if game_id is not None:
            values = (
                game_id[0],
                row["HOME_SPREAD"],
                row["AWAY_SPREAD"],
                row["OVER_UNDER"],
                row["HOME_MONEYLINE"],
                row["AWAY_MONEYLINE"],
            )
            cur.execute(
                """INSERT INTO betting(GAME_ID, HOME_SPREAD, AWAY_SPREAD, OVER_UNDER, HOME_MONEYLINE, AWAY_MONEYLINE)
                                VALUES(?, ?, ?, ?, ?, ?)""",
                values,
            )
            conn.commit()


def add_injuries(conn, if_exists="append", sleep=1):
    table_name = "injuries"
    # we ever drop the table, because the scraped page is static
    conn.execute("DROP TABLE IF EXISTS " + table_name)
    conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (PLAYER TEXT, TEAM TEXT, INJURED_ON TEXT, RETURNED TEXT, DAYS_MISSED INTEGER)""".format(
            table_name
        )
    )
    try:
        print("Scraping injuries")
        injury = util.injuries()
        mask = injury["INJURED_ON"].str.contains("INJURED ON")
        injury = injury[~mask]
        injury["INJURED_ON"] = pd.to_datetime(injury["INJURED_ON"])
        injury["RETURNED"] = pd.to_datetime(injury["RETURNED"])
        injury["INJURED_ON"] = injury["INJURED_ON"].dt.strftime("%Y-%m-%d")
        injury["RETURNED"] = injury["RETURNED"].dt.strftime("%Y-%m-%d")
        injury.sort_values
        injury.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)
    except:
        print("Error reading injuries")
        return


def add_rest_days(conn, start_season, end_season, if_exists="append", sleep=1):
    table_name = "rest_days"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, HOME_REST_DAYS TEXT, AWAY_REST_DAYS TEXT)""".format(
            table_name
        )
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " games rest days")
        query = f"SELECT * FROM games WHERE SEASON = '{season}'"
        games = pd.read_sql(query, conn)
        games = util.addRestDays(games)
        games_to_table = games[["ID", "HOME_REST_DAYS", "AWAY_REST_DAYS"]].copy()
        games_to_table.rename(columns={"ID": "GAME_ID"}, inplace=True)
        games_to_table.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def add_injured_stars(conn, start_season, end_season, if_exists="append", sleep=1):
    table_name = "injured_stars"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, STAR1_HOME TEXT, 
        STAR2_HOME TEXT, STAR3_HOME TEXT, STAR1_AWAY TEXT, STAR2_AWAY TEXT, STAR3_AWAY TEXT,
        INJURED_STAR1_HOME INTEGER, INJURED_STAR2_HOME INTEGER, INJURED_STAR3_HOME INTEGER, 
        INJURED_STAR1_AWAY INTEGER, INJURED_STAR2_AWAY INTEGER, INJURED_STAR3_AWAY INTEGER     
        )""".format(
            table_name
        )
    )

    query = f"SELECT * FROM injuries"
    injury = pd.read_sql(query, conn)
    injury["INJURED_ON"] = pd.to_datetime(injury["INJURED_ON"])
    injury["RETURNED"] = pd.to_datetime(injury["RETURNED"])

    query = f"SELECT * FROM players"
    df_players = pd.read_sql(query, conn)

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " injured stars")

        query = f"SELECT * FROM games WHERE SEASON = '{season}'"
        games = pd.read_sql(query, conn)
        games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])

        query = f"SELECT * FROM player_season_stats WHERE SEASON = '{season}'"
        df_player_season_stats = pd.read_sql(query, conn)
        result = util.getBest3(df_player_season_stats, df_players)
        roster_in_game = templates.create_roster_in_game_table()

        for index, row in games.iterrows():
            if row["GAME_DATE"] <= util.trade_deadline(season):
                roster_yesterday = roster_in_game
                try:
                    result_temp_home = util.get_players_stats_per_date(
                        int(row["SEASON"]),
                        row["GAME_DATE"],
                        row["HOME_TEAM_ID"],
                        row["MATCHUP"],
                        sleep,
                    )
                    result_temp_home["SEASON"] = row["SEASON"]
                    result_temp_home["DATE"] = row["GAME_DATE"]
                    result_temp_home["REAL_TEAM_ID"] = row["HOME_TEAM_ID"]
                    who_leave = result_temp_home[
                        result_temp_home["REAL_TEAM_ID"] != result_temp_home["TEAM_ID"]
                    ]
                    new_df = roster_yesterday[
                        roster_yesterday["REAL_TEAM_ID"] != row["HOME_TEAM_ID"]
                    ]
                    for index2, row2 in who_leave.iterrows():
                        ban1 = new_df["PLAYER_ID"].isin([row2["PLAYER_ID"]]).any()
                        if ban1:
                            result_temp_home = result_temp_home[
                                result_temp_home["PLAYER_ID"] != row2["PLAYER_ID"]
                            ]
                    roster_in_game = roster_in_game.append(
                        result_temp_home, ignore_index=True
                    )
                    roster_yesterday = roster_in_game
                    result_temp_home = util.getBest3_v2(result_temp_home, df_players)
                    p1, p2, p3 = util.getNames_v2(result_temp_home)
                except:
                    print("Use default values")
                    p1, p2, p3 = util.getNames(
                        result, int(row["SEASON"]), int(row["HOME_TEAM_ID"])
                    )
                try:
                    result_temp_away = util.get_players_stats_per_date(
                        int(row["SEASON"]),
                        row["GAME_DATE"],
                        row["AWAY_TEAM_ID"],
                        row["MATCHUP"],
                        sleep,
                    )
                    result_temp_away["SEASON"] = row["SEASON"]
                    result_temp_away["DATE"] = row["GAME_DATE"]
                    result_temp_away["REAL_TEAM_ID"] = row["AWAY_TEAM_ID"]
                    who_leave = result_temp_away[
                        result_temp_away["REAL_TEAM_ID"] != result_temp_away["TEAM_ID"]
                    ]
                    new_df = roster_yesterday[
                        roster_yesterday["REAL_TEAM_ID"] != row["AWAY_TEAM_ID"]
                    ]
                    for index2, row2 in who_leave.iterrows():
                        ban1 = new_df["PLAYER_ID"].isin([row2["PLAYER_ID"]]).any()
                        if ban1:
                            result_temp_away = result_temp_away[
                                result_temp_away["PLAYER_ID"] != row2["PLAYER_ID"]
                            ]
                    roster_in_game = roster_in_game.append(
                        result_temp_away, ignore_index=True
                    )
                    result_temp_away = util.getBest3_v2(result_temp_away, df_players)
                    p4, p5, p6 = util.getNames_v2(result_temp_away)
                except:
                    p4, p5, p6 = util.getNames(
                        result, int(row["SEASON"]), int(row["AWAY_TEAM_ID"])
                    )
            else:
                p1, p2, p3 = util.getNames(
                    result, int(row["SEASON"]), int(row["HOME_TEAM_ID"])
                )
                p4, p5, p6 = util.getNames(
                    result, int(row["SEASON"]), int(row["AWAY_TEAM_ID"])
                )
            p1i = util.injuredOrNot(
                injury, p1, row["HOME_TEAM_ID"], row["GAME_DATE"], conn
            )
            p2i = util.injuredOrNot(
                injury, p2, row["HOME_TEAM_ID"], row["GAME_DATE"], conn
            )
            p3i = util.injuredOrNot(
                injury, p3, row["HOME_TEAM_ID"], row["GAME_DATE"], conn
            )
            p4i = util.injuredOrNot(
                injury, p4, row["AWAY_TEAM_ID"], row["GAME_DATE"], conn
            )
            p5i = util.injuredOrNot(
                injury, p5, row["AWAY_TEAM_ID"], row["GAME_DATE"], conn
            )
            p6i = util.injuredOrNot(
                injury, p6, row["AWAY_TEAM_ID"], row["GAME_DATE"], conn
            )
            games.at[index, "STAR1_HOME"] = p1
            games.at[index, "STAR2_HOME"] = p2
            games.at[index, "STAR3_HOME"] = p3
            games.at[index, "STAR1_AWAY"] = p4
            games.at[index, "STAR2_AWAY"] = p5
            games.at[index, "STAR3_AWAY"] = p6
            games.at[index, "INJURED_STAR1_HOME"] = int(p1i)
            games.at[index, "INJURED_STAR2_HOME"] = int(p2i)
            games.at[index, "INJURED_STAR3_HOME"] = int(p3i)
            games.at[index, "INJURED_STAR1_AWAY"] = int(p4i)
            games.at[index, "INJURED_STAR2_AWAY"] = int(p5i)
            games.at[index, "INJURED_STAR3_AWAY"] = int(p6i)
            # games.to_csv("/Users/davidcamilo07/Desktop/taaaaaaable11111.csv")
        games_to_table = games[
            [
                "ID",
                "STAR1_HOME",
                "STAR2_HOME",
                "STAR3_HOME",
                "STAR1_AWAY",
                "STAR2_AWAY",
                "STAR3_AWAY",
                "INJURED_STAR1_HOME",
                "INJURED_STAR2_HOME",
                "INJURED_STAR3_HOME",
                "INJURED_STAR1_AWAY",
                "INJURED_STAR2_AWAY",
                "INJURED_STAR3_AWAY",
            ]
        ].copy()
        games_to_table.rename(columns={"ID": "GAME_ID"}, inplace=True)
        games_to_table.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def add_team_last5_season_stats(
    conn, start_season, end_season, if_exists="replace", sleep=1
):
    table_name = "game_last5_season_stats"
    table_issues = "issues"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_issues)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, LAST5_HOME_GAMES REAL, LAST5_AWAY_GAMES REAL, 
        LAST5_HOME_AS_HOME REAL, LAST5_AWAY_AS_AWAY REAL, LAST5_HOME_H2H REAL, LAST5_HOME_GP REAL, LAST5_HOME_W REAL, 
        LAST5_HOME_L REAL, LAST5_HOME_W_PCT REAL, LAST5_HOME_MIN REAL, LAST5_HOME_FGM REAL, LAST5_HOME_FGA REAL, 
        LAST5_HOME_FG_PCT REAL, LAST5_HOME_FG3M REAL, LAST5_HOME_FG3A REAL, LAST5_HOME_FG3_PCT REAL, LAST5_HOME_FTM REAL, 
        LAST5_HOME_FTA REAL, LAST5_HOME_FT_PCT REAL, LAST5_HOME_OREB REAL, LAST5_HOME_DREB REAL, LAST5_HOME_REB REAL, 
        LAST5_HOME_AST REAL, LAST5_HOME_TOV REAL, LAST5_HOME_STL REAL, LAST5_HOME_BLK REAL, LAST5_HOME_BLKA REAL, 
        LAST5_HOME_PF REAL, LAST5_HOME_PFD REAL, LAST5_HOME_PTS REAL, LAST5_HOME_PLUS_MINUS REAL, LAST5_AWAY_GP REAL, LAST5_AWAY_W REAL, 
        LAST5_AWAY_L REAL, LAST5_AWAY_W_PCT REAL, LAST5_AWAY_MIN REAL, LAST5_AWAY_FGM REAL, LAST5_AWAY_FGA REAL, LAST5_AWAY_FG_PCT REAL, 
        LAST5_AWAY_FG3M REAL, LAST5_AWAY_FG3A REAL, LAST5_AWAY_FG3_PCT REAL, LAST5_AWAY_FTM REAL, LAST5_AWAY_FTA REAL, LAST5_AWAY_FT_PCT REAL, 
        LAST5_AWAY_OREB REAL, LAST5_AWAY_DREB REAL, LAST5_AWAY_REB REAL, LAST5_AWAY_AST REAL, LAST5_AWAY_TOV REAL, LAST5_AWAY_STL REAL, LAST5_AWAY_BLK REAL, 
        LAST5_AWAY_BLKA REAL, LAST5_AWAY_PF REAL, LAST5_AWAY_PFD REAL, LAST5_AWAY_PTS REAL, LAST5_AWAY_PLUS_MINUS REAL)""".format(
            table_name
        )
    )

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_issues)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (SEASON INTEGER, SEASON_STR TEXT, GAME_ID TEXT, TEAM_ID TEXT, DATE_FROM TEXT, DATE_TO TEXT)""".format(
            table_issues
        )
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " per game last 5 and season stats")
        query = f"SELECT * FROM games WHERE SEASON = '{season}'"
        games = pd.read_sql(query, conn)
        games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
        games.rename(columns={"ID": "GAME_ID"}, inplace=True)
        copy = games.copy()
        bandera = pd.DataFrame()

        query = f"SELECT * FROM games"
        gamesH2H = pd.read_sql(query, conn)
        gamesH2H["GAME_DATE"] = pd.to_datetime(gamesH2H["GAME_DATE"])
        gamesH2H.rename(columns={"ID": "GAME_ID"}, inplace=True)

        for index, row in games.iterrows():
            last5_stats = util.GetLast5Games(
                copy,
                row["GAME_DATE"],
                row["HOME_TEAM_ID"],
                row["AWAY_TEAM_ID"],
                sleep,
                gamesH2H,
                row["GAME_ID"],
                conn,
            )
            temp_df = pd.DataFrame(row).T
            temp_df = temp_df.reset_index(drop=True)
            last5_stats = last5_stats.reset_index(drop=True)
            result_df = pd.concat([temp_df, last5_stats], axis=1)
            bandera = pd.concat([bandera, result_df]).reset_index(drop=True)
        bandera = bandera.drop(
            [
                "SEASON",
                "HOME_TEAM_ID",
                "AWAY_TEAM_ID",
                "GAME_DATE",
                "MATCHUP",
                "HOME_WL",
            ],
            axis=1,
        )
        bandera.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def add_game_season_record(
    conn, start_season, end_season, if_exists="replace", sleep=1
):
    table_name = "game_season_record"
    table_issues = "issues_season_record"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, SEASON_HOME_GP INTEGER, SEASON_HOME_W INTEGER, SEASON_HOME_L INTEGER, SEASON_HOME_W_PCT REAL, 
        SEASON_HOME_PTS REAL, SEASON_HOME_W_RANK INTEGER, SEASON_HOME_L_RANK INTEGER, SEASON_HOME_W_PCT_RANK INTEGER, SEASON_HOME_PTS_RANK INTEGER, 
        SEASON_AWAY_GP INTEGER, SEASON_AWAY_W INTEGER, SEASON_AWAY_L INTEGER, SEASON_AWAY_W_PCT REAL, SEASON_AWAY_PTS REAL, SEASON_AWAY_W_RANK INTEGER, 
        SEASON_AWAY_L_RANK INTEGER, SEASON_AWAY_W_PCT_RANK INTEGER, SEASON_AWAY_PTS_RANK INTEGER)""".format(
            table_name
        )
    )

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_issues)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (SEASON INTEGER, SEASON_STR TEXT, GAME_ID TEXT, TEAM_ID TEXT, DATE_FROM TEXT, DATE_TO TEXT)""".format(
            table_issues
        )
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " per game season records")
        query = f"SELECT * FROM games WHERE SEASON = '{season}'"
        games = pd.read_sql(query, conn)
        games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
        games.rename(columns={"ID": "GAME_ID"}, inplace=True)
        copy = games.copy()
        bandera = pd.DataFrame()

        for index, row in games.iterrows():
            last5_stats = util.GetLast5GamesV2(
                copy,
                row["GAME_DATE"],
                row["HOME_TEAM_ID"],
                row["AWAY_TEAM_ID"],
                sleep,
                row["GAME_ID"],
                conn,
            )
            temp_df = pd.DataFrame(row).T
            temp_df = temp_df.reset_index(drop=True)
            last5_stats = last5_stats.reset_index(drop=True)
            result_df = pd.concat([temp_df, last5_stats], axis=1)
            bandera = pd.concat([bandera, result_df]).reset_index(drop=True)
            print(result_df.columns)
        bandera = bandera.drop(
            [
                "SEASON",
                "HOME_TEAM_ID",
                "AWAY_TEAM_ID",
                "GAME_DATE",
                "MATCHUP",
                "HOME_WL",
            ],
            axis=1,
        )
        bandera.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def add_game_best5_players(
    conn, start_season, end_season, if_exists="replace", sleep=1
):
    table_name = "game_best5_players"

    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS " + table_name)
        conn.execute("VACUUM")

    conn.execute(
        """CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, HOME_P1_PLAYER TEXT, HOME_P1_MIN REAL, HOME_P1_PTS, HOME_P1_AST, HOME_P1_DREB, 
        HOME_P1_OREB, HOME_P1_BLK, HOME_P1_STL, HOME_P1_TOV, HOME_P1_FGA, HOME_P1_FTA, HOME_P1_FG3A, HOME_P1_PF, HOME_P2_PLAYER TEXT, HOME_P2_MIN REAL, 
        HOME_P2_PTS, HOME_P2_AST, HOME_P2_DREB, HOME_P2_OREB, HOME_P2_BLK, HOME_P2_STL, HOME_P2_TOV, HOME_P2_FGA, HOME_P2_FTA, HOME_P2_FG3A, HOME_P2_PF, HOME_P3_PLAYER TEXT, 
        HOME_P3_MIN REAL, HOME_P3_PTS, HOME_P3_AST, HOME_P3_DREB, HOME_P3_OREB, HOME_P3_BLK, HOME_P3_STL, HOME_P3_TOV, HOME_P3_FGA, HOME_P3_FTA, 
        HOME_P3_FG3A, HOME_P3_PF, HOME_P4_PLAYER TEXT, HOME_P4_MIN REAL, HOME_P4_PTS, HOME_P4_AST, HOME_P4_DREB, HOME_P4_OREB, HOME_P4_BLK, HOME_P4_STL, HOME_P4_TOV, 
        HOME_P4_FGA, HOME_P4_FTA, HOME_P4_FG3A, HOME_P4_PF, HOME_P5_PLAYER TEXT, HOME_P5_MIN REAL, HOME_P5_PTS, HOME_P5_AST, HOME_P5_DREB, HOME_P5_OREB, HOME_P5_BLK, HOME_P5_STL, 
        HOME_P5_TOV, HOME_P5_FGA, HOME_P5_FTA, HOME_P5_FG3A, HOME_P5_PF, AWAY_P1_PLAYER TEXT, AWAY_P1_MIN REAL, AWAY_P1_PTS, AWAY_P1_AST, AWAY_P1_DREB, AWAY_P1_OREB, AWAY_P1_BLK, 
        AWAY_P1_STL, AWAY_P1_TOV, AWAY_P1_FGA, AWAY_P1_FTA, AWAY_P1_FG3A, AWAY_P1_PF, AWAY_P2_PLAYER TEXT, AWAY_P2_MIN REAL, AWAY_P2_PTS, AWAY_P2_AST, AWAY_P2_DREB, 
        AWAY_P2_OREB, AWAY_P2_BLK, AWAY_P2_STL, AWAY_P2_TOV, AWAY_P2_FGA, AWAY_P2_FTA, AWAY_P2_FG3A, AWAY_P2_PF, AWAY_P3_PLAYER TEXT, AWAY_P3_MIN REAL,
        AWAY_P3_PTS, AWAY_P3_AST, AWAY_P3_DREB, AWAY_P3_OREB, AWAY_P3_BLK, AWAY_P3_STL, AWAY_P3_TOV, AWAY_P3_FGA, AWAY_P3_FTA, AWAY_P3_FG3A, AWAY_P3_PF, AWAY_P4_PLAYER TEXT, 
        AWAY_P4_MIN REAL, AWAY_P4_PTS, AWAY_P4_AST, AWAY_P4_DREB, AWAY_P4_OREB, AWAY_P4_BLK, AWAY_P4_STL, AWAY_P4_TOV, AWAY_P4_FGA, AWAY_P4_FTA, AWAY_P4_FG3A, AWAY_P4_PF, 
        AWAY_P5_PLAYER TEXT, AWAY_P5_MIN REAL, AWAY_P5_PTS, AWAY_P5_AST, AWAY_P5_DREB, AWAY_P5_OREB, AWAY_P5_BLK, AWAY_P5_STL, AWAY_P5_TOV, AWAY_P5_FGA, AWAY_P5_FTA, AWAY_P5_FG3A, AWAY_P5_PF
        
        
        )""".format(
            table_name
        )
    )

    for season in range(start_season, end_season + 1):
        print("Reading " + season_str(season) + " per game best 5 players stats")
        query = f"SELECT * FROM games WHERE SEASON = '{season}'"
        games = pd.read_sql(query, conn)
        games = games[["ID", "HOME_TEAM_ID", "AWAY_TEAM_ID"]]
        games.rename(columns={"ID": "GAME_ID"}, inplace=True)
        bandera = pd.DataFrame()

        for index, row in games.iterrows():
            query = f"SELECT * FROM player_game_stats WHERE GAME_ID = '{row['GAME_ID']}' AND TEAM_ID = '{row['HOME_TEAM_ID']}'"
            home = pd.read_sql(query, conn)
            home = home.sort_values(by=["MIN"], ascending=[False])
            home.drop(home.index[5:], inplace=True)
            home = home.sort_values(by=["PTS"], ascending=[False]).reset_index(
                drop=True
            )
            home["PLAYER"] = home.apply(util.get_player_name, args=(conn,), axis=1)
            home = home[
                [
                    "PLAYER",
                    "MIN",
                    "PTS",
                    "AST",
                    "DREB",
                    "OREB",
                    "BLK",
                    "STL",
                    "TOV",
                    "FGA",
                    "FTA",
                    "FG3A",
                    "PF",
                ]
            ]
            diccionario_dataframes = {}
            for i, row2 in home.iterrows():
                prefijo = f"HOME_P{i+1}_"
                df_temporal = (
                    pd.DataFrame(row2).T.add_prefix(prefijo).reset_index(drop=True)
                )
                diccionario_dataframes[f"df_{i}"] = df_temporal
            try:
                df_home = pd.concat(diccionario_dataframes.values(), axis=1)
            except:
                print("error in game: " + str(row["GAME_ID"]))
                df_home = pd.DataFrame()
            query = f"SELECT * FROM player_game_stats WHERE GAME_ID = '{row['GAME_ID']}' AND TEAM_ID = '{row['AWAY_TEAM_ID']}'"
            away = pd.read_sql(query, conn)
            away = away.sort_values(by=["MIN"], ascending=[False])
            away = away.iloc[:5]
            away = away.sort_values(by=["PTS"], ascending=[False]).reset_index(
                drop=True
            )
            away["PLAYER"] = away.apply(util.get_player_name, args=(conn,), axis=1)
            away = away[
                [
                    "PLAYER",
                    "MIN",
                    "PTS",
                    "AST",
                    "DREB",
                    "OREB",
                    "BLK",
                    "STL",
                    "TOV",
                    "FGA",
                    "FTA",
                    "FG3A",
                    "PF",
                ]
            ]
            diccionario_dataframes2 = {}
            for i, row2 in away.iterrows():
                prefijo = f"AWAY_P{i+1}_"
                df_temporal = (
                    pd.DataFrame(row2).T.add_prefix(prefijo).reset_index(drop=True)
                )
                diccionario_dataframes2[f"df_{i}"] = df_temporal
            try:
                df_away = pd.concat(diccionario_dataframes2.values(), axis=1)
            except:
                print("error in game: " + str(row["GAME_ID"]))
                df_away = pd.DataFrame()
            result = pd.concat([df_home, df_away], axis=1)
            result["GAME_ID"] = row["GAME_ID"]
            bandera = pd.concat([bandera, result]).reset_index(drop=True)

        bandera.to_sql(table_name, conn, if_exists="append", index=False)
        time.sleep(sleep)


def build_database(database, start_season, end_season, if_exists="replace", sleep=3):
    conn = sqlite3.connect(database)
    if if_exists == "replace":
        add_teams(conn)

    add_player_game_stats(conn, start_season, end_season, if_exists, sleep)
    add_player_season_stats(conn, start_season, end_season, if_exists, sleep)
    add_team_game_stats(conn, start_season, end_season, if_exists, sleep)
    add_team_season_stats(conn, start_season, end_season, if_exists, sleep)
    add_betting(conn, if_exists, sleep)
    add_rest_days(conn, start_season, end_season, if_exists, sleep)
    add_injuries(conn, if_exists, sleep)
    add_injured_stars(
        conn, start_season, end_season, if_exists, sleep
    )  # <--- HERE is important to calibrate the sleep depending the hours of the day
    add_team_last5_season_stats(
        conn, start_season, end_season, if_exists, sleep
    )  # <--- HERE is important to calibrate the sleep depending the hours of the day
    add_game_season_record(
        conn, start_season, end_season, if_exists, sleep
    )  # <--- HERE is important to calibrate the sleep depending the hours of the day
    add_game_best5_players(conn, start_season, end_season, if_exists, sleep)
    conn.close()
