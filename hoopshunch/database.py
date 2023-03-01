import sqlite3
from collections import namedtuple
from itertools import product, starmap
import numpy as np
import pandas as pd
from datetime import datetime


class Database:
    def __init__(self, database):
        self.__conn = sqlite3.connect(database)

        select_results = """
            SEASON,
            home.GAME_ID as GAME_ID,
            TEAM_ID,
            TEAM_PTS,
            OPP_ID,
            OPP_PTS,
            HOME_WL
        """

        team_stats_str_results = """
            games.SEASON,
            GAME_ID,
            TEAM_ID,
            PTS AS TEAM_PTS,
            HOME_WL
        """

        opp_stats_str_results = """
            GAME_ID,
            TEAM_ID AS OPP_ID,
            PTS AS OPP_PTS
        """

        self.__game_results_query = """
            SELECT {0}
            FROM
                (SELECT {1}
                    FROM team_game_stats
                    JOIN games
                    ON TEAM_ID = games.HOME_TEAM_ID AND GAME_ID = games.ID) as home,
                (SELECT {2}
                    FROM team_game_stats
                    JOIN games
                    ON OPP_ID = games.AWAY_TEAM_ID AND GAME_ID = games.ID) as away
            WHERE home.GAME_ID = away.GAME_ID
            UNION
            SELECT {0}
            FROM
                (SELECT {1}
                    FROM team_game_stats
                    JOIN games
                    ON TEAM_ID = games.AWAY_TEAM_ID AND GAME_ID = games.ID) as home,
                (SELECT {2}
                    FROM team_game_stats
                    JOIN games
                    ON OPP_ID = games.HOME_TEAM_ID AND GAME_ID = games.ID) as away
            WHERE home.GAME_ID = away.GAME_ID
        """.format(
            select_results, team_stats_str_results, opp_stats_str_results
        )

    def game_results(self):
        return pd.read_sql(self.__game_results_query, self.__conn)

    def betting_stats(self):
        data = self.game_results()

        games = pd.read_sql(
            "SELECT * FROM games JOIN betting ON games.ID is betting.GAME_ID",
            self.__conn,
        )
        games = games.merge(
            data,
            left_on=["SEASON", "ID", "HOME_TEAM_ID"],
            right_on=["SEASON", "GAME_ID", "TEAM_ID"],
        )
        games = games.merge(
            data,
            left_on=["SEASON", "ID", "AWAY_TEAM_ID"],
            right_on=["SEASON", "GAME_ID", "TEAM_ID"],
            suffixes=("", "_AWAY"),
        )

        labels = [
            "ID",
            "GAME_ID_y",
            "GAME_ID_x",
            "HOME_WL_y",
            "HOME_WL_x",
            "TEAM_ID_AWAY",
            "TEAM_PTS_AWAY",
            "OPP_ID_AWAY",
            "OPP_PTS_AWAY",
            "TEAM_ID",
            "OPP_ID",
        ]
        games.drop(labels, axis=1, inplace=True)
        games.rename(
            columns={"TEAM_PTS": "HOME_PTS", "OPP_PTS": "AWAY_PTS"}, inplace=True
        )

        return games

    def create_model_stats(self):
        data = self.game_results()

        games = pd.read_sql(
            "SELECT * FROM games JOIN betting ON games.ID is betting.GAME_ID JOIN rest_days ON games.ID is rest_days.GAME_ID JOIN injured_stars ON games.ID is injured_stars.GAME_ID JOIN game_season_record ON games.ID is game_season_record.GAME_ID JOIN game_last5_season_stats ON games.ID is game_last5_season_stats.GAME_ID JOIN game_best5_players ON games.ID is game_best5_players.GAME_ID",
            self.__conn,
        )

        games = games.merge(
            data,
            left_on=["SEASON", "ID", "HOME_TEAM_ID"],
            right_on=["SEASON", "GAME_ID", "TEAM_ID"],
        )
        games = games.merge(
            data,
            left_on=["SEASON", "ID", "AWAY_TEAM_ID"],
            right_on=["SEASON", "GAME_ID", "TEAM_ID"],
            suffixes=("", "_AWAY"),
        )

        labels = [
            "ID",
            "GAME_ID_y",
            "GAME_ID_x",
            "HOME_WL_y",
            "HOME_WL_x",
            "TEAM_ID_AWAY",
            "TEAM_PTS_AWAY",
            "OPP_ID_AWAY",
            "OPP_PTS_AWAY",
            "TEAM_ID",
            "OPP_ID",
        ]
        games.drop(labels, axis=1, inplace=True)
        games.rename(
            columns={"TEAM_PTS": "HOME_PTS", "OPP_PTS": "AWAY_PTS"}, inplace=True
        )

        return games

    def upload_model_stats(self, model_stats, if_exists="replace"):

        table_name = "complete_model_stats"

        if if_exists == "replace":
            self.__conn.execute("DROP TABLE IF EXISTS " + table_name)
            self.__conn.execute("VACUUM")

        self.__conn.execute(
            """CREATE TABLE IF NOT EXISTS {} (SEASON INTEGER,GAME_ID TEXT,GAME_DATE TEXT,HOME_TEAM_ID INTEGER,AWAY_TEAM_ID INTEGER,
            MATCHUP TEXT,HOME_SPREAD REAL,AWAY_SPREAD REAL,HOME_SPREAD_PROB REAL,AWAY_SPREAD_PROB REAL,HOME_MONEYLINE REAL,
            AWAY_MONEYLINE REAL,HOME_MONEYLINE_DECIMAL REAL,AWAY_MONEYLINE_DECIMAL REAL,OVER_UNDER REAL,HOME_REST_DAYS TEXT,
            AWAY_REST_DAYS TEXT,HOME_FATIGUE_LEVEL REAL,AWAY_FATIGUE_LEVEL REAL,HOME_PTS INTEGER,AWAY_PTS INTEGER,TOTAL_PTS INTEGER,
            HOME_WL INTEGER,HOME_SPREAD_WL INTEGER,AWAY_SPREAD_WL INTEGER,OVER_UNDER_WL INTEGER,STAR1_HOME TEXT,STAR2_HOME TEXT,STAR3_HOME TEXT,STAR1_AWAY TEXT,STAR2_AWAY TEXT,
            STAR3_AWAY TEXT,INJURED_STAR1_HOME INTEGER,INJURED_STAR2_HOME INTEGER,INJURED_STAR3_HOME INTEGER,INJURED_STAR1_AWAY INTEGER,
            INJURED_STAR2_AWAY INTEGER,INJURED_STAR3_AWAY INTEGER,SEASON_HOME_GP INTEGER,SEASON_HOME_W INTEGER,SEASON_HOME_L INTEGER,
            SEASON_HOME_W_PCT REAL,SEASON_HOME_PTS REAL,SEASON_HOME_W_RANK INTEGER,SEASON_HOME_L_RANK INTEGER,SEASON_HOME_W_PCT_RANK INTEGER,
            SEASON_HOME_PTS_RANK INTEGER,SEASON_AWAY_GP INTEGER,SEASON_AWAY_W INTEGER,SEASON_AWAY_L INTEGER,SEASON_AWAY_W_PCT REAL,
            SEASON_AWAY_PTS REAL,SEASON_AWAY_W_RANK INTEGER,SEASON_AWAY_L_RANK INTEGER,SEASON_AWAY_W_PCT_RANK INTEGER,
            SEASON_AWAY_PTS_RANK INTEGER,LAST5_HOME_GAMES REAL,LAST5_AWAY_GAMES REAL,LAST5_HOME_AS_HOME REAL,LAST5_AWAY_AS_AWAY REAL,
            LAST5_HOME_H2H REAL,LAST5_HOME_GP INTEGER,LAST5_HOME_W INTEGER,LAST5_HOME_L INTEGER,LAST5_HOME_W_PCT REAL,LAST5_HOME_MIN REAL,
            LAST5_HOME_FGM REAL,LAST5_HOME_FGA REAL,LAST5_HOME_FG_PCT REAL,LAST5_HOME_FG3M REAL,LAST5_HOME_FG3A REAL,LAST5_HOME_FG3_PCT REAL,
            LAST5_HOME_FTM REAL,LAST5_HOME_FTA REAL,LAST5_HOME_FT_PCT REAL,LAST5_HOME_OREB REAL,LAST5_HOME_DREB REAL,LAST5_HOME_REB REAL,
            LAST5_HOME_AST REAL,LAST5_HOME_TOV REAL,LAST5_HOME_STL REAL,LAST5_HOME_BLK REAL,LAST5_HOME_BLKA REAL,LAST5_HOME_PF REAL,
            LAST5_HOME_PFD REAL,LAST5_HOME_PTS REAL,LAST5_HOME_PLUS_MINUS REAL,LAST5_AWAY_GP INTEGER,LAST5_AWAY_W INTEGER,
            LAST5_AWAY_L INTEGER,LAST5_AWAY_W_PCT REAL,LAST5_AWAY_MIN REAL,LAST5_AWAY_FGM REAL,LAST5_AWAY_FGA REAL,LAST5_AWAY_FG_PCT REAL,
            LAST5_AWAY_FG3M REAL,LAST5_AWAY_FG3A REAL,LAST5_AWAY_FG3_PCT REAL,LAST5_AWAY_FTM REAL,LAST5_AWAY_FTA REAL,LAST5_AWAY_FT_PCT REAL,
            LAST5_AWAY_OREB REAL,LAST5_AWAY_DREB REAL,LAST5_AWAY_REB REAL,LAST5_AWAY_AST REAL,LAST5_AWAY_TOV REAL,LAST5_AWAY_STL REAL,
            LAST5_AWAY_BLK REAL,LAST5_AWAY_BLKA REAL,LAST5_AWAY_PF REAL,LAST5_AWAY_PFD REAL,LAST5_AWAY_PTS REAL,LAST5_AWAY_PLUS_MINUS REAL,
            HOME_P1_PLAYER TEXT,HOME_P1_MIN INTEGER,HOME_P1_PTS INTEGER,HOME_P1_AST INTEGER,HOME_P1_DREB INTEGER,HOME_P1_OREB INTEGER,
            HOME_P1_BLK INTEGER,HOME_P1_STL INTEGER,HOME_P1_TOV INTEGER,HOME_P1_FGA INTEGER,HOME_P1_FTA INTEGER,HOME_P1_FG3A INTEGER,
            HOME_P1_PF INTEGER,HOME_P2_PLAYER TEXT,HOME_P2_MIN INTEGER,HOME_P2_PTS INTEGER,HOME_P2_AST INTEGER,HOME_P2_DREB INTEGER,
            HOME_P2_OREB INTEGER,HOME_P2_BLK INTEGER,HOME_P2_STL INTEGER,HOME_P2_TOV INTEGER,HOME_P2_FGA INTEGER,HOME_P2_FTA INTEGER,
            HOME_P2_FG3A INTEGER,HOME_P2_PF INTEGER,HOME_P3_PLAYER TEXT,HOME_P3_MIN INTEGER,HOME_P3_PTS INTEGER,HOME_P3_AST INTEGER,
            HOME_P3_DREB INTEGER,HOME_P3_OREB INTEGER,HOME_P3_BLK INTEGER,HOME_P3_STL INTEGER,HOME_P3_TOV INTEGER,HOME_P3_FGA INTEGER,
            HOME_P3_FTA INTEGER,HOME_P3_FG3A INTEGER,HOME_P3_PF INTEGER,HOME_P4_PLAYER TEXT,HOME_P4_MIN INTEGER,HOME_P4_PTS INTEGER,
            HOME_P4_AST INTEGER,HOME_P4_DREB INTEGER,HOME_P4_OREB INTEGER,HOME_P4_BLK INTEGER,HOME_P4_STL INTEGER,HOME_P4_TOV INTEGER,
            HOME_P4_FGA INTEGER,HOME_P4_FTA INTEGER,HOME_P4_FG3A INTEGER,HOME_P4_PF INTEGER,HOME_P5_PLAYER TEXT,HOME_P5_MIN INTEGER,
            HOME_P5_PTS INTEGER,HOME_P5_AST INTEGER,HOME_P5_DREB INTEGER,HOME_P5_OREB INTEGER,HOME_P5_BLK INTEGER,HOME_P5_STL INTEGER,
            HOME_P5_TOV INTEGER,HOME_P5_FGA INTEGER,HOME_P5_FTA INTEGER,HOME_P5_FG3A INTEGER,HOME_P5_PF INTEGER,AWAY_P1_PLAYER TEXT,
            AWAY_P1_MIN INTEGER,AWAY_P1_PTS INTEGER,AWAY_P1_AST INTEGER,AWAY_P1_DREB INTEGER,AWAY_P1_OREB INTEGER,AWAY_P1_BLK INTEGER,
            AWAY_P1_STL INTEGER,AWAY_P1_TOV INTEGER,AWAY_P1_FGA INTEGER,AWAY_P1_FTA INTEGER,AWAY_P1_FG3A INTEGER,AWAY_P1_PF INTEGER,
            AWAY_P2_PLAYER TEXT,AWAY_P2_MIN INTEGER,AWAY_P2_PTS INTEGER,AWAY_P2_AST INTEGER,AWAY_P2_DREB INTEGER,AWAY_P2_OREB INTEGER,
            AWAY_P2_BLK INTEGER,AWAY_P2_STL INTEGER,AWAY_P2_TOV INTEGER,AWAY_P2_FGA INTEGER,AWAY_P2_FTA INTEGER,AWAY_P2_FG3A INTEGER,
            AWAY_P2_PF INTEGER,AWAY_P3_PLAYER TEXT,AWAY_P3_MIN INTEGER,AWAY_P3_PTS INTEGER,AWAY_P3_AST INTEGER,AWAY_P3_DREB INTEGER,
            AWAY_P3_OREB INTEGER,AWAY_P3_BLK INTEGER,AWAY_P3_STL INTEGER,AWAY_P3_TOV INTEGER,AWAY_P3_FGA INTEGER,AWAY_P3_FTA INTEGER,
            AWAY_P3_FG3A INTEGER,AWAY_P3_PF INTEGER,AWAY_P4_PLAYER TEXT,AWAY_P4_MIN INTEGER,AWAY_P4_PTS INTEGER,AWAY_P4_AST INTEGER,
            AWAY_P4_DREB INTEGER,AWAY_P4_OREB INTEGER,AWAY_P4_BLK INTEGER,AWAY_P4_STL INTEGER,AWAY_P4_TOV INTEGER,AWAY_P4_FGA INTEGER,
            AWAY_P4_FTA INTEGER,AWAY_P4_FG3A INTEGER,AWAY_P4_PF INTEGER,AWAY_P5_PLAYER TEXT,AWAY_P5_MIN INTEGER,AWAY_P5_PTS INTEGER,
            AWAY_P5_AST INTEGER,AWAY_P5_DREB INTEGER,AWAY_P5_OREB INTEGER,AWAY_P5_BLK INTEGER,AWAY_P5_STL INTEGER,AWAY_P5_TOV INTEGER,
            AWAY_P5_FGA INTEGER,AWAY_P5_FTA INTEGER,AWAY_P5_FG3A INTEGER,AWAY_P5_PF INTEGER)""".format(
                table_name
            )
        )
        model_stats.to_sql(table_name, self.__conn, if_exists="append", index=False)
        
    def get_model_stats(self, table_name="complete_model_stats"):
        return pd.read_sql_query("SELECT * FROM {}".format(table_name), self.__conn)
