#!/usr/bin/env python3
"""
NBA Data Processor for RAG-LLM Chatbot

This script processes NBA data collected by nba_data_collector.py and
transforms it into documents suitable for insertion into a MongoDB Atlas
vector database.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_data_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NBADataProcessor:
    """
    Processes NBA data and transforms it into documents suitable for
    insertion into a MongoDB Atlas vector database.
    """
    
    def __init__(self, data_dir: str = "nba_data", output_dir: str = "nba_processed_data"):
        """
        Initialize the NBA data processor.
        
        Args:
            data_dir: Directory containing collected NBA data
            output_dir: Directory to save processed data
        """
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Load team and player data for reference
        self._load_reference_data()
        
        logger.info("NBA Data Processor initialized")
    
    def _load_reference_data(self):
        """
        Load reference data for teams and players.
        """
        try:
            # Load teams
            with open(f"{self.data_dir}/all_teams.json", "r") as f:
                self.teams = json.load(f)
            
            # Create team lookup by ID
            self.team_lookup = {team["id"]: team for team in self.teams}
            
            # Load players
            with open(f"{self.data_dir}/all_players.json", "r") as f:
                self.players = json.load(f)
            
            # Create player lookup by ID
            self.player_lookup = {player["id"]: player for player in self.players}
            
            logger.info(f"Loaded reference data: {len(self.teams)} teams, {len(self.players)} players")
        except FileNotFoundError as e:
            logger.error(f"Reference data not found: {e}")
            raise
    
    def _format_player_basic_info(self, player_id: int) -> str:
        """
        Format basic player information as a natural language text.
        
        Args:
            player_id: NBA API player ID
            
        Returns:
            Formatted text about the player
        """
        try:
            # Load player data
            with open(f"{self.data_dir}/player_{player_id}.json", "r") as f:
                player_data = json.load(f)
            
            player = player_data["player"]
            info = player_data["info"]
            
            # Extract common player info
            common_info = info.get("CommonPlayerInfo", [{}])[0] if "CommonPlayerInfo" in info else {}
            
            # Format text
            name = player.get("full_name", "")
            position = common_info.get("POSITION", "")
            height = common_info.get("HEIGHT", "")
            weight = common_info.get("WEIGHT", "")
            birthdate = common_info.get("BIRTHDATE", "")
            country = common_info.get("COUNTRY", "")
            school = common_info.get("SCHOOL", "")
            draft_year = common_info.get("DRAFT_YEAR", "")
            draft_round = common_info.get("DRAFT_ROUND", "")
            draft_number = common_info.get("DRAFT_NUMBER", "")
            
            # Get current team
            team_id = common_info.get("TEAM_ID")
            team_name = self.team_lookup.get(team_id, {}).get("full_name", "") if team_id else ""
            
            # Format draft info
            draft_info = ""
            if draft_year and draft_round and draft_number:
                if draft_round == "1" and draft_number == "1":
                    draft_info = f"He was selected first overall in the {draft_year} NBA Draft"
                else:
                    draft_info = f"He was selected {draft_number}th in round {draft_round} of the {draft_year} NBA Draft"
                
                if team_id:
                    draft_info += f" by the {team_name}."
                else:
                    draft_info += "."
            
            # Format text
            text = f"{name} is a professional basketball player"
            
            if team_name:
                text += f" for the {team_name}"
            
            text += "."
            
            if birthdate:
                try:
                    birth_date = datetime.strptime(birthdate, "%Y-%m-%dT%H:%M:%S")
                    text += f" Born on {birth_date.strftime('%B %d, %Y')}"
                    
                    if country and country.lower() != "usa":
                        text += f" in {country}"
                    
                    text += ","
                except:
                    pass
            
            if height or weight:
                text += " he"
                
                if height:
                    text += f" stands {height}"
                
                if height and weight:
                    text += " and"
                
                if weight:
                    text += f" weighs {weight} pounds"
                
                text += "."
            
            if position:
                text += f" He plays the {position} position."
            
            if draft_info:
                text += f" {draft_info}"
            
            if school and school.lower() != "none":
                text += f" He attended {school}."
            
            return text
        except Exception as e:
            logger.error(f"Error formatting player basic info for player ID {player_id}: {e}")
            return f"Information about player with ID {player_id}."
    
    def _format_player_career_stats(self, player_id: int) -> str:
        """
        Format player career statistics as a natural language text.
        
        Args:
            player_id: NBA API player ID
            
        Returns:
            Formatted text about the player's career statistics
        """
        try:
            # Load player data
            with open(f"{self.data_dir}/player_{player_id}.json", "r") as f:
                player_data = json.load(f)
            
            player = player_data["player"]
            career_stats = player_data["career_stats"]
            
            # Get player name
            name = player.get("full_name", "")
            
            # Extract career totals
            career_totals = []
            if "CareerTotalsRegularSeason" in career_stats:
                career_totals = career_stats["CareerTotalsRegularSeason"]
            
            if not career_totals:
                return f"{name} has no recorded career statistics in the NBA."
            
            # Get the first (and only) row of career totals
            totals = career_totals[0] if career_totals else {}
            
            # Calculate career averages
            games = totals.get("GP", 0)
            seasons = totals.get("SEASON_COUNT", 0)
            
            if games == 0:
                return f"{name} has not played any games in the NBA."
            
            ppg = round(totals.get("PTS", 0) / games, 1)
            rpg = round(totals.get("REB", 0) / games, 1)
            apg = round(totals.get("AST", 0) / games, 1)
            spg = round(totals.get("STL", 0) / games, 1)
            bpg = round(totals.get("BLK", 0) / games, 1)
            
            # Calculate shooting percentages
            fg_pct = round(totals.get("FG_PCT", 0) * 100, 1)
            fg3_pct = round(totals.get("FG3_PCT", 0) * 100, 1)
            ft_pct = round(totals.get("FT_PCT", 0) * 100, 1)
            
            # Format text
            text = f"{name} Career NBA Statistics: {ppg} points per game, {rpg} rebounds per game, {apg} assists per game, {spg} steals per game, {bpg} blocks per game, {fg_pct}% field goal percentage, {fg3_pct}% three-point percentage, {ft_pct}% free throw percentage"
            
            if seasons > 0:
                if seasons == 1:
                    text += " in 1 season."
                else:
                    text += f" across {seasons} seasons."
            else:
                text += "."
            
            return text
        except Exception as e:
            logger.error(f"Error formatting career stats for player ID {player_id}: {e}")
            return f"Career statistics for player with ID {player_id}."
    
    def _format_player_season_stats(self, player_id: int) -> List[str]:
        """
        Format player season statistics as natural language texts.
        
        Args:
            player_id: NBA API player ID
            
        Returns:
            List of formatted texts about the player's season statistics
        """
        try:
            # Load player data
            with open(f"{self.data_dir}/player_{player_id}.json", "r") as f:
                player_data = json.load(f)
            
            player = player_data["player"]
            career_stats = player_data["career_stats"]
            
            # Get player name
            name = player.get("full_name", "")
            
            # Extract season stats
            season_stats = []
            if "SeasonTotalsRegularSeason" in career_stats:
                season_stats = career_stats["SeasonTotalsRegularSeason"]
            
            if not season_stats:
                return [f"{name} has no recorded season statistics in the NBA."]
            
            # Format text for each season
            texts = []
            
            for season in season_stats:
                season_id = season.get("SEASON_ID", "")
                team_id = season.get("TEAM_ID")
                team_name = self.team_lookup.get(team_id, {}).get("full_name", "") if team_id else "N/A"
                
                games = season.get("GP", 0)
                
                if games == 0:
                    continue
                
                ppg = round(season.get("PTS", 0) / games, 1)
                rpg = round(season.get("REB", 0) / games, 1)
                apg = round(season.get("AST", 0) / games, 1)
                spg = round(season.get("STL", 0) / games, 1)
                bpg = round(season.get("BLK", 0) / games, 1)
                
                # Calculate shooting percentages
                fg_pct = round(season.get("FG_PCT", 0) * 100, 1)
                fg3_pct = round(season.get("FG3_PCT", 0) * 100, 1)
                ft_pct = round(season.get("FT_PCT", 0) * 100, 1)
                
                # Format text
                text = f"In the {season_id} NBA season, {name} played for the {team_name} and averaged {ppg} points, {rpg} rebounds, {apg} assists, {spg} steals, and {bpg} blocks per game while shooting {fg_pct}% from the field, {fg3_pct}% from three-point range, and {ft_pct}% from the free throw line across {games} games."
                
                texts.append(text)
            
            return texts
        except Exception as e:
            logger.error(f"Error formatting season stats for player ID {player_id}: {e}")
            return [f"Season statistics for player with ID {player_id}."]
    
    def _format_team_basic_info(self, team_id: int) -> str:
        """
        Format basic team information as a natural language text.
        
        Args:
            team_id: NBA API team ID
            
        Returns:
            Formatted text about the team
        """
        try:
            # Load team data
            with open(f"{self.data_dir}/team_{team_id}.json", "r") as f:
                team_data = json.load(f)
            
            team = team_data["team"]
            details = team_data["details"]
            
            # Extract team info
            team_info = details.get("TeamBackground", [{}])[0] if "TeamBackground" in details else {}
            
            # Format text
            name = team.get("full_name", "")
            city = team.get("city", "")
            state = team.get("state", "")
            year_founded = team_info.get("YEARFOUNDED")
            arena = team_info.get("ARENA")
            arena_capacity = team_info.get("ARENACAPACITY")
            owner = team_info.get("OWNER")
            head_coach = team_info.get("HEADCOACH")
            
            # Format text
            text = f"The {name} are an American professional basketball team based in {city}"
            
            if state:
                text += f", {state}"
            
            text += ". The team competes in the National Basketball Association"
            
            if "TeamDetails" in details and details["TeamDetails"]:
                conference = details["TeamDetails"][0].get("CONFERENCE", "")
                division = details["TeamDetails"][0].get("DIVISION", "")
                
                if conference and division:
                    text += f" as a member of the league's {conference} Conference {division} Division"
            
            text += "."
            
            if arena:
                text += f" The {name} play their home games at {arena}"
                
                if arena_capacity:
                    text += f", which has a capacity of {arena_capacity}"
                
                text += "."
            
            if year_founded:
                text += f" The franchise was founded in {year_founded}."
            
            if owner:
                text += f" The team is owned by {owner}."
            
            if head_coach:
                text += f" The current head coach is {head_coach}."
            
            return text
        except Exception as e:
            logger.error(f"Error formatting team basic info for team ID {team_id}: {e}")
            return f"Information about team with ID {team_id}."
    
    def _format_team_history(self, team_id: int) -> str:
        """
        Format team history as a natural language text.
        
        Args:
            team_id: NBA API team ID
            
        Returns:
            Formatted text about the team's history
        """
        try:
            # Load team data
            with open(f"{self.data_dir}/team_{team_id}.json", "r") as f:
                team_data = json.load(f)
            
            team = team_data["team"]
            history = team_data["history"]
            
            # Get team name
            name = team.get("full_name", "")
            
            # Extract team history
            team_history = []
            if "TeamYearByYearStats" in history:
                team_history = history["TeamYearByYearStats"]
            
            if not team_history:
                return f"The {name} have no recorded history in the NBA."
            
            # Count championships
            championships = 0
            playoff_appearances = 0
            
            for season in team_history:
                if season.get("LEAGUE_CHAMPION") == "Y":
                    championships += 1
                
                if season.get("PLAYOFF_APPEARANCE") == "Y":
                    playoff_appearances += 1
            
            # Format text
            text = f"The {name} franchise history includes"
            
            if championships > 0:
                if championships == 1:
                    text += " 1 NBA championship"
                else:
                    text += f" {championships} NBA championships"
            
            if championships > 0 and playoff_appearances > 0:
                text += " and"
            
            if playoff_appearances > 0:
                if playoff_appearances == 1:
                    text += " 1 playoff appearance"
                else:
                    text += f" {playoff_appearances} playoff appearances"
            
            if championships == 0 and playoff_appearances == 0:
                text += " no NBA championships or playoff appearances"
            
            text += "."
            
            # Add notable seasons
            if team_history:
                # Sort by win percentage
                team_history.sort(key=lambda x: float(x.get("WIN_PCT", 0)), reverse=True)
                
                best_season = team_history[0]
                best_season_id = best_season.get("YEAR", "")
                best_season_wins = best_season.get("WINS", 0)
                best_season_losses = best_season.get("LOSSES", 0)
                
                text += f" The team's best regular season record was {best_season_wins}-{best_season_losses} during the {best_season_id} season."
            
            return text
        except Exception as e:
            logger.error(f"Error formatting team history for team ID {team_id}: {e}")
            return f"History of team with ID {team_id}."
    
    def _format_team_season_stats(self, team_id: int) -> List[str]:
        """
        Format team season statistics as natural language texts.
        
        Args:
            team_id: NBA API team ID
            
        Returns:
            List of formatted texts about the team's season statistics
        """
        try:
            # Load team data
            with open(f"{self.data_dir}/team_{team_id}.json", "r") as f:
                team_data = json.load(f)
            
            team = team_data["team"]
            history = team_data["history"]
            
            # Get team name
            name = team.get("full_name", "")
            
            # Extract team history
            team_history = []
            if "TeamYearByYearStats" in history:
                team_history = history["TeamYearByYearStats"]
            
            if not team_history:
                return [f"The {name} have no recorded season statistics in the NBA."]
            
            # Format text for each season (limit to last 10 seasons)
            team_history.sort(key=lambda x: x.get("YEAR", ""), reverse=True)
            recent_seasons = team_history[:10]
            
            texts = []
            
            for season in recent_seasons:
                season_id = season.get("YEAR", "")
                wins = season.get("WINS", 0)
                losses = season.get("LOSSES", 0)
                win_pct = round(float(season.get("WIN_PCT", 0)) * 100, 1)
                
                ppg = season.get("PTS", 0)
                opp_ppg = season.get("OPP_PTS", 0)
                
                # Format text
                text = f"In the {season_id} NBA season, the {name} finished with a record of {wins}-{losses} ({win_pct}% win percentage)"
                
                if ppg and opp_ppg:
                    text += f", averaging {ppg} points per game while allowing {opp_ppg} points per game"
                
                text += "."
                
                if season.get("PLAYOFF_APPEARANCE") == "Y":
                    text += " The team made the playoffs"
                    
                    if season.get("LEAGUE_CHAMPION") == "Y":
                        text += " and won the NBA championship."
                    else:
                        text += "."
                else:
                    text += " The team did not make the playoffs."
                
                texts.append(text)
            
            return texts
        except Exception as e:
            logger.error(f"Error formatting team season stats for team ID {team_id}: {e}")
            return [f"Season statistics for team with ID {team_id}."]
    
    def _format_league_standings(self, season: str = "2023-24") -> List[str]:
        """
        Format league standings as natural language texts.
        
        Args:
            season: NBA season in format "YYYY-YY"
            
        Returns:
            List of formatted texts about the league standings
        """
        try:
            # Load league data
            with open(f"{self.data_dir}/league_{season}.json", "r") as f:
                league_data = json.load(f)
            
            standings = league_data.get("standings", {})
            
            # Extract standings
            team_standings = []
            if "Standings" in standings:
                team_standings = standings["Standings"]
            
            if not team_standings:
                return [f"No standings data available for the {season} NBA season."]
            
            # Group by conference
            east_teams = [team for team in team_standings if team.get("CONFERENCE", "") == "East"]
            west_teams = [team for team in team_standings if team.get("CONFERENCE", "") == "West"]
            
            # Sort by conference rank
            east_teams.sort(key=lambda x: int(x.get("CONFERENCE_RANK", 99)))
            west_teams.sort(key=lambda x: int(x.get("CONFERENCE_RANK", 99)))
            
            # Format Eastern Conference text
            east_text = f"Eastern Conference Standings for the {season} NBA season:\n"
            
            for i, team in enumerate(east_teams[:8], 1):
                team_id = team.get("TEAM_ID")
                team_name = self.team_lookup.get(team_id, {}).get("full_name", "") if team_id else "N/A"
                wins = team.get("WINS", 0)
                losses = team.get("LOSSES", 0)
                
                east_text += f"{i}. {team_name}: {wins}-{losses}"
                
                if i < 8:
                    east_text += ", "
            
            # Format Western Conference text
            west_text = f"Western Conference Standings for the {season} NBA season:\n"
            
            for i, team in enumerate(west_teams[:8], 1):
                team_id = team.get("TEAM_ID")
                team_name = self.team_lookup.get(team_id, {}).get("full_name", "") if team_id else "N/A"
                wins = team.get("WINS", 0)
                losses = team.get("LOSSES", 0)
                
                west_text += f"{i}. {team_name}: {wins}-{losses}"
                
                if i < 8:
                    west_text += ", "
            
            # Format overall standings
            all_teams = team_standings.copy()
            all_teams.sort(key=lambda x: int(x.get("LEAGUE_RANK", 99)))
            
            overall_text = f"Overall NBA Standings for the {season} season:\n"
            
            for i, team in enumerate(all_teams[:10], 1):
                team_id = team.get("TEAM_ID")
                team_name = self.team_lookup.get(team_id, {}).get("full_name", "") if team_id else "N/A"
                wins = team.get("WINS", 0)
                losses = team.get("LOSSES", 0)
                
                overall_text += f"{i}. {team_name}: {wins}-{losses}"
                
                if i < 10:
                    overall_text += ", "
            
            return [east_text, west_text, overall_text]
        except Exception as e:
            logger.error(f"Error formatting league standings for season {season}: {e}")
            return [f"Standings for the {season} NBA season."]
    
    def _format_league_leaders(self, season: str = "2023-24") -> List[str]:
        """
        Format league leaders as natural language texts.
        
        Args:
            season: NBA season in format "YYYY-YY"
            
        Returns:
            List of formatted texts about the league leaders
        """
        try:
            # Load league data
            with open(f"{self.data_dir}/league_{season}.json", "r") as f:
                league_data = json.load(f)
            
            leaders = league_data.get("leaders", {})
            
            if not leaders:
                return [f"No league leaders data available for the {season} NBA season."]
            
            texts = []
            
            # Format scoring leaders
            if "PTS" in leaders:
                pts_leaders = leaders["PTS"].get("LeagueLeaders", [])
                
                if pts_leaders:
                    pts_text = f"NBA Scoring Leaders for the {season} season:\n"
                    
                    for i, player in enumerate(pts_leaders[:10], 1):
                        player_id = player.get("PLAYER_ID")
                        player_name = self.player_lookup.get(player_id, {}).get("full_name", "") if player_id else player.get("PLAYER", "N/A")
                        team_id = player.get("TEAM_ID")
                        team_name = self.team_lookup.get(team_id, {}).get("abbreviation", "") if team_id else "N/A"
                        ppg = round(player.get("PTS", 0), 1)
                        
                        pts_text += f"{i}. {player_name} ({team_name}): {ppg} PPG"
                        
                        if i < 10:
                            pts_text += ", "
                    
                    texts.append(pts_text)
            
            # Format rebounding leaders
            if "REB" in leaders:
                reb_leaders = leaders["REB"].get("LeagueLeaders", [])
                
                if reb_leaders:
                    reb_text = f"NBA Rebounding Leaders for the {season} season:\n"
                    
                    for i, player in enumerate(reb_leaders[:10], 1):
                        player_id = player.get("PLAYER_ID")
                        player_name = self.player_lookup.get(player_id, {}).get("full_name", "") if player_id else player.get("PLAYER", "N/A")
                        team_id = player.get("TEAM_ID")
                        team_name = self.team_lookup.get(team_id, {}).get("abbreviation", "") if team_id else "N/A"
                        rpg = round(player.get("REB", 0), 1)
                        
                        reb_text += f"{i}. {player_name} ({team_name}): {rpg} RPG"
                        
                        if i < 10:
                            reb_text += ", "
                    
                    texts.append(reb_text)
            
            # Format assist leaders
            if "AST" in leaders:
                ast_leaders = leaders["AST"].get("LeagueLeaders", [])
                
                if ast_leaders:
                    ast_text = f"NBA Assist Leaders for the {season} season:\n"
                    
                    for i, player in enumerate(ast_leaders[:10], 1):
                        player_id = player.get("PLAYER_ID")
                        player_name = self.player_lookup.get(player_id, {}).get("full_name", "") if player_id else player.get("PLAYER", "N/A")
                        team_id = player.get("TEAM_ID")
                        team_name = self.team_lookup.get(team_id, {}).get("abbreviation", "") if team_id else "N/A"
                        apg = round(player.get("AST", 0), 1)
                        
                        ast_text += f"{i}. {player_name} ({team_name}): {apg} APG"
                        
                        if i < 10:
                            ast_text += ", "
                    
                    texts.append(ast_text)
            
            return texts
        except Exception as e:
            logger.error(f"Error formatting league leaders for season {season}: {e}")
            return [f"Statistical leaders for the {season} NBA season."]
    
    def _format_game_summary(self, game_data: Dict[str, Any]) -> str:
        """
        Format game summary as a natural language text.
        
        Args:
            game_data: Game data dictionary
            
        Returns:
            Formatted text about the game
        """
        try:
            # Extract game header
            game_header = game_data.get("GameHeader", [{}])[0] if "GameHeader" in game_data else {}
            
            game_id = game_header.get("GAME_ID", "")
            game_date = game_header.get("GAME_DATE_EST", "")
            home_team_id = game_header.get("HOME_TEAM_ID")
            visitor_team_id = game_header.get("VISITOR_TEAM_ID")
            
            home_team_name = self.team_lookup.get(home_team_id, {}).get("full_name", "") if home_team_id else "Home Team"
            visitor_team_name = self.team_lookup.get(visitor_team_id, {}).get("full_name", "") if visitor_team_id else "Visiting Team"
            
            home_score = game_header.get("HOME_TEAM_SCORE", 0)
            visitor_score = game_header.get("VISITOR_TEAM_SCORE", 0)
            
            # Format date
            formatted_date = game_date
            try:
                date_obj = datetime.strptime(game_date, "%Y-%m-%d")
                formatted_date = date_obj.strftime("%B %d, %Y")
            except:
                pass
            
            # Determine winner
            if home_score > visitor_score:
                result = f"the {home_team_name} defeated the {visitor_team_name} {home_score}-{visitor_score}"
            elif visitor_score > home_score:
                result = f"the {visitor_team_name} defeated the {home_team_name} {visitor_score}-{home_score}"
            else:
                result = f"the {home_team_name} and the {visitor_team_name} tied {home_score}-{visitor_score}"
            
            # Format text
            text = f"On {formatted_date}, {result}."
            
            return text
        except Exception as e:
            logger.error(f"Error formatting game summary: {e}")
            return "NBA game summary."
    
    def _format_recent_games(self) -> List[str]:
        """
        Format recent games as natural language texts.
        
        Returns:
            List of formatted texts about recent games
        """
        try:
            # Load recent games data
            with open(f"{self.data_dir}/recent_games.json", "r") as f:
                recent_games_data = json.load(f)
            
            games = recent_games_data.get("games", {})
            
            # Extract game headers
            game_headers = []
            if "GameHeader" in games:
                game_headers = games["GameHeader"]
            
            if not game_headers:
                return ["No recent NBA games data available."]
            
            # Sort by date (most recent first)
            game_headers.sort(key=lambda x: x.get("GAME_DATE_EST", ""), reverse=True)
            
            # Format text for each game
            texts = []
            
            for game in game_headers:
                text = self._format_game_summary({"GameHeader": [game]})
                texts.append(text)
            
            return texts
        except Exception as e:
            logger.error(f"Error formatting recent games: {e}")
            return ["Recent NBA games."]
    
    def process_player_data(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Process player data into documents for the vector database.
        
        Args:
            limit: Optional limit on number of players to process
            
        Returns:
            List of processed documents
        """
        logger.info("Processing player data")
        
        documents = []
        
        # Get all player IDs
        player_files = [f for f in os.listdir(self.data_dir) if f.startswith("player_") and f.endswith(".json")]
        player_ids = [int(f.replace("player_", "").replace(".json", "")) for f in player_files]
        
        if limit:
            player_ids = player_ids[:limit]
        
        for player_id in tqdm(player_ids, desc="Processing players"):
            try:
                # Basic info document
                basic_info = self._format_player_basic_info(player_id)
                
                if basic_info:
                    player_name = self.player_lookup.get(player_id, {}).get("full_name", f"Player {player_id}")
                    
                    documents.append({
                        "text": basic_info,
                        "category": "player",
                        "entity_id": str(player_id),
                        "season": "career",
                        "metadata": {
                            "player_name": player_name,
                            "doc_type": "basic_info"
                        }
                    })
                
                # Career stats document
                career_stats = self._format_player_career_stats(player_id)
                
                if career_stats:
                    player_name = self.player_lookup.get(player_id, {}).get("full_name", f"Player {player_id}")
                    
                    documents.append({
                        "text": career_stats,
                        "category": "player_stats",
                        "entity_id": str(player_id),
                        "season": "career",
                        "metadata": {
                            "player_name": player_name,
                            "doc_type": "career_stats"
                        }
                    })
                
                # Season stats documents
                season_stats = self._format_player_season_stats(player_id)
                
                for i, stats in enumerate(season_stats):
                    player_name = self.player_lookup.get(player_id, {}).get("full_name", f"Player {player_id}")
                    
                    documents.append({
                        "text": stats,
                        "category": "player_stats",
                        "entity_id": str(player_id),
                        "season": f"season_{i}",
                        "metadata": {
                            "player_name": player_name,
                            "doc_type": "season_stats"
                        }
                    })
            except Exception as e:
                logger.error(f"Error processing player {player_id}: {e}")
        
        # Save processed documents
        with open(f"{self.output_dir}/processed_players.json", "w") as f:
            json.dump(documents, f)
        
        logger.info(f"Processed {len(documents)} player documents")
        return documents
    
    def process_team_data(self) -> List[Dict[str, Any]]:
        """
        Process team data into documents for the vector database.
        
        Returns:
            List of processed documents
        """
        logger.info("Processing team data")
        
        documents = []
        
        # Get all team IDs
        team_files = [f for f in os.listdir(self.data_dir) if f.startswith("team_") and f.endswith(".json")]
        team_ids = [int(f.replace("team_", "").replace(".json", "")) for f in team_files]
        
        for team_id in tqdm(team_ids, desc="Processing teams"):
            try:
                # Basic info document
                basic_info = self._format_team_basic_info(team_id)
                
                if basic_info:
                    team_name = self.team_lookup.get(team_id, {}).get("full_name", f"Team {team_id}")
                    
                    documents.append({
                        "text": basic_info,
                        "category": "team",
                        "entity_id": str(team_id),
                        "season": "all",
                        "metadata": {
                            "team_name": team_name,
                            "doc_type": "basic_info"
                        }
                    })
                
                # Team history document
                team_history = self._format_team_history(team_id)
                
                if team_history:
                    team_name = self.team_lookup.get(team_id, {}).get("full_name", f"Team {team_id}")
                    
                    documents.append({
                        "text": team_history,
                        "category": "team",
                        "entity_id": str(team_id),
                        "season": "all",
                        "metadata": {
                            "team_name": team_name,
                            "doc_type": "history"
                        }
                    })
                
                # Season stats documents
                season_stats = self._format_team_season_stats(team_id)
                
                for i, stats in enumerate(season_stats):
                    team_name = self.team_lookup.get(team_id, {}).get("full_name", f"Team {team_id}")
                    
                    documents.append({
                        "text": stats,
                        "category": "team_stats",
                        "entity_id": str(team_id),
                        "season": f"season_{i}",
                        "metadata": {
                            "team_name": team_name,
                            "doc_type": "season_stats"
                        }
                    })
            except Exception as e:
                logger.error(f"Error processing team {team_id}: {e}")
        
        # Save processed documents
        with open(f"{self.output_dir}/processed_teams.json", "w") as f:
            json.dump(documents, f)
        
        logger.info(f"Processed {len(documents)} team documents")
        return documents
    
    def process_league_data(self, seasons: List[str] = ["2023-24"]) -> List[Dict[str, Any]]:
        """
        Process league data into documents for the vector database.
        
        Args:
            seasons: List of NBA seasons in format "YYYY-YY"
            
        Returns:
            List of processed documents
        """
        logger.info("Processing league data")
        
        documents = []
        
        for season in tqdm(seasons, desc="Processing seasons"):
            try:
                # Standings documents
                standings = self._format_league_standings(season)
                
                for i, text in enumerate(standings):
                    documents.append({
                        "text": text,
                        "category": "league",
                        "entity_id": f"standings_{i}",
                        "season": season,
                        "metadata": {
                            "doc_type": "standings",
                            "part": i + 1,
                            "total_parts": len(standings)
                        }
                    })
                
                # Leaders documents
                leaders = self._format_league_leaders(season)
                
                for i, text in enumerate(leaders):
                    documents.append({
                        "text": text,
                        "category": "league",
                        "entity_id": f"leaders_{i}",
                        "season": season,
                        "metadata": {
                            "doc_type": "leaders",
                            "part": i + 1,
                            "total_parts": len(leaders)
                        }
                    })
            except Exception as e:
                logger.error(f"Error processing league data for season {season}: {e}")
        
        # Save processed documents
        with open(f"{self.output_dir}/processed_league.json", "w") as f:
            json.dump(documents, f)
        
        logger.info(f"Processed {len(documents)} league documents")
        return documents
    
    def process_game_data(self) -> List[Dict[str, Any]]:
        """
        Process game data into documents for the vector database.
        
        Returns:
            List of processed documents
        """
        logger.info("Processing game data")
        
        documents = []
        
        try:
            # Process recent games
            recent_games = self._format_recent_games()
            
            for i, text in enumerate(recent_games):
                documents.append({
                    "text": text,
                    "category": "game",
                    "entity_id": f"recent_{i}",
                    "season": "current",
                    "metadata": {
                        "doc_type": "game_summary"
                    }
                })
        except Exception as e:
            logger.error(f"Error processing game data: {e}")
        
        # Save processed documents
        with open(f"{self.output_dir}/processed_games.json", "w") as f:
            json.dump(documents, f)
        
        logger.info(f"Processed {len(documents)} game documents")
        return documents
    
    def process_all_data(self, player_limit: Optional[int] = None, seasons: List[str] = ["2023-24"]) -> List[Dict[str, Any]]:
        """
        Process all NBA data into documents for the vector database.
        
        Args:
            player_limit: Optional limit on number of players to process
            seasons: List of NBA seasons in format "YYYY-YY"
            
        Returns:
            List of all processed documents
        """
        logger.info("Processing all NBA data")
        
        all_documents = []
        
        # Process player data
        player_docs = self.process_player_data(limit=player_limit)
        all_documents.extend(player_docs)
        
        # Process team data
        team_docs = self.process_team_data()
        all_documents.extend(team_docs)
        
        # Process league data
        league_docs = self.process_league_data(seasons=seasons)
        all_documents.extend(league_docs)
        
        # Process game data
        game_docs = self.process_game_data()
        all_documents.extend(game_docs)
        
        # Add timestamps
        now = datetime.now().isoformat()
        
        for doc in all_documents:
            doc["created_at"] = now
            doc["updated_at"] = now
        
        # Save all processed documents
        with open(f"{self.output_dir}/all_processed_data.json", "w") as f:
            json.dump(all_documents, f)
        
        logger.info(f"Processed {len(all_documents)} total documents")
        return all_documents


if __name__ == "__main__":
    # Create processor
    processor = NBADataProcessor()
    
    # Process all data with limited player data for testing
    processor.process_all_data(player_limit=10)
