#!/usr/bin/env python3
"""
NBA Data Collector for RAG-LLM Chatbot

This script collects NBA data from various sources and prepares it for
insertion into a MongoDB Atlas vector database.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

import pandas as pd
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv

# NBA API imports
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    playercareerstats, 
    commonplayerinfo, 
    leagueleaders, 
    leaguestandings,
    teamdetails,
    teamyearbyyearstats,
    scoreboard,
    boxscoretraditionalv2,
    playergamelog,
    teamgamelog
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_data_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NBADataCollector:
    """
    Collects NBA data from various sources and prepares it for
    insertion into a MongoDB Atlas vector database.
    """
    
    def __init__(self, output_dir: str = "nba_data"):
        """
        Initialize the NBA data collector.
        
        Args:
            output_dir: Directory to save collected data
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Cache for API responses to avoid duplicate requests
        self.cache = {}
        
        logger.info("NBA Data Collector initialized")
    
    def _get_with_cache(self, cache_key: str, fetch_func, *args, **kwargs):
        """
        Get data from cache or fetch it using the provided function.
        
        Args:
            cache_key: Key to store/retrieve data in cache
            fetch_func: Function to fetch data if not in cache
            *args, **kwargs: Arguments to pass to fetch_func
            
        Returns:
            The fetched or cached data
        """
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Add delay to avoid rate limiting
        time.sleep(0.6)
        
        try:
            data = fetch_func(*args, **kwargs)
            self.cache[cache_key] = data
            return data
        except Exception as e:
            logger.error(f"Error fetching data for {cache_key}: {e}")
            raise
    
    def collect_all_players(self) -> List[Dict[str, Any]]:
        """
        Collect information about all NBA players.
        
        Returns:
            List of player dictionaries
        """
        logger.info("Collecting all NBA players")
        all_players = players.get_players()
        
        # Save to file
        with open(f"{self.output_dir}/all_players.json", "w") as f:
            json.dump(all_players, f)
        
        logger.info(f"Collected {len(all_players)} players")
        return all_players
    
    def collect_all_teams(self) -> List[Dict[str, Any]]:
        """
        Collect information about all NBA teams.
        
        Returns:
            List of team dictionaries
        """
        logger.info("Collecting all NBA teams")
        all_teams = teams.get_teams()
        
        # Save to file
        with open(f"{self.output_dir}/all_teams.json", "w") as f:
            json.dump(all_teams, f)
        
        logger.info(f"Collected {len(all_teams)} teams")
        return all_teams
    
    def collect_player_info(self, player_id: int) -> Dict[str, Any]:
        """
        Collect detailed information about a specific player.
        
        Args:
            player_id: NBA API player ID
            
        Returns:
            Dictionary with player information
        """
        cache_key = f"player_info_{player_id}"
        
        def fetch_player_info():
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            return player_info.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_player_info)
    
    def collect_player_career_stats(self, player_id: int) -> Dict[str, Any]:
        """
        Collect career statistics for a specific player.
        
        Args:
            player_id: NBA API player ID
            
        Returns:
            Dictionary with player career statistics
        """
        cache_key = f"player_career_stats_{player_id}"
        
        def fetch_career_stats():
            career_stats = playercareerstats.PlayerCareerStats(player_id=player_id)
            return career_stats.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_career_stats)
    
    def collect_team_details(self, team_id: int) -> Dict[str, Any]:
        """
        Collect detailed information about a specific team.
        
        Args:
            team_id: NBA API team ID
            
        Returns:
            Dictionary with team information
        """
        cache_key = f"team_details_{team_id}"
        
        def fetch_team_details():
            team_details = teamdetails.TeamDetails(team_id=team_id)
            return team_details.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_team_details)
    
    def collect_team_history(self, team_id: int) -> Dict[str, Any]:
        """
        Collect historical statistics for a specific team.
        
        Args:
            team_id: NBA API team ID
            
        Returns:
            Dictionary with team historical statistics
        """
        cache_key = f"team_history_{team_id}"
        
        def fetch_team_history():
            team_history = teamyearbyyearstats.TeamYearByYearStats(team_id=team_id)
            return team_history.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_team_history)
    
    def collect_league_leaders(self, season: str = "2023-24", stat_category: str = "PTS") -> Dict[str, Any]:
        """
        Collect league leaders for a specific statistical category.
        
        Args:
            season: NBA season in format "YYYY-YY"
            stat_category: Statistical category (PTS, REB, AST, etc.)
            
        Returns:
            Dictionary with league leaders
        """
        cache_key = f"league_leaders_{season}_{stat_category}"
        
        def fetch_league_leaders():
            leaders = leagueleaders.LeagueLeaders(
                season=season,
                stat_category_abbreviation=stat_category,
                season_type_all_star="Regular Season"
            )
            return leaders.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_league_leaders)
    
    def collect_standings(self, season: str = "2023-24") -> Dict[str, Any]:
        """
        Collect league standings for a specific season.
        
        Args:
            season: NBA season in format "YYYY-YY"
            
        Returns:
            Dictionary with league standings
        """
        cache_key = f"standings_{season}"
        
        def fetch_standings():
            standings = leaguestandings.LeagueStandings(season=season)
            return standings.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_standings)
    
    def collect_recent_games(self, days: int = 7) -> Dict[str, Any]:
        """
        Collect information about recent games.
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with recent games
        """
        cache_key = f"recent_games_{days}"
        
        def fetch_recent_games():
            games = scoreboard.Scoreboard(day_offset=days, league_id="00")
            return games.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_recent_games)
    
    def collect_game_details(self, game_id: str) -> Dict[str, Any]:
        """
        Collect detailed information about a specific game.
        
        Args:
            game_id: NBA API game ID
            
        Returns:
            Dictionary with game details
        """
        cache_key = f"game_details_{game_id}"
        
        def fetch_game_details():
            game_details = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            return game_details.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_game_details)
    
    def collect_player_game_log(self, player_id: int, season: str = "2023-24") -> Dict[str, Any]:
        """
        Collect game log for a specific player.
        
        Args:
            player_id: NBA API player ID
            season: NBA season in format "YYYY-YY"
            
        Returns:
            Dictionary with player game log
        """
        cache_key = f"player_game_log_{player_id}_{season}"
        
        def fetch_player_game_log():
            game_log = playergamelog.PlayerGameLog(
                player_id=player_id,
                season=season,
                season_type_all_star="Regular Season"
            )
            return game_log.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_player_game_log)
    
    def collect_team_game_log(self, team_id: int, season: str = "2023-24") -> Dict[str, Any]:
        """
        Collect game log for a specific team.
        
        Args:
            team_id: NBA API team ID
            season: NBA season in format "YYYY-YY"
            
        Returns:
            Dictionary with team game log
        """
        cache_key = f"team_game_log_{team_id}_{season}"
        
        def fetch_team_game_log():
            game_log = teamgamelog.TeamGameLog(
                team_id=team_id,
                season=season,
                season_type_all_star="Regular Season"
            )
            return game_log.get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_team_game_log)
    
    def collect_data_for_all_players(self, limit: Optional[int] = None) -> None:
        """
        Collect data for all NBA players.
        
        Args:
            limit: Optional limit on number of players to process
        """
        all_players = self.collect_all_players()
        
        if limit:
            all_players = all_players[:limit]
        
        for player in tqdm(all_players, desc="Collecting player data"):
            player_id = player["id"]
            try:
                # Collect player info
                player_info = self.collect_player_info(player_id)
                
                # Collect career stats
                career_stats = self.collect_player_career_stats(player_id)
                
                # Save to file
                with open(f"{self.output_dir}/player_{player_id}.json", "w") as f:
                    json.dump({
                        "player": player,
                        "info": player_info,
                        "career_stats": career_stats
                    }, f)
                
                logger.info(f"Collected data for player {player['full_name']} (ID: {player_id})")
            except Exception as e:
                logger.error(f"Error collecting data for player {player['full_name']} (ID: {player_id}): {e}")
    
    def collect_data_for_all_teams(self) -> None:
        """
        Collect data for all NBA teams.
        """
        all_teams = self.collect_all_teams()
        
        for team in tqdm(all_teams, desc="Collecting team data"):
            team_id = team["id"]
            try:
                # Collect team details
                team_details = self.collect_team_details(team_id)
                
                # Collect team history
                team_history = self.collect_team_history(team_id)
                
                # Save to file
                with open(f"{self.output_dir}/team_{team_id}.json", "w") as f:
                    json.dump({
                        "team": team,
                        "details": team_details,
                        "history": team_history
                    }, f)
                
                logger.info(f"Collected data for team {team['full_name']} (ID: {team_id})")
            except Exception as e:
                logger.error(f"Error collecting data for team {team['full_name']} (ID: {team_id}): {e}")
    
    def collect_league_data(self, seasons: List[str] = ["2023-24"]) -> None:
        """
        Collect league-wide data for specified seasons.
        
        Args:
            seasons: List of NBA seasons in format "YYYY-YY"
        """
        for season in tqdm(seasons, desc="Collecting league data"):
            try:
                # Collect standings
                standings = self.collect_standings(season)
                
                # Collect league leaders for various stat categories
                stat_categories = ["PTS", "REB", "AST", "STL", "BLK", "FG_PCT", "FT_PCT", "FG3_PCT"]
                leaders = {}
                
                for stat in stat_categories:
                    leaders[stat] = self.collect_league_leaders(season, stat)
                
                # Save to file
                with open(f"{self.output_dir}/league_{season}.json", "w") as f:
                    json.dump({
                        "season": season,
                        "standings": standings,
                        "leaders": leaders
                    }, f)
                
                logger.info(f"Collected league data for season {season}")
            except Exception as e:
                logger.error(f"Error collecting league data for season {season}: {e}")
    
    def collect_recent_game_data(self, days_back: int = 30) -> None:
        """
        Collect data for recent games.
        
        Args:
            days_back: Number of days to look back
        """
        try:
            # Collect recent games
            recent_games = self.collect_recent_games(days_back)
            
            # Extract game IDs
            games = recent_games.get("GameHeader", [])
            game_ids = [game.get("GAME_ID") for game in games if game.get("GAME_ID")]
            
            # Collect details for each game
            game_details = {}
            
            for game_id in tqdm(game_ids, desc="Collecting game details"):
                try:
                    game_details[game_id] = self.collect_game_details(game_id)
                except Exception as e:
                    logger.error(f"Error collecting details for game {game_id}: {e}")
            
            # Save to file
            with open(f"{self.output_dir}/recent_games.json", "w") as f:
                json.dump({
                    "games": recent_games,
                    "details": game_details
                }, f)
            
            logger.info(f"Collected data for {len(game_ids)} recent games")
        except Exception as e:
            logger.error(f"Error collecting recent game data: {e}")
    
    def run_collection(self, 
                      collect_players: bool = True, 
                      collect_teams: bool = True,
                      collect_league: bool = True,
                      collect_games: bool = True,
                      player_limit: Optional[int] = None,
                      seasons: List[str] = ["2023-24"]) -> None:
        """
        Run the data collection process.
        
        Args:
            collect_players: Whether to collect player data
            collect_teams: Whether to collect team data
            collect_league: Whether to collect league data
            collect_games: Whether to collect game data
            player_limit: Optional limit on number of players to process
            seasons: List of NBA seasons to collect data for
        """
        start_time = time.time()
        logger.info("Starting NBA data collection")
        
        if collect_players:
            self.collect_data_for_all_players(limit=player_limit)
        
        if collect_teams:
            self.collect_data_for_all_teams()
        
        if collect_league:
            self.collect_league_data(seasons=seasons)
        
        if collect_games:
            self.collect_recent_game_data()
        
        elapsed_time = time.time() - start_time
        logger.info(f"NBA data collection completed in {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    # Create collector
    collector = NBADataCollector()
    
    # Run collection with limited player data for testing
    collector.run_collection(player_limit=10)
