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
from typing import Dict, List, Any, Optional
from tqdm import tqdm
from dotenv import load_dotenv

from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    playercareerstats, 
    commonplayerinfo, 
    leagueleaders, 
    leaguestandings,
    teamdetails,
    teamyearbyyearstats,
    ScoreboardV2,
    boxscoretraditionalv2,
    playergamelog,
    teamgamelog
)

import concurrent.futures
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_data_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

import requests
from functools import wraps

def call_with_retries(api_func, max_retries=5, initial_delay=2, timeout=60, *args, **kwargs):
    """
    Call an API function with retries and exponential backoff on timeout/network errors.
    """
    delay = initial_delay
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            # Always pass timeout if supported
            if 'timeout' in api_func.__code__.co_varnames:
                result = api_func(*args, timeout=timeout, **kwargs)
            else:
                result = api_func(*args, **kwargs)
            return result
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"[Attempt {attempt}/{max_retries}] Timeout or connection error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2
            last_exc = e
        except Exception as e:
            logger.warning(f"[Attempt {attempt}/{max_retries}] General error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2
            last_exc = e
    logger.error(f"API call failed after {max_retries} attempts: {last_exc}")
    raise last_exc

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
            return call_with_retries(lambda: commonplayerinfo.CommonPlayerInfo(player_id=player_id), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: playercareerstats.PlayerCareerStats(player_id=player_id), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: teamdetails.TeamDetails(team_id=team_id), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: teamyearbyyearstats.TeamYearByYearStats(team_id=team_id), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: leagueleaders.LeagueLeaders(
                season=season,
                stat_category_abbreviation=stat_category,
                season_type_all_star="Regular Season"
            ), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: leaguestandings.LeagueStandings(season=season), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: ScoreboardV2(day_offset=days, league_id="00"), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: playergamelog.PlayerGameLog(
                player_id=player_id,
                season=season,
                season_type_all_star="Regular Season"
            ), timeout=60).get_normalized_dict()
        
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
            return call_with_retries(lambda: teamgamelog.TeamGameLog(
                team_id=team_id,
                season=season,
                season_type_all_star="Regular Season"
            ), timeout=60).get_normalized_dict()
        
        return self._get_with_cache(cache_key, fetch_team_game_log)
    
    def _already_collected_ids(self, prefix: str) -> set:
        """
        Return a set of player or team IDs for which data files already exist.
        """
        ids = set()
        if not os.path.exists(self.output_dir):
            return ids
        for fname in os.listdir(self.output_dir):
            if fname.startswith(prefix) and fname.endswith('.json'):
                try:
                    id_part = fname[len(prefix):-5]
                    ids.add(int(id_part))
                except Exception:
                    continue
        return ids

    def collect_data_for_all_players(self, limit: Optional[int] = None, max_workers: int = 2, timeout_tracker=None) -> None:
        """
        Collect data for all NBA players concurrently, skipping already collected ones.
        Args:
            limit: Optional limit on number of players to process
            max_workers: Number of concurrent threads
            timeout_tracker: dict for tracking timeouts/errors
        """
        all_players = self.collect_all_players()
        if limit:
            all_players = all_players[:limit]
        already_collected = self._already_collected_ids('player_')
        players_to_process = [p for p in all_players if p['id'] not in already_collected]
        if not players_to_process:
            logger.info("All player data already collected. Skipping.")
            return

        def process_player(player):
            player_id = player["id"]
            try:
                time.sleep(random.uniform(1.0, 2.5))
                player_info = self.collect_player_info(player_id)
                career_stats = self.collect_player_career_stats(player_id)
                with open(f"{self.output_dir}/player_{player_id}.json", "w") as f:
                    json.dump({
                        "player": player,
                        "info": player_info,
                        "career_stats": career_stats
                    }, f)
                logger.info(f"Collected data for player {player['full_name']} (ID: {player_id})")
            except Exception as e:
                logger.error(f"Error collecting data for player {player['full_name']} (ID: {player_id}): {e}")
                if timeout_tracker is not None:
                    timeout_tracker['count'] += 1

        timeout_tracker = {'count': 0}
        max_timeouts = 10
        timeout_window = 30  # Check after every 30 players
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i, _ in enumerate(tqdm(executor.map(process_player, players_to_process), total=len(players_to_process), desc="Collecting player data (concurrent)")):
                if (i + 1) % timeout_window == 0:
                    if timeout_tracker['count'] >= max_timeouts:
                        logger.error(f"Too many timeouts/errors ({timeout_tracker['count']}) in last {timeout_window} players. Aborting collection and saving progress.")
                        break
                    timeout_tracker['count'] = 0  # Reset for next window

    def collect_data_for_all_teams(self, max_workers: int = 2, timeout_tracker=None) -> None:
        """
        Collect data for all NBA teams concurrently, skipping already collected ones.
        Args:
            max_workers: Number of concurrent threads
            timeout_tracker: dict for tracking timeouts/errors
        """
        all_teams = self.collect_all_teams()
        already_collected = self._already_collected_ids('team_')
        teams_to_process = [t for t in all_teams if t['id'] not in already_collected]
        if not teams_to_process:
            logger.info("All team data already collected. Skipping.")
            return
        def process_team(team):
            team_id = team["id"]
            try:
                time.sleep(random.uniform(1.0, 2.5))
                team_details = self.collect_team_details(team_id)
                team_history = self.collect_team_history(team_id)
                with open(f"{self.output_dir}/team_{team_id}.json", "w") as f:
                    json.dump({
                        "team": team,
                        "details": team_details,
                        "history": team_history
                    }, f)
                logger.info(f"Collected data for team {team['full_name']} (ID: {team_id})")
            except Exception as e:
                logger.error(f"Error collecting data for team {team['full_name']} (ID: {team_id}): {e}")
                if timeout_tracker is not None:
                    timeout_tracker['count'] += 1
        timeout_tracker = {'count': 0}
        max_timeouts = 5
        timeout_window = 10
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i, _ in enumerate(tqdm(executor.map(process_team, teams_to_process), total=len(teams_to_process), desc="Collecting team data (concurrent)")):
                if (i + 1) % timeout_window == 0:
                    if timeout_tracker['count'] >= max_timeouts:
                        logger.error(f"Too many timeouts/errors ({timeout_tracker['count']}) in last {timeout_window} teams. Aborting collection and saving progress.")
                        break
                    timeout_tracker['count'] = 0

    def collect_league_data(self, seasons: List[str] = ["2023-24"]) -> None:
        """
        Collect league-wide data for specified seasons.
        
        Args:
            seasons: List of NBA seasons in format "YYYY-YY"
        """
        for season in tqdm(seasons, desc="Collecting league data"):
            try:
                standings = self.collect_standings(season)
                
                stat_categories = ["PTS", "REB", "AST", "STL", "BLK", "FG_PCT", "FT_PCT", "FG3_PCT"]
                leaders = {}
                
                for stat in stat_categories:
                    leaders[stat] = self.collect_league_leaders(season, stat)
                
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
            recent_games = self.collect_recent_games(days_back)
            
            games = recent_games.get("GameHeader", [])
            game_ids = [game.get("GAME_ID") for game in games if game.get("GAME_ID")]
            
            game_details = {}
            
            for game_id in tqdm(game_ids, desc="Collecting game details"):
                try:
                    game_details[game_id] = self.collect_game_details(game_id)
                except Exception as e:
                    logger.error(f"Error collecting details for game {game_id}: {e}")
            
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
                      seasons: List[str] = ["2023-24"],
                      max_workers: int = 2) -> None:
        """
        Run the data collection process with checkpointing and error handling.
        """
        start_time = time.time()
        logger.info("Starting NBA data collection")
        if collect_players:
            self.collect_data_for_all_players(limit=player_limit, max_workers=max_workers)
        if collect_teams:
            self.collect_data_for_all_teams(max_workers=max_workers)
        if collect_league:
            self.collect_league_data(seasons=seasons)
        if collect_games:
            self.collect_recent_game_data()
        elapsed_time = time.time() - start_time
        logger.info(f"NBA data collection completed in {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    collector = NBADataCollector()
    collector.run_collection(player_limit=None, max_workers=2)
