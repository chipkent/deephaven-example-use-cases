#!/usr/bin/env python3
"""
Generic Replay Orchestrator for Deephaven Enterprise

This script orchestrates the execution of replay persistent queries across multiple dates and workers.
It creates and manages Deephaven Enterprise replay sessions based on a configuration file.

Usage:
    python replay_orchestrator.py --config simple_worker/config.yaml
"""

import argparse
import json
import logging
import os
import re
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Deque
import yaml

from deephaven_enterprise.client.generate_scheduling import GenerateScheduling
from deephaven_enterprise.client.session_manager import SessionManager
from deephaven_enterprise.proto.persistent_query_pb2 import PersistentQueryConfigMessage


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Default to INFO, can be changed to DEBUG with --verbose


# Exit codes
EXIT_SUCCESS = 0
EXIT_CREATION_FAILURES = 1
EXIT_EXECUTION_FAILURES = 2
EXIT_BOTH_FAILURES = 3
EXIT_ERROR = 4

# Configuration constants
DEFAULT_MAX_RETRIES = 3
DEFAULT_STATUS_TIMEOUT_SECONDS = 5
DEFAULT_RETRY_DELAY_SECONDS = 1
MAX_IDLE_ITERATIONS = 10  # Max iterations with no progress before warning
MAX_IDLE_ITERATIONS_STARTUP = 30  # Higher threshold during startup phase

# Persistent Query Configuration constants
PQ_SERIAL_NEW = -2**63  # Sentinel value for new PQ (not yet created)
DEFAULT_BUFFER_POOL_RATIO = 0.25  # Ratio of buffer pool to heap size
DEFAULT_RESTART_USERS = 1  # Number of concurrent users for restart


class ReplayOrchestrator:
    """Orchestrates replay persistent query execution across dates and workers."""
    
    def __init__(self, config_path: str, dry_run: bool = False):
        """Initialize orchestrator with configuration file.
        
        Args:
            config_path: Path to YAML configuration file
            dry_run: If True, validate config but don't create sessions
        """
        self.config_path = Path(config_path).resolve()
        self.dry_run = dry_run
        self.config: Dict[str, Any] = self._load_config()
        self.session_mgr: Optional[SessionManager] = None
        self.sessions: Dict[Tuple[str, int], int] = {}  # (date, worker_id) -> serial
        self.failed_sessions: List[Tuple[str, int]] = []
        self.retry_counts: Dict[Tuple[str, int], int] = {}
        self.worker_script_content: Optional[str] = None
        self.shutdown_requested = False
        self._cleanup_done = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        
    def _load_config(self) -> Dict:
        """Load and validate configuration file."""
        logger.info(f"Loading configuration from {self.config_path}")
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self._validate_config(config)
        self._expand_env_vars(config)
        
        return config
    
    def _validate_config(self, config: Dict):
        """Validate required configuration fields and reject unexpected ones."""
        self._validate_config_structure(config)
        self._validate_deephaven_config(config['deephaven'])
        self._validate_execution_config(config['execution'])
        self._validate_replay_config(config['replay'])
        self._validate_dates_config(config['dates'])
    
    def _validate_config_structure(self, config: Dict):
        """Validate top-level config structure, sections, and field presence."""
        # Define all expected sections
        expected_sections = {'name', 'deephaven', 'execution', 'replay', 'dates', 'env'}
        
        # Check for unexpected sections
        actual_sections = set(config.keys())
        unexpected_sections = actual_sections - expected_sections
        if unexpected_sections:
            raise ValueError(f"Unexpected config sections: {', '.join(sorted(unexpected_sections))}")
        
        # Check required top-level fields
        if 'name' not in config:
            raise ValueError("Missing required field: name")
        if not isinstance(config['name'], str) or not config['name'].strip():
            raise ValueError("name must be a non-empty string")
        
        # Check required sections exist
        required_sections = ['deephaven', 'execution', 'replay', 'dates', 'env']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")
        
        # Validate env section type
        if not isinstance(config['env'], dict):
            raise ValueError("env section must be a dictionary")
        
        # Define required and optional fields for each section
        required_fields = {
            'deephaven': ['connection_url', 'auth_method', 'username'],
            'execution': ['worker_script', 'num_workers', 'max_concurrent_sessions'],
            'replay': ['heap_size_gb', 'replay_start', 'replay_speed', 'script_language'],
            'dates': ['start', 'end']
        }
        
        allowed_fields = {
            'deephaven': {'connection_url', 'auth_method', 'username', 'password', 'private_key_path'},
            'execution': {'worker_script', 'num_workers', 'max_concurrent_sessions', 'max_retries', 'delete_successful_queries', 'delete_failed_queries'},
            'replay': {
                'heap_size_gb', 'init_timeout_minutes',
                'replay_start', 'replay_speed', 'sorted_replay',
                'buffer_rows', 'replay_timestamp_columns',
                'script_language', 'jvm_profile', 'server_name'
            },
            'dates': {'start', 'end', 'weekdays_only'},
            'env': None  # env section can have arbitrary user-defined variables
        }
        
        # Validate each section has required fields and no unexpected fields
        for section, fields in required_fields.items():
            # Check required fields exist
            for field in fields:
                if field not in config[section]:
                    raise ValueError(f"Missing required field: {section}.{field}")
            
            # Check for unexpected fields (except env which is user-defined)
            if allowed_fields[section] is not None:
                actual_fields = set(config[section].keys())
                unexpected_fields = actual_fields - allowed_fields[section]
                if unexpected_fields:
                    raise ValueError(
                        f"Unexpected fields in {section}: {', '.join(sorted(unexpected_fields))}. "
                        f"Allowed fields: {', '.join(sorted(allowed_fields[section]))}"
                    )
    
    def _validate_deephaven_config(self, dh_config: Dict):
        """Validate Deephaven connection and authentication configuration."""
        # Validate required string fields are non-empty
        connection_url = dh_config['connection_url']
        if not isinstance(connection_url, str) or not connection_url.strip():
            raise ValueError("connection_url must be a non-empty string")
        
        username = dh_config['username']
        if not isinstance(username, str) or not username.strip():
            raise ValueError("username must be a non-empty string")
        
        # Validate auth_method (required field)
        auth_method = dh_config['auth_method']
        if not isinstance(auth_method, str):
            raise ValueError(f"auth_method must be a string (got {type(auth_method).__name__})")
        if auth_method not in ['password', 'private_key']:
            raise ValueError(f"auth_method must be 'password' or 'private_key' (got '{auth_method}')")
        
        # Validate auth credentials based on method
        if auth_method == 'password':
            if 'password' not in dh_config or not dh_config['password']:
                raise ValueError("password is required when auth_method is 'password'")
            password = dh_config['password']
            if not isinstance(password, str):
                raise ValueError(f"password must be a string (got {type(password).__name__})")
        elif auth_method == 'private_key':
            if 'private_key_path' not in dh_config or not dh_config['private_key_path']:
                raise ValueError("private_key_path is required when auth_method is 'private_key'")
            private_key_path = dh_config['private_key_path']
            if not isinstance(private_key_path, str) or not private_key_path.strip():
                raise ValueError("private_key_path must be a non-empty string")
    
    def _validate_execution_config(self, exec_config: Dict):
        """Validate execution configuration (workers, script, concurrency)."""
        # Validate worker script path
        worker_script = exec_config['worker_script']
        if not isinstance(worker_script, str) or not worker_script.strip():
            raise ValueError("worker_script must be a non-empty string")
        
        # Validate num_workers (required)
        workers = exec_config['num_workers']
        if not isinstance(workers, int):
            raise ValueError(f"num_workers must be an integer (got {type(workers).__name__})")
        if workers <= 0:
            raise ValueError(f"num_workers must be > 0 (got {workers})")
        if workers > 1000:
            raise ValueError(f"num_workers too high (got {workers}, max 1000)")
        
        # Validate optional numeric fields
        if 'max_concurrent_sessions' in exec_config:
            concurrent = exec_config['max_concurrent_sessions']
            if not isinstance(concurrent, int):
                raise ValueError(f"max_concurrent_sessions must be an integer (got {type(concurrent).__name__})")
            if concurrent <= 0:
                raise ValueError(f"max_concurrent_sessions must be > 0 (got {concurrent})")
            if concurrent > 1000:
                raise ValueError(f"max_concurrent_sessions too high (got {concurrent}, max 1000)")
        
        if 'max_retries' in exec_config:
            retries = exec_config['max_retries']
            if not isinstance(retries, int):
                raise ValueError(f"max_retries must be an integer (got {type(retries).__name__})")
            if retries < 0:
                raise ValueError(f"max_retries must be >= 0 (got {retries})")
        
        # Validate optional boolean fields
        if 'delete_successful_queries' in exec_config:
            delete_successful = exec_config['delete_successful_queries']
            if not isinstance(delete_successful, bool):
                raise ValueError(f"delete_successful_queries must be a boolean (got {type(delete_successful).__name__})")
        
        if 'delete_failed_queries' in exec_config:
            delete_failed = exec_config['delete_failed_queries']
            if not isinstance(delete_failed, bool):
                raise ValueError(f"delete_failed_queries must be a boolean (got {type(delete_failed).__name__})")
    
    def _validate_replay_config(self, replay_config: Dict):
        """Validate replay configuration (heap, speed, replay settings)."""
        # Validate heap_size_gb (required)
        heap = replay_config['heap_size_gb']
        if not isinstance(heap, (int, float)):
            raise ValueError(f"heap_size_gb must be a number (got {type(heap).__name__})")
        if heap <= 0:
            raise ValueError(f"heap_size_gb must be > 0 (got {heap})")
        if heap > 512:
            raise ValueError(f"heap_size_gb too high (got {heap}, max 512)")
        
        # Validate replay_speed (required)
        speed = replay_config['replay_speed']
        if not isinstance(speed, (int, float)):
            raise ValueError(f"replay_speed must be a number (got {type(speed).__name__})")
        if speed < 1.0:
            raise ValueError(f"replay_speed must be >= 1.0 for backtesting (got {speed})")
        if speed > 100.0:
            raise ValueError(f"replay_speed too high (got {speed}, max 100).")
        
        # Validate optional timeout
        if 'init_timeout_minutes' in replay_config:
            timeout = replay_config['init_timeout_minutes']
            if not isinstance(timeout, (int, float)):
                raise ValueError(f"init_timeout_minutes must be a number (got {type(timeout).__name__})")
            if timeout <= 0:
                raise ValueError(f"init_timeout_minutes must be > 0 (got {timeout})")
        
        # Validate optional buffer_rows
        if 'buffer_rows' in replay_config:
            buffer = replay_config['buffer_rows']
            if not isinstance(buffer, int):
                raise ValueError(f"buffer_rows must be an integer (got {type(buffer).__name__})")
            if buffer <= 0:
                raise ValueError(f"buffer_rows must be > 0 (got {buffer})")
        
        # Validate replay_start format (HH:MM:SS, required)
        replay_start = replay_config['replay_start']
        if not isinstance(replay_start, str):
            raise ValueError(f"replay_start must be a string (got {type(replay_start).__name__})")
        if not re.match(r'^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$', replay_start):
            raise ValueError(f"replay_start must be in HH:MM:SS format (got '{replay_start}')")
        
        # Validate sorted_replay (optional boolean)
        if 'sorted_replay' in replay_config:
            sorted_replay = replay_config['sorted_replay']
            if not isinstance(sorted_replay, bool):
                raise ValueError(f"sorted_replay must be a boolean (got {type(sorted_replay).__name__})")
        
        # Validate script_language (required)
        lang = replay_config['script_language']
        if not isinstance(lang, str):
            raise ValueError(f"script_language must be a string (got {type(lang).__name__})")
        if lang not in ['Python', 'Groovy']:
            raise ValueError(f"script_language must be 'Python' or 'Groovy' (got '{lang}')")
        
        # Validate optional jvm_profile
        if 'jvm_profile' in replay_config:
            jvm_profile = replay_config['jvm_profile']
            if not isinstance(jvm_profile, str) or not jvm_profile.strip():
                raise ValueError("jvm_profile must be a non-empty string")
        
        # Validate optional server_name
        if 'server_name' in replay_config:
            server_name = replay_config['server_name']
            # Note: Valid server names are environment-specific, so we don't validate the exact value here
            if not isinstance(server_name, str) or not server_name.strip():
                raise ValueError(f"server_name must be a non-empty string (got '{server_name}')")
        
        # Validate replay_timestamp_columns (optional list)
        if 'replay_timestamp_columns' in replay_config:
            ts_cols = replay_config['replay_timestamp_columns']
            if not isinstance(ts_cols, list):
                raise ValueError("replay_timestamp_columns must be a list")
            for idx, ts_config in enumerate(ts_cols):
                if not isinstance(ts_config, dict):
                    raise ValueError(f"replay_timestamp_columns[{idx}] must be a dict")
                required_keys = {'namespace', 'table', 'column'}
                actual_keys = set(ts_config.keys())
                missing_keys = required_keys - actual_keys
                if missing_keys:
                    raise ValueError(f"replay_timestamp_columns[{idx}] missing required keys: {', '.join(sorted(missing_keys))}")
                unexpected_keys = actual_keys - required_keys
                if unexpected_keys:
                    raise ValueError(f"replay_timestamp_columns[{idx}] has unexpected keys: {', '.join(sorted(unexpected_keys))}")
                # Validate that all values are non-empty strings
                for key in required_keys:
                    value = ts_config[key]
                    if not isinstance(value, str) or not value.strip():
                        raise ValueError(f"replay_timestamp_columns[{idx}].{key} must be a non-empty string (got {type(value).__name__})")
    
    def _validate_dates_config(self, dates_config: Dict):
        """Validate dates configuration (start, end, weekdays_only)."""
        # Validate optional weekdays_only
        if 'weekdays_only' in dates_config:
            weekdays = dates_config['weekdays_only']
            if not isinstance(weekdays, bool):
                raise ValueError(f"weekdays_only must be a boolean (got {type(weekdays).__name__})")
        
        # Validate date format and range
        start_str = dates_config['start']
        if not isinstance(start_str, str):
            raise ValueError(f"dates.start must be a string (got {type(start_str).__name__})")
        try:
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"dates.start must be in YYYY-MM-DD format: {e}")
        
        end_str = dates_config['end']
        if not isinstance(end_str, str):
            raise ValueError(f"dates.end must be a string (got {type(end_str).__name__})")
        try:
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"dates.end must be in YYYY-MM-DD format: {e}")
        
        # Validate date range logic
        if end_date < start_date:
            raise ValueError(f"dates.end ({end_str}) must be >= dates.start ({start_str})")
    
    def _cleanup_sessions(self):
        """Stop and delete all tracked sessions. Idempotent - safe to call multiple times."""
        if self._cleanup_done:
            return
        
        self._cleanup_done = True
        
        if not self.sessions:
            return
        
        logger.info(f"Cleaning up {len(self.sessions)} sessions...")
        stopped = 0
        deleted = 0
        errors = 0
        
        for session_key, serial in self.sessions.items():
            date, worker_id = session_key
            try:
                # Try to stop the query first
                try:
                    self.session_mgr.controller_client.stop_query(serial)
                    stopped += 1
                    logger.debug(f"Stopped session: date={date}, worker={worker_id}, serial={serial}")
                except Exception as e:
                    logger.debug(f"Could not stop session (may already be stopped): date={date}, worker={worker_id}, error={e}")
                
                # Try to delete the query
                try:
                    self.session_mgr.controller_client.delete_query(serial)
                    deleted += 1
                    logger.debug(f"Deleted session: date={date}, worker={worker_id}, serial={serial}")
                except Exception as e:
                    logger.debug(f"Could not delete session (may already be deleted): date={date}, worker={worker_id}, error={e}")
                    errors += 1
            except Exception as e:
                logger.error(f"Error cleaning up session: date={date}, worker={worker_id}, error={e}")
                errors += 1
        
        logger.info(f"Cleanup complete: {stopped} stopped, {deleted} deleted, {errors} errors")
    
    def _handle_shutdown_signal(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.warning("\nShutdown signal received. Stopping orchestrator...")
        self.shutdown_requested = True
    
    def _expand_env_vars(self, config: Dict[str, Any]):
        """Expand environment variables in config values."""
        env_vars_found = set()
        
        def expand_value(value):
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                if env_var not in os.environ:
                    raise ValueError(f"Environment variable not set: {env_var}")
                env_vars_found.add(env_var)
                return os.environ[env_var]
            return value
        
        for section in config:
            if isinstance(config[section], dict):
                for key in config[section]:
                    config[section][key] = expand_value(config[section][key])
        
        if env_vars_found:
            logger.debug(f"Expanded environment variables: {', '.join(sorted(env_vars_found))}")
    
    def _generate_dates(self) -> List[str]:
        """Generate list of dates based on configuration."""
        start_date = datetime.strptime(self.config['dates']['start'], '%Y-%m-%d')
        end_date = datetime.strptime(self.config['dates']['end'], '%Y-%m-%d')
        weekdays_only = self.config['dates'].get('weekdays_only', False)
        
        dates = []
        current_date = start_date
        
        while current_date <= end_date:
            if weekdays_only:
                if current_date.weekday() < 5:  # Monday=0, Friday=4
                    dates.append(current_date.strftime('%Y-%m-%d'))
            else:
                dates.append(current_date.strftime('%Y-%m-%d'))
            
            current_date += timedelta(days=1)
        
        weekdays_msg = " (weekdays only)" if weekdays_only else ""
        logger.info(f"Generated {len(dates)} dates: {self.config['dates']['start']} to {self.config['dates']['end']}{weekdays_msg}")
        return dates
    
    def _authenticate(self):
        """Authenticate with Deephaven Enterprise."""
        if self.dry_run:
            logger.info("[DRY RUN] Skipping authentication")
            return
        
        logger.info("Connecting to Deephaven Enterprise")
        
        connection_url = self.config['deephaven']['connection_url']
        
        try:
            self.session_mgr = SessionManager(url=connection_url)
        except Exception as e:
            # Don't log the full exception as it might contain sensitive info
            logger.error(f"Failed to connect to {connection_url}")
            raise ConnectionError(f"Failed to connect to Deephaven Enterprise") from e
        
        # Wrap authentication in try-except to cleanup SessionManager on failure
        try:
            self._authenticate_session()
        except Exception:
            # Clean up session manager on authentication failure
            if self.session_mgr:
                try:
                    # SessionManager may not have explicit close, but clear reference
                    self.session_mgr = None
                except Exception:
                    pass
            raise
    
    def _authenticate_session(self):
        """Perform authentication with session manager (extracted for error handling)."""
        auth_method = self.config['deephaven']['auth_method']
        
        if auth_method == 'password':
            username = self.config['deephaven'].get('username')
            password = self.config['deephaven'].get('password')
            
            if not username or not password:
                raise ValueError("Username and password required for password authentication")
            
            logger.info(f"Authenticating as user: {username}")
            try:
                self.session_mgr.password(user=username, password=password)
            except Exception as e:
                # Don't log password in error message
                logger.error(f"Authentication failed for user: {username}")
                raise ValueError(f"Authentication failed") from e
            
        elif auth_method == 'private_key':
            key_path = self.config['deephaven'].get('private_key_path')
            
            if not key_path:
                raise ValueError("private_key_path required for private_key authentication")
            
            # Validate and resolve private key path
            key_path_obj = Path(key_path).resolve()
            if not key_path_obj.exists():
                raise FileNotFoundError(f"Private key file not found: {key_path}")
            if not key_path_obj.is_file():
                raise ValueError(f"Private key path is not a file: {key_path}")
            
            logger.info(f"Authenticating with private key: {key_path}")
            try:
                self.session_mgr.private_key(str(key_path_obj))
            except Exception as e:
                logger.error(f"Private key authentication failed")
                raise ValueError(f"Private key authentication failed") from e
            
        else:
            raise ValueError(f"Unsupported auth_method: {auth_method}")
        
        logger.info("Authentication successful")
    
    def _build_persistent_query_config(self, date: str, worker_id: int) -> PersistentQueryConfigMessage:
        """Build PersistentQueryConfigMessage for a specific date and worker."""
        # Create basic config message
        config_msg = PersistentQueryConfigMessage()
        
        # Basic fields
        config_msg.serial = PQ_SERIAL_NEW
        config_msg.version = 1
        config_msg.configurationType = "ReplayScript"
        config_msg.name = f"replay_{self.config['name']}_{date.replace('-', '')}_{worker_id}"
        config_msg.owner = self.config['deephaven']['username']
        config_msg.enabled = True
        config_msg.serverName = self.config['replay'].get('server_name', 'AutoQuery')
        config_msg.heapSizeGb = self.config['replay']['heap_size_gb']
        config_msg.bufferPoolToHeapRatio = DEFAULT_BUFFER_POOL_RATIO
        config_msg.detailedGCLoggingEnabled = True
        config_msg.scriptLanguage = self.config['replay']['script_language']
        config_msg.jvmProfile = self.config['replay'].get('jvm_profile', 'Default')
        config_msg.workerKind = "DeephavenCommunity"
        config_msg.restartUsers = DEFAULT_RESTART_USERS
        
        # Initialization timeout (default: 60 seconds)
        init_timeout_minutes = self.config['replay'].get('init_timeout_minutes', 1)
        config_msg.timeoutNanos = int(init_timeout_minutes * 60 * 1_000_000_000)
        
        # Load script content (cached)
        if self.worker_script_content is None:
            worker_script_path = Path(self.config['execution']['worker_script'])
            
            # Resolve worker script path (supports both absolute and relative paths)
            if worker_script_path.is_absolute():
                worker_script = worker_script_path.resolve()
            else:
                # Relative paths are resolved relative to config directory
                worker_script = (self.config_path.parent / worker_script_path).resolve()
            
            if not worker_script.exists():
                raise FileNotFoundError(f"Worker script not found: {worker_script}")
            if not worker_script.is_file():
                raise ValueError(f"Worker script path is not a file: {worker_script}")
            
            with open(worker_script, 'r') as f:
                self.worker_script_content = f.read()
            logger.info(f"Loaded worker script: {worker_script} ({len(self.worker_script_content)} bytes)")
        else:
            logger.debug(f"Using cached worker script content ({len(self.worker_script_content)} bytes)")
        
        config_msg.scriptCode = self.worker_script_content
        
        # Replay-specific fields (JSON) - must be wrapped in type/value structure
        # Controller expects PascalCase which it maps to snake_case in ReplaySettings
        # ReplaySorted uses boolean type, others use string type
        encoded_fields = {
            "ReplaySorted": {
                "type": "boolean",
                "value": self.config['replay'].get('sorted_replay', True)
            },
            "ReplayStart": {
                "type": "string",
                "value": self.config['replay']['replay_start']
            },
            "ReplayDate": {
                "type": "string",
                "value": date
            },
            "ReplaySpeed": {
                "type": "string",
                "value": str(self.config['replay']['replay_speed'])
            }
        }
        config_msg.typeSpecificFieldsJson = json.dumps(encoded_fields)
        
        # Environment variables - must be in alternating name/value pairs
        env_vars = [
            "SIMULATION_NAME", self.config['name'],
            "SIMULATION_DATE", date,
            "WORKER_ID", str(worker_id),
            "NUM_WORKERS", str(self.config['execution']['num_workers']),
            "QUERY_NAME", config_msg.name
        ]
        
        for key, value in self.config['env'].items():
            env_vars.extend([key, str(value)])
        
        config_msg.extraEnvironmentVariables.extend(env_vars)
        
        # JVM arguments
        jvm_args = []
        
        if 'buffer_rows' in self.config['replay']:
            jvm_args.append(f"-DReplayDatabase.BufferSize={self.config['replay']['buffer_rows']}")
        
        # Automatically scale targetCycleDurationMillis to maintain simulated update frequency
        # At 1x speed, cycles run at ~1000ms. At higher speeds, we scale proportionally to maintain
        # the same simulated frequency. Formula: 1000ms / replay_speed
        # Example: 60x speed → 16ms cycles maintains the same simulated update frequency as 1x
        replay_speed = self.config['replay'].get('replay_speed', 1.0)
        if replay_speed > 1.0:
            target_cycle_ms = int(1000 / replay_speed)
            if target_cycle_ms < 10:
                raise ValueError(
                    f"replay_speed={replay_speed} results in targetCycleDurationMillis={target_cycle_ms}ms, "
                    f"which is below the 10ms minimum. Maximum replay_speed is 100."
                )
            jvm_args.append(f"-DPeriodicUpdateGraph.targetCycleDurationMillis={target_cycle_ms}")
            logger.debug(f"Auto-configured targetCycleDurationMillis={target_cycle_ms}ms for replay_speed={replay_speed}x to maintain simulated update frequency")
        
        if 'replay_timestamp_columns' in self.config['replay']:
            for ts_config in self.config['replay']['replay_timestamp_columns']:
                namespace = ts_config['namespace']
                table = ts_config['table']
                column = ts_config['column']
                jvm_args.append(f"-DReplayDatabase.TimestampColumn.{namespace}.{table}={column}")
        
        config_msg.extraJvmArguments.extend(jvm_args)
        
        # Continuous scheduler - starts immediately and runs until worker stops it
        # Temporary scheduler is incompatible with replay queries (cannot have stop time)
        scheduling = GenerateScheduling.generate_continuous_scheduler(
            start_time="00:00:00",
            time_zone="UTC",
            restart_daily=False
        )
        config_msg.scheduling.extend(scheduling)
        
        return config_msg
    
    def _create_session(self, date: str, worker_id: int) -> Tuple[bool, Optional[int]]:
        """Create a replay persistent query session.
        
        Args:
            date: Date string in YYYY-MM-DD format
            worker_id: Worker ID number
            
        Returns:
            Tuple of (success: bool, serial: Optional[int])
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would create session: date={date}, worker={worker_id}")
            return True, -1  # Fake serial for dry run
        
        session_key = (date, worker_id)
        
        try:
            config_msg = self._build_persistent_query_config(date, worker_id)
            serial = self.session_mgr.controller_client.add_query(config_msg)
            
            logger.info(f"Session created: date={date}, worker={worker_id}, serial={serial}")
            return True, serial
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to create session (connection/timeout): date={date}, worker={worker_id}, "
                f"error={e}. Check network connectivity and server availability."
            )
            return False, None
        except ValueError as e:
            logger.error(
                f"Failed to create session (configuration error): date={date}, worker={worker_id}, "
                f"error={e}. Review worker script and replay configuration."
            )
            return False, None
        except Exception as e:
            logger.error(
                f"Failed to create session (unexpected error): date={date}, worker={worker_id}, "
                f"error={type(e).__name__}: {e}"
            )
            return False, None
    
    def _generate_session_tasks(self) -> List[Tuple[str, int]]:
        """Generate list of (date, worker_id) tuples for all sessions."""
        dates = self._generate_dates()
        num_workers = self.config['execution']['num_workers']
        
        tasks = []
        for date in dates:
            for worker_id in range(num_workers):
                tasks.append((date, worker_id))
        
        logger.info(f"Generated {len(tasks)} tasks ({len(dates)} dates × {num_workers} workers)")
        return tasks
    
    def _get_running_session_count(self, pq_info_map: Dict[int, Any], active_sessions: Set[Tuple[str, int]]) -> int:
        """Count currently running sessions managed by this orchestrator.
        
        Args:
            pq_info_map: Map of serial -> PQ info from controller
            active_sessions: Set of (date, worker_id) tuples currently active
            
        Returns:
            Count of running sessions
        """
        if self.dry_run:
            return len(active_sessions)
        
        running_count = 0
        # Only check active sessions, not all sessions
        for session_key in active_sessions:
            serial = self.sessions.get(session_key)
            if serial and serial in pq_info_map:
                pq_info = pq_info_map[serial]
                if self.session_mgr.controller_client.is_running(pq_info.state.status):
                    running_count += 1
        
        logger.debug(f"Running sessions: {running_count}, Active: {len(active_sessions)}")
        return running_count
    
    def _check_session_status(self, session_key: Tuple[str, int], pq_info_map: Dict[int, Any]) -> Optional[str]:
        """Check status of a session.
        
        Args:
            session_key: Tuple of (date, worker_id)
            pq_info_map: Map of serial -> PQ info from controller
            
        Returns:
            'completed', 'failed', 'active', or None if not found
            
        Notes:
            'active' covers all non-terminal states (initializing, acquiring worker, running, etc.)
        """
        if self.dry_run:
            # In dry run, simulate completion
            return 'completed'
        
        serial = self.sessions.get(session_key)
        if serial is None:
            return None
        
        if serial not in pq_info_map:
            return None
        
        pq_info = pq_info_map[serial]
        status = pq_info.state.status
        
        if self.session_mgr.controller_client.is_terminal(status):
            status_name = self.session_mgr.controller_client.status_name(status)
            if status_name in ('PQS_COMPLETED', 'PQS_STOPPED'):
                return 'completed'
            else:
                return 'failed'
        else:
            # All non-terminal states: keep monitoring
            return 'active'
    
    def _print_header(self):
        """Print orchestrator header."""
        logger.info("=" * 80)
        logger.info("Starting Replay Orchestrator")
        logger.info("=" * 80)
    
    def _print_config_summary(self, total_tasks: int, max_concurrent: int, max_retries: int):
        """Print configuration summary."""
        logger.info(f"Total sessions to create: {total_tasks}")
        logger.info(f"Dates: {self.config['dates']['start']} to {self.config['dates']['end']}")
        logger.info(f"Workers: {self.config['execution']['num_workers']}")
        logger.info(f"Max concurrent sessions: {max_concurrent}")
        logger.info(f"Max retries: {max_retries}")
    
    def _launch_pending_sessions(self, pending_tasks: Deque[Tuple[str, int]], active_sessions: Set[Tuple[str, int]], 
                                 max_concurrent: int, max_retries: int, total_tasks: int, 
                                 pq_info_map: Dict[int, Any]) -> Tuple[int, bool]:
        """Launch sessions up to capacity.
        
        Args:
            pending_tasks: Deque of (date, worker_id) tasks to launch
            active_sessions: Set of currently active (date, worker_id) tuples (modified in place)
            max_concurrent: Maximum concurrent sessions allowed
            max_retries: Maximum retry attempts for failed creations
            total_tasks: Total number of tasks for logging
            pq_info_map: Current PQ info map from controller
            
        Returns:
            Tuple of (created_count: int, map_needs_refresh: bool)
        """
        created_count = 0
        map_needs_refresh = False
        
        while True:
            running_count = self._get_running_session_count(pq_info_map, active_sessions)
            if running_count >= max_concurrent:
                logger.debug(f"At capacity: {running_count}/{max_concurrent} sessions running")
                break
            if not pending_tasks:
                logger.debug("No pending tasks remaining")
                break
            if self.shutdown_requested:
                logger.info("Shutdown requested, stopping session creation")
                break
            
            date, worker_id = pending_tasks.popleft()
            session_key = (date, worker_id)
            
            success, serial = self._create_session(date, worker_id)
            
            if success:
                active_sessions.add(session_key)
                self.sessions[session_key] = serial
                created_count += 1
                map_needs_refresh = True  # Map is stale after creation
                
                # Clean up retry tracking for successful creation
                self.retry_counts.pop(session_key, None)
                
                logger.info(f"Created session {len(self.sessions)}/{total_tasks}: date={date}, worker={worker_id}, serial={serial}")
            else:
                # Handle creation failure
                retry_count = self.retry_counts.get(session_key, 0)
                
                if retry_count < max_retries:
                    self.retry_counts[session_key] = retry_count + 1
                    # Use appendleft for immediate retry on next iteration
                    pending_tasks.appendleft((date, worker_id))
                    logger.warning(f"Retrying session: date={date}, worker={worker_id} (attempt {retry_count + 1}/{max_retries})")
                    time.sleep(DEFAULT_RETRY_DELAY_SECONDS)
                else:
                    self.failed_sessions.append(session_key)
                    # Clean up retry tracking after final failure
                    self.retry_counts.pop(session_key, None)
                    logger.error(f"Failed to create session after {max_retries} retries: date={date}, worker={worker_id}")
        
        return created_count, map_needs_refresh
    
    def _process_active_sessions(self, active_sessions: Set[Tuple[str, int]], pq_info_map: Dict[int, Any]) -> Tuple[int, int]:
        """Process active sessions, checking for completion or failure.
        
        Args:
            active_sessions: Set of currently active (date, worker_id) tuples (modified in place)
            pq_info_map: Current PQ info map from controller
            
        Returns:
            Tuple of (completed_count: int, failed_count: int)
            
        Side effects:
            - Removes completed/failed sessions from active_sessions
            - Appends failed sessions to self.failed_sessions
        """
        completed_count = 0
        failed_count = 0
        
        for session_key in list(active_sessions):
            status = self._check_session_status(session_key, pq_info_map)
            date, worker_id = session_key
            
            if status == 'completed':
                active_sessions.remove(session_key)
                completed_count += 1
                logger.info(f"Session completed successfully: date={date}, worker={worker_id}")
            elif status == 'failed':
                active_sessions.remove(session_key)
                failed_count += 1
                self.failed_sessions.append(session_key)
                serial = self.sessions.get(session_key)
                if serial and serial in pq_info_map:
                    status_name = self.session_mgr.controller_client.status_name(pq_info_map[serial].state.status)
                    logger.error(
                        f"Session failed: date={date}, worker={worker_id}, serial={serial}, status={status_name}. "
                        f"Check persistent query logs for serial {serial} for details. "
                        f"Common causes: script errors, insufficient heap memory, missing data."
                    )
                else:
                    logger.error(
                        f"Session failed: date={date}, worker={worker_id}. "
                        f"Session info not available in PQ map."
                    )
            # 'active' status: leave session in active_sessions and continue monitoring
        
        return completed_count, failed_count
    
    def _wait_for_status_change(self, map_version: Optional[int], timeout_seconds: Optional[int] = None) -> Tuple[Dict[int, Any], Optional[int]]:
        """Wait for PQ status changes.
        
        Args:
            map_version: Current map version to wait for change from
            timeout_seconds: Timeout in seconds (uses DEFAULT_STATUS_TIMEOUT_SECONDS if None)
            
        Returns:
            Tuple of (pq_info_map: Dict, new_map_version: Optional[int])
        """
        if self.dry_run:
            time.sleep(0.1)  # Small delay for dry run
            return {}, None
        
        if timeout_seconds is None:
            timeout_seconds = DEFAULT_STATUS_TIMEOUT_SECONDS
        
        try:
            pq_info_map, new_version = self.session_mgr.controller_client.map_and_version()
            logger.debug(f"Waiting for status change from version {new_version} (timeout: {timeout_seconds}s)")
            self.session_mgr.controller_client.wait_for_change_from_version(
                map_version=new_version,
                timeout_seconds=timeout_seconds
            )
            return pq_info_map, new_version
        except (TimeoutError, ConnectionError) as e:
            logger.debug(f"Status check timeout or error: {e}")
            time.sleep(DEFAULT_RETRY_DELAY_SECONDS)
            # Return current map version even on timeout
            try:
                pq_info_map, new_version = self.session_mgr.controller_client.map_and_version()
                return pq_info_map, new_version
            except (TimeoutError, ConnectionError) as e2:
                logger.debug(f"Map refresh also failed: {e2}")
                return {}, map_version
            except Exception as e2:
                logger.warning(f"Unexpected error refreshing map: {type(e2).__name__}: {e2}")
                return {}, map_version
    
    def _delete_queries(self, delete_successful: bool, delete_failed: bool):
        """Delete queries managed by this orchestrator based on options.
        
        Args:
            delete_successful: If True, delete successfully completed queries
            delete_failed: If True, delete failed queries
        """
        if self.dry_run:
            return
        
        if not delete_successful and not delete_failed:
            return
        
        deleted_successful = 0
        deleted_failed = 0
        failed_deletions = 0
        
        for session_key, serial in self.sessions.items():
            is_failed = session_key in self.failed_sessions
            
            # Decide whether to delete based on session status and config
            should_delete = (is_failed and delete_failed) or (not is_failed and delete_successful)
            
            if not should_delete:
                continue
            
            try:
                self.session_mgr.controller_client.delete_query(serial)
                if is_failed:
                    deleted_failed += 1
                else:
                    deleted_successful += 1
                date, worker_id = session_key
                logger.debug(f"Deleted {'failed' if is_failed else 'successful'} query: date={date}, worker={worker_id}, serial={serial}")
            except Exception as e:
                failed_deletions += 1
                date, worker_id = session_key
                logger.warning(f"Failed to delete query: date={date}, worker={worker_id}, serial={serial}: {e}")
        
        if deleted_successful > 0:
            logger.info(f"Deleted {deleted_successful} successful queries")
        if deleted_failed > 0:
            logger.info(f"Deleted {deleted_failed} failed queries")
        if failed_deletions > 0:
            logger.warning(f"Failed to delete {failed_deletions} queries")
    
    def _print_summary(self, total_tasks: int, created: int, completed: int, failed: int):
        """Print final execution summary."""
        logger.info("=" * 80)
        logger.info("Orchestrator Complete")
        logger.info("=" * 80)
        logger.info(f"Total sessions created: {created}")
        logger.info(f"Successfully completed: {completed}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Creation failures: {total_tasks - created}")
        
        if self.failed_sessions:
            logger.error("Failed sessions:")
            for date, worker_id in self.failed_sessions:
                logger.error(f"  - date={date}, worker={worker_id}")
    
    def run(self) -> int:
        """Execute the orchestrator.
        
        Returns:
            Exit code: EXIT_SUCCESS, EXIT_CREATION_FAILURES, EXIT_EXECUTION_FAILURES, 
                      EXIT_BOTH_FAILURES, or EXIT_ERROR
        """
        self._print_header()
        
        if self.dry_run:
            logger.info("[DRY RUN MODE] - No sessions will be created")
        
        # Authenticate
        self._authenticate()
        
        # Generate tasks
        tasks = self._generate_session_tasks()
        total_tasks = len(tasks)
        
        # Configuration
        max_concurrent = self.config['execution']['max_concurrent_sessions']
        max_retries = self.config['execution'].get('max_retries', DEFAULT_MAX_RETRIES)
        self._print_config_summary(total_tasks, max_concurrent, max_retries)
        
        if self.dry_run:
            logger.info("[DRY RUN] Validation complete. Exiting.")
            return EXIT_SUCCESS
        
        # Execute sessions
        created, completed, failed = 0, 0, 0
        active_sessions: Set[Tuple[str, int]] = set()
        pending_tasks: Deque[Tuple[str, int]] = deque(tasks)
        
        # Get initial PQ info map
        pq_info_map, map_version = self.session_mgr.controller_client.map_and_version()
        
        # Progress tracking
        idle_iterations = 0
        last_progress_count = 0
        startup_phase = True  # Use higher threshold during startup when queries are initializing
        
        while (pending_tasks or active_sessions) and not self.shutdown_requested:
            # Launch new sessions up to capacity
            created_delta, map_needs_refresh = self._launch_pending_sessions(
                pending_tasks, active_sessions, max_concurrent, max_retries, total_tasks, pq_info_map
            )
            created += created_delta
            
            # Refresh map if we created sessions
            if map_needs_refresh:
                old_version = map_version
                pq_info_map, map_version = self.session_mgr.controller_client.map_and_version()
                logger.debug(f"Refreshed PQ info map (version: {old_version} → {map_version})")
            
            # Check for completed/failed sessions
            completed_delta, failed_delta = self._process_active_sessions(active_sessions, pq_info_map)
            completed += completed_delta
            failed += failed_delta
            
            # Exit startup phase once first session completes or fails
            if startup_phase and (completed > 0 or failed > 0):
                startup_phase = False
                logger.debug("Exiting startup phase, sessions are progressing")
            
            # Track progress to detect stalls
            current_progress = created + completed + failed
            if current_progress == last_progress_count:
                idle_iterations += 1
                # Use higher threshold during startup when queries are initializing
                threshold = MAX_IDLE_ITERATIONS_STARTUP if startup_phase else MAX_IDLE_ITERATIONS
                if idle_iterations >= threshold:
                    if startup_phase:
                        logger.warning(
                            f"Sessions initializing ({threshold} iterations without progress). "
                            f"Created: {created}, Active: {len(active_sessions)}, Pending: {len(pending_tasks)}. "
                            f"This is normal during startup as queries initialize and acquire workers."
                        )
                    else:
                        logger.warning(
                            f"No progress for {threshold} iterations. "
                            f"Created: {created}, Active: {len(active_sessions)}, Pending: {len(pending_tasks)}, "
                            f"Completed: {completed}, Failed: {failed}. "
                            f"If stuck, check server logs and query status in the UI."
                        )
                    idle_iterations = 0  # Reset after warning
            else:
                idle_iterations = 0
                last_progress_count = current_progress
            
            # Wait for status changes if we have work remaining
            if active_sessions or pending_tasks:
                pq_info_map, map_version = self._wait_for_status_change(map_version)
        
        # Handle shutdown
        if self.shutdown_requested:
            logger.warning(f"Orchestrator stopped by user. Active sessions: {len(active_sessions)}, Pending: {len(pending_tasks)}")
            self._cleanup_sessions()
        
        # Delete queries based on configuration (before summary so deletion is included)
        delete_successful = self.config['execution'].get('delete_successful_queries', True)
        delete_failed = self.config['execution'].get('delete_failed_queries', False)
        if not self.dry_run:
            self._delete_queries(delete_successful, delete_failed)
        
        # Print summary
        self._print_summary(total_tasks, created, completed, failed)
        
        # Determine exit code
        creation_failures = total_tasks - created
        execution_failures = failed
        
        if creation_failures > 0 and execution_failures > 0:
            return EXIT_BOTH_FAILURES
        elif execution_failures > 0:
            return EXIT_EXECUTION_FAILURES
        elif creation_failures > 0:
            return EXIT_CREATION_FAILURES
        else:
            return EXIT_SUCCESS


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Orchestrate Deephaven Enterprise replay persistent queries'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Path to configuration YAML file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without creating sessions'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose (DEBUG) logging for detailed troubleshooting'
    )
    
    args = parser.parse_args()
    
    # Configure logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        orchestrator = ReplayOrchestrator(args.config, dry_run=args.dry_run)
        exit_code = orchestrator.run()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        sys.exit(EXIT_ERROR)
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}", exc_info=True)
        sys.exit(EXIT_ERROR)


if __name__ == '__main__':
    main()
