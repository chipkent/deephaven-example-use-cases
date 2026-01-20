#!/usr/bin/env python3
"""
Generic Replay Orchestrator for Deephaven Enterprise

This script orchestrates the execution of replay persistent queries across multiple dates and partitions.
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
    """Orchestrates replay persistent query execution across dates and partitions."""
    
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
        self.sessions: Dict[Tuple[str, int], int] = {}  # (date, partition_id) -> serial
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
    
    def _validate_config_sections(self, config: Dict):
        """Validate that config has expected sections and required sections."""
        expected_sections = {'name', 'deephaven', 'execution', 'replay', 'dates', 'env'}
        
        actual_sections = set(config.keys())
        unexpected_sections = actual_sections - expected_sections
        if unexpected_sections:
            raise ValueError(f"Unexpected config sections: {', '.join(sorted(unexpected_sections))}")
        
        if 'name' not in config:
            raise ValueError("Missing required field: name")
        if not isinstance(config['name'], str) or not config['name'].strip():
            raise ValueError("name must be a non-empty string")
        
        required_sections = ['deephaven', 'execution', 'replay', 'dates', 'env']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")
        
        if not isinstance(config['env'], dict):
            raise ValueError("env section must be a dictionary")
    
    def _validate_section_fields(self, config: Dict):
        """Validate that each section has required fields and no unexpected fields."""
        required_fields = {
            'deephaven': ['connection_url', 'auth_method', 'username'],
            'execution': ['worker_script', 'num_partitions', 'max_concurrent_sessions'],
            'replay': ['heap_size_gb', 'replay_start', 'replay_speed', 'script_language'],
            'dates': ['start', 'end']
        }
        
        allowed_fields = {
            'deephaven': {'connection_url', 'auth_method', 'username', 'password', 'private_key_path'},
            'execution': {
                'worker_script',
                'num_partitions',
                'max_concurrent_sessions',
                'max_retries',
                'max_failures',
                'delete_successful_queries',
                'delete_failed_queries',
            },
            'replay': {
                'heap_size_gb', 'init_timeout_minutes',
                'replay_start', 'replay_speed', 'sorted_replay',
                'buffer_rows', 'replay_timestamp_columns',
                'script_language', 'jvm_profile', 'server_name'
            },
            'dates': {'start', 'end', 'weekdays_only'},
            'env': None
        }
        
        for section, fields in required_fields.items():
            for field in fields:
                if field not in config[section]:
                    raise ValueError(f"Missing required field: {section}.{field}")
            
            if allowed_fields[section] is not None:
                actual_fields = set(config[section].keys())
                unexpected_fields = actual_fields - allowed_fields[section]
                if unexpected_fields:
                    raise ValueError(
                        f"Unexpected fields in {section}: {', '.join(sorted(unexpected_fields))}. "
                        f"Allowed fields: {', '.join(sorted(allowed_fields[section]))}"
                    )
    
    def _validate_config_structure(self, config: Dict):
        """Validate top-level config structure, sections, and field presence."""
        self._validate_config_sections(config)
        self._validate_section_fields(config)
    
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
    
    def _validate_execution_numeric_fields(self, exec_config: Dict):
        """Validate numeric fields in execution configuration."""
        worker_script = exec_config['worker_script']
        if not isinstance(worker_script, str) or not worker_script.strip():
            raise ValueError("worker_script must be a non-empty string")
        
        partitions = exec_config['num_partitions']
        if not isinstance(partitions, int):
            raise ValueError(f"num_partitions must be an integer (got {type(partitions).__name__})")
        if partitions <= 0:
            raise ValueError(f"num_partitions must be > 0 (got {partitions})")
        if partitions > 1000:
            raise ValueError(f"num_partitions too high (got {partitions}, max 1000)")
        
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
        
        if 'max_failures' in exec_config:
            failures = exec_config['max_failures']
            if not isinstance(failures, int):
                raise ValueError(f"max_failures must be an integer (got {type(failures).__name__})")
            if failures < 0:
                raise ValueError(f"max_failures must be >= 0 (got {failures})")
    
    def _validate_execution_boolean_fields(self, exec_config: Dict):
        """Validate boolean fields in execution configuration."""
        if 'delete_successful_queries' in exec_config:
            delete_successful = exec_config['delete_successful_queries']
            if not isinstance(delete_successful, bool):
                raise ValueError(f"delete_successful_queries must be a boolean (got {type(delete_successful).__name__})")
        
        if 'delete_failed_queries' in exec_config:
            delete_failed = exec_config['delete_failed_queries']
            if not isinstance(delete_failed, bool):
                raise ValueError(f"delete_failed_queries must be a boolean (got {type(delete_failed).__name__})")
    
    def _validate_execution_config(self, exec_config: Dict):
        """Validate execution configuration (workers, script, concurrency)."""
        self._validate_execution_numeric_fields(exec_config)
        self._validate_execution_boolean_fields(exec_config)
    
    def _validate_replay_numeric_fields(self, replay_config: Dict):
        """Validate numeric fields in replay configuration."""
        heap = replay_config['heap_size_gb']
        if not isinstance(heap, (int, float)):
            raise ValueError(f"heap_size_gb must be a number (got {type(heap).__name__})")
        if heap <= 0:
            raise ValueError(f"heap_size_gb must be > 0 (got {heap})")
        if heap > 512:
            raise ValueError(f"heap_size_gb too high (got {heap}, max 512)")
        
        speed = replay_config['replay_speed']
        if not isinstance(speed, (int, float)):
            raise ValueError(f"replay_speed must be a number (got {type(speed).__name__})")
        if speed < 1.0:
            raise ValueError(f"replay_speed must be >= 1.0 for backtesting (got {speed})")
        if speed > 100.0:
            raise ValueError(f"replay_speed too high (got {speed}, max 100).")
        
        if 'init_timeout_minutes' in replay_config:
            timeout = replay_config['init_timeout_minutes']
            if not isinstance(timeout, (int, float)):
                raise ValueError(f"init_timeout_minutes must be a number (got {type(timeout).__name__})")
            if timeout <= 0:
                raise ValueError(f"init_timeout_minutes must be > 0 (got {timeout})")
        
        if 'buffer_rows' in replay_config:
            buffer = replay_config['buffer_rows']
            if not isinstance(buffer, int):
                raise ValueError(f"buffer_rows must be an integer (got {type(buffer).__name__})")
            if buffer <= 0:
                raise ValueError(f"buffer_rows must be > 0 (got {buffer})")
    
    def _validate_replay_string_fields(self, replay_config: Dict):
        """Validate string fields in replay configuration."""
        replay_start = replay_config['replay_start']
        if not isinstance(replay_start, str):
            raise ValueError(f"replay_start must be a string (got {type(replay_start).__name__})")
        if not re.match(r'^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$', replay_start):
            raise ValueError(f"replay_start must be in HH:MM:SS format (got '{replay_start}')")
        
        if 'sorted_replay' in replay_config:
            sorted_replay = replay_config['sorted_replay']
            if not isinstance(sorted_replay, bool):
                raise ValueError(f"sorted_replay must be a boolean (got {type(sorted_replay).__name__})")
        
        lang = replay_config['script_language']
        if not isinstance(lang, str):
            raise ValueError(f"script_language must be a string (got {type(lang).__name__})")
        if lang not in ['Python', 'Groovy']:
            raise ValueError(f"script_language must be 'Python' or 'Groovy' (got '{lang}')")
        
        if 'jvm_profile' in replay_config:
            jvm_profile = replay_config['jvm_profile']
            if not isinstance(jvm_profile, str) or not jvm_profile.strip():
                raise ValueError("jvm_profile must be a non-empty string")
        
        if 'server_name' in replay_config:
            server_name = replay_config['server_name']
            if not isinstance(server_name, str) or not server_name.strip():
                raise ValueError(f"server_name must be a non-empty string (got '{server_name}')")
    
    def _validate_replay_timestamp_columns(self, replay_config: Dict):
        """Validate replay_timestamp_columns configuration."""
        if 'replay_timestamp_columns' not in replay_config:
            return
        
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
            
            for key in required_keys:
                value = ts_config[key]
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"replay_timestamp_columns[{idx}].{key} must be a non-empty string (got {type(value).__name__})")
    
    def _validate_replay_config(self, replay_config: Dict):
        """Validate replay configuration (heap, speed, replay settings)."""
        self._validate_replay_numeric_fields(replay_config)
        self._validate_replay_string_fields(replay_config)
        self._validate_replay_timestamp_columns(replay_config)
    
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
            date, partition_id = session_key
            try:
                # Try to stop the query first
                try:
                    self.session_mgr.controller_client.stop_query(serial)
                    stopped += 1
                    logger.debug(f"Stopped session: date={date}, partition={partition_id}, serial={serial}")
                except Exception as e:
                    logger.debug(f"Could not stop session (may already be stopped): date={date}, partition={partition_id}, error={e}")
                
                # Try to delete the query
                try:
                    self.session_mgr.controller_client.delete_query(serial)
                    deleted += 1
                    logger.debug(f"Deleted session: date={date}, partition={partition_id}, serial={serial}")
                except Exception as e:
                    logger.debug(f"Could not delete session (may already be deleted): date={date}, partition={partition_id}, error={e}")
                    errors += 1
            except Exception as e:
                logger.error(f"Error cleaning up session: date={date}, partition={partition_id}, error={e}")
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
    
    def _load_worker_script_content(self):
        """Load and cache worker script content."""
        if self.worker_script_content is None:
            worker_script_path = Path(self.config['execution']['worker_script'])
            
            if worker_script_path.is_absolute():
                worker_script = worker_script_path.resolve()
            else:
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
    
    def _build_pq_basic_fields(self, config_msg: PersistentQueryConfigMessage, date: str, partition_id: int):
        """Populate basic fields in PersistentQueryConfigMessage."""
        config_msg.serial = PQ_SERIAL_NEW
        config_msg.version = 1
        config_msg.configurationType = "ReplayScript"
        config_msg.name = f"replay_{self.config['name']}_{date.replace('-', '')}_{partition_id}"
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
        
        init_timeout_minutes = self.config['replay'].get('init_timeout_minutes', 1)
        config_msg.timeoutNanos = int(init_timeout_minutes * 60 * 1_000_000_000)
        
        config_msg.scriptCode = self.worker_script_content
    
    def _build_pq_replay_fields(self, date: str) -> str:
        """Build replay-specific fields as JSON string.
        
        Returns:
            JSON string with type/value wrapped replay fields
        """
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
        return json.dumps(encoded_fields)
    
    def _build_pq_environment_vars(self, date: str, partition_id: int, query_name: str) -> list:
        """Build environment variables list in alternating name/value pairs.
        
        Args:
            date: Simulation date
            partition_id: Partition ID
            query_name: Query name for QUERY_NAME environment variable
            
        Returns:
            List of alternating name/value strings
        """
        env_vars = [
            "SIMULATION_NAME", self.config['name'],
            "SIMULATION_DATE", date,
            "PARTITION_ID", str(partition_id),
            "NUM_PARTITIONS", str(self.config['execution']['num_partitions']),
            "QUERY_NAME", query_name
        ]
        
        for key, value in self.config['env'].items():
            env_vars.extend([key, str(value)])
        
        return env_vars
    
    def _build_pq_jvm_arguments(self) -> list:
        """Build JVM arguments list.
        
        Returns:
            List of JVM argument strings
        """
        jvm_args = []
        
        if 'buffer_rows' in self.config['replay']:
            jvm_args.append(f"-DReplayDatabase.BufferSize={self.config['replay']['buffer_rows']}")
        
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
        
        return jvm_args
    
    def _build_pq_scheduling(self) -> list:
        """Build scheduling configuration.
        
        Returns:
            List of scheduling messages
        """
        return GenerateScheduling.generate_continuous_scheduler(
            start_time="00:00:00",
            time_zone="UTC",
            restart_daily=False
        )
    
    def _build_persistent_query_config(self, date: str, partition_id: int) -> PersistentQueryConfigMessage:
        """Build PersistentQueryConfigMessage for a specific date and partition."""
        config_msg = PersistentQueryConfigMessage()
        
        self._load_worker_script_content()
        self._build_pq_basic_fields(config_msg, date, partition_id)
        
        config_msg.typeSpecificFieldsJson = self._build_pq_replay_fields(date)
        
        env_vars = self._build_pq_environment_vars(date, partition_id, config_msg.name)
        config_msg.extraEnvironmentVariables.extend(env_vars)
        
        jvm_args = self._build_pq_jvm_arguments()
        config_msg.extraJvmArguments.extend(jvm_args)
        
        scheduling = self._build_pq_scheduling()
        config_msg.scheduling.extend(scheduling)
        
        return config_msg
    
    def _create_session(self, date: str, partition_id: int) -> Tuple[bool, Optional[int]]:
        """Create a replay persistent query session.
        
        Args:
            date: Date string in YYYY-MM-DD format
            partition_id: Partition ID number
            
        Returns:
            Tuple of (success: bool, serial: Optional[int])
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would create session: date={date}, partition={partition_id}")
            return True, -1  # Fake serial for dry run
        
        session_key = (date, partition_id)
        
        try:
            config_msg = self._build_persistent_query_config(date, partition_id)
            serial = self.session_mgr.controller_client.add_query(config_msg)
            
            logger.info(f"Session created: date={date}, partition={partition_id}, serial={serial}")
            return True, serial
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to create session (connection/timeout): date={date}, partition={partition_id}, "
                f"error={e}. Check network connectivity and server availability."
            )
            return False, None
        except ValueError as e:
            logger.error(
                f"Failed to create session (configuration error): date={date}, partition={partition_id}, "
                f"error={e}. Review worker script and replay configuration."
            )
            return False, None
        except Exception as e:
            logger.error(
                f"Failed to create session (unexpected error): date={date}, partition={partition_id}, "
                f"error={type(e).__name__}: {e}"
            )
            return False, None
    
    def _generate_session_tasks(self) -> List[Tuple[str, int]]:
        """Generate list of (date, partition_id) tuples for all sessions."""
        dates = self._generate_dates()
        num_partitions = self.config['execution']['num_partitions']
        
        tasks = []
        for date in dates:
            for partition_id in range(num_partitions):
                tasks.append((date, partition_id))
        
        logger.info(f"Generated {len(tasks)} tasks ({len(dates)} dates × {num_partitions} partitions)")
        return tasks
    
    def _get_running_session_count(self, pq_info_map: Dict[int, Any], active_sessions: Set[Tuple[str, int]]) -> int:
        """Count currently active (non-terminal) sessions managed by this orchestrator.
        
        Args:
            pq_info_map: Map of serial -> PQ info from controller
            active_sessions: Set of (date, partition_id) tuples currently active
            
        Returns:
            Count of active (non-terminal) sessions including initializing, acquiring workers, and running
        """
        if self.dry_run:
            return len(active_sessions)
        
        active_count = 0
        # Count all non-terminal sessions to properly enforce concurrency limit
        for session_key in active_sessions:
            serial = self.sessions.get(session_key)
            if serial is None:
                # Should not happen due to ordering in _launch_pending_sessions
                continue
            
            if serial in pq_info_map:
                pq_info = pq_info_map[serial]
                # Only skip if we know it's terminal
                if not self.session_mgr.controller_client.is_terminal(pq_info.state.status):
                    active_count += 1
            else:
                # Session not in map yet (newly created), assume it's active
                active_count += 1
        
        logger.debug(f"Active (non-terminal) sessions: {active_count}, Tracked active: {len(active_sessions)}")
        return active_count
    
    def _check_session_status(self, session_key: Tuple[str, int], pq_info_map: Dict[int, Any]) -> Optional[str]:
        """Check status of a session.
        
        Args:
            session_key: Tuple of (date, partition_id)
            pq_info_map: Map of serial -> PQ info from controller
            
        Returns:
            'completed', 'failed', 'resource_unavailable', 'active', or None if not found
            
        Notes:
            'active' covers all non-terminal states (initializing, acquiring worker, running, etc.)
            'resource_unavailable' indicates autoscaler has no available capacity (transient, retriable)
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
                # Check if this is a resource unavailability failure
                date, partition_id = session_key
                exception_details = pq_info.state.exceptionDetails
                exception_str = str(exception_details) if exception_details else ''
                
                # DEBUG: Log exception details and pattern matching
                logger.warning(f"[DEBUG] Session failed: date={date}, partition={partition_id}, status={status_name}")
                logger.warning(f"[DEBUG] Exception (first 1000 chars): {exception_str[:1000]}")
                
                has_resources_unavailable = 'ResourcesUnavailableException' in exception_str
                has_unable_to_find = 'Unable to find available server' in exception_str
                has_unable_to_determine = 'Unable to determine dispatcher' in exception_str
                has_no_dispatcher_resources = 'No dispatcher resources available' in exception_str
                
                logger.warning(f"[DEBUG] Pattern checks: ResourcesUnavailableException={has_resources_unavailable}, "
                              f"Unable to find available server={has_unable_to_find}, "
                              f"Unable to determine dispatcher={has_unable_to_determine}, "
                              f"No dispatcher resources available={has_no_dispatcher_resources}")
                
                if exception_str and (has_resources_unavailable or has_unable_to_find or has_unable_to_determine or has_no_dispatcher_resources):
                    logger.warning(f"[DEBUG] CLASSIFYING AS RESOURCE_UNAVAILABLE")
                    return 'resource_unavailable'
                
                logger.warning(f"[DEBUG] CLASSIFYING AS FAILED")
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
        logger.info(f"Partitions: {self.config['execution']['num_partitions']}")
        logger.info(f"Max concurrent sessions: {max_concurrent}")
        logger.info(f"Max retries: {max_retries}")
    
    def _handle_session_creation_failure(self, session_key: Tuple[str, int], max_retries: int, 
                                         pending_tasks: Deque[Tuple[str, int]]):
        """Handle session creation failure with retry logic.
        
        Args:
            session_key: Tuple of (date, partition_id)
            max_retries: Maximum retry attempts
            pending_tasks: Deque to add retry tasks to (modified in place)
        """
        date, partition_id = session_key
        retry_count = self.retry_counts.get(session_key, 0)
        
        if retry_count < max_retries:
            self.retry_counts[session_key] = retry_count + 1
            pending_tasks.appendleft((date, partition_id))
            logger.warning(f"Retrying session: date={date}, partition={partition_id} (attempt {retry_count + 1}/{max_retries})")
            time.sleep(DEFAULT_RETRY_DELAY_SECONDS)
        else:
            self.retry_counts.pop(session_key, None)
            logger.error(f"Failed to create session after {max_retries} retries: date={date}, partition={partition_id}")
    
    def _launch_pending_sessions(self, pending_tasks: Deque[Tuple[str, int]], active_sessions: Set[Tuple[str, int]], 
                                 max_concurrent: int, max_retries: int, total_tasks: int, 
                                 pq_info_map: Dict[int, Any]) -> int:
        """Launch sessions up to capacity.
        
        Args:
            pending_tasks: Deque of (date, partition_id) tasks to launch
            active_sessions: Set of currently active (date, partition_id) tuples (modified in place)
            max_concurrent: Maximum concurrent sessions allowed
            max_retries: Maximum retry attempts for failed creations
            total_tasks: Total number of tasks for logging
            pq_info_map: Current PQ info map from controller
            
        Returns:
            Number of sessions created this call
        """
        created_count = 0
        
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
            
            date, partition_id = pending_tasks.popleft()
            session_key = (date, partition_id)
            
            success, serial = self._create_session(date, partition_id)
            
            if success:
                self.sessions[session_key] = serial
                active_sessions.add(session_key)
                created_count += 1
                self.retry_counts.pop(session_key, None)
                logger.info(f"Created session {len(self.sessions)}/{total_tasks}: date={date}, partition={partition_id}, serial={serial}")
            else:
                self._handle_session_creation_failure(session_key, max_retries, pending_tasks)
        
        return created_count
    
    def _handle_resource_unavailability(self, session_key: Tuple[str, int], pending_tasks: Deque[Tuple[str, int]]):
        """Handle resource unavailability by deleting PQ and re-queueing task.
        
        Args:
            session_key: Tuple of (date, partition_id)
            pending_tasks: Deque to add retry task to (modified in place)
        """
        date, partition_id = session_key
        serial = self.sessions.get(session_key)
        
        logger.warning(
            f"Resource unavailable for session: date={date}, partition={partition_id}, serial={serial}. "
            f"Autoscaler has no available capacity. Deleting PQ and re-queueing for retry."
        )
        
        # Delete the failed PQ immediately
        if serial is not None:
            try:
                self.session_mgr.controller_client.delete_query(serial)
                logger.info(f"Deleted PQ serial {serial} due to resource unavailability")
            except Exception as e:
                logger.error(f"Failed to delete PQ serial {serial}: {e}")
        
        # Remove from tracking
        self.sessions.pop(session_key, None)
        
        # Re-queue at front of pending tasks for immediate retry when capacity available
        pending_tasks.appendleft((date, partition_id))
        
        # Wait before continuing to allow autoscaler time to provision
        time.sleep(5)
    
    def _log_session_failure(self, session_key: Tuple[str, int], pq_info_map: Dict[int, Any]):
        """Log detailed failure information for a session.
        
        Args:
            session_key: Tuple of (date, partition_id)
            pq_info_map: Current PQ info map from controller
        """
        date, partition_id = session_key
        serial = self.sessions.get(session_key)
        
        if serial and serial in pq_info_map:
            status_name = self.session_mgr.controller_client.status_name(pq_info_map[serial].state.status)
            logger.error(
                f"Session failed: date={date}, partition={partition_id}, serial={serial}, status={status_name}. "
                f"Check persistent query logs for serial {serial} for details. "
                f"Session left in Deephaven for inspection. "
                f"Common causes: script errors, insufficient heap memory, missing data."
            )
        else:
            logger.error(
                f"Session failed: date={date}, partition={partition_id}. "
                f"Session info not available in PQ map."
            )
    
    def _process_active_sessions(self, active_sessions: Set[Tuple[str, int]], pq_info_map: Dict[int, Any], 
                                 pending_tasks: Deque[Tuple[str, int]]) -> Tuple[int, int]:
        """Process active sessions, checking for completion or failure.
        
        Args:
            active_sessions: Set of currently active (date, partition_id) tuples (modified in place)
            pq_info_map: Current PQ info map from controller
            pending_tasks: Deque to add resource-unavailable retries to (modified in place)
            
        Returns:
            Tuple of (completed_count: int, failed_count: int)
            
        Side effects:
            - Removes completed/failed sessions from active_sessions
            - Appends failed sessions to self.failed_sessions
            - Failed sessions are left in Deephaven for inspection (not deleted)
            - Resource-unavailable sessions are deleted and re-queued (not counted as failures)
        """
        completed_count = 0
        failed_count = 0
        
        for session_key in list(active_sessions):
            status = self._check_session_status(session_key, pq_info_map)
            date, partition_id = session_key
            
            if status == 'completed':
                active_sessions.remove(session_key)
                completed_count += 1
                self.retry_counts.pop(session_key, None)
                logger.info(f"Session completed successfully: date={date}, partition={partition_id}")
            elif status == 'resource_unavailable':
                active_sessions.remove(session_key)
                self._handle_resource_unavailability(session_key, pending_tasks)
            elif status == 'failed':
                active_sessions.remove(session_key)
                failed_count += 1
                self.failed_sessions.append(session_key)
                self._log_session_failure(session_key, pq_info_map)
        
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
                date, partition_id = session_key
                logger.debug(f"Deleted {'failed' if is_failed else 'successful'} query: date={date}, partition={partition_id}, serial={serial}")
            except Exception as e:
                failed_deletions += 1
                date, partition_id = session_key
                logger.warning(f"Failed to delete query: date={date}, partition={partition_id}, serial={serial}: {e}")
        
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
            for date, partition_id in self.failed_sessions:
                logger.error(f"  - date={date}, partition={partition_id}")
    
    def _setup_orchestration(self) -> Tuple[list, int, int, int, int]:
        """Setup orchestration: header, auth, tasks, and config.
        
        Returns:
            Tuple of (tasks, total_tasks, max_concurrent, max_retries, max_failures)
        """
        self._print_header()
        
        if self.dry_run:
            logger.info("[DRY RUN MODE] - No sessions will be created")
        
        self._authenticate()
        
        tasks = self._generate_session_tasks()
        total_tasks = len(tasks)
        
        max_concurrent = self.config['execution']['max_concurrent_sessions']
        max_retries = self.config['execution'].get('max_retries', DEFAULT_MAX_RETRIES)
        max_failures = self.config['execution'].get('max_failures', 10)
        self._print_config_summary(total_tasks, max_concurrent, max_retries)
        
        return tasks, total_tasks, max_concurrent, max_retries, max_failures
    
    def _update_progress_tracking(self, created: int, completed: int, failed: int, 
                                 active_sessions: Set[Tuple[str, int]], pending_tasks: Deque[Tuple[str, int]],
                                 idle_iterations: int, last_progress_count: int, startup_phase: bool) -> Tuple[int, int, bool]:
        """Update progress tracking and detect stalls.
        
        Args:
            created: Number of sessions created so far
            completed: Number of sessions completed so far
            failed: Number of sessions failed so far
            active_sessions: Set of currently active sessions
            pending_tasks: Deque of pending tasks
            idle_iterations: Current idle iteration count
            last_progress_count: Last recorded progress count
            startup_phase: Whether still in startup phase
            
        Returns:
            Tuple of (idle_iterations, last_progress_count, startup_phase)
        """
        if startup_phase and (completed > 0 or failed > 0):
            startup_phase = False
            logger.debug("Exiting startup phase, sessions are progressing")
        
        current_progress = created + completed + failed
        if current_progress == last_progress_count:
            idle_iterations += 1
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
                idle_iterations = 0
        else:
            idle_iterations = 0
            last_progress_count = current_progress
        
        return idle_iterations, last_progress_count, startup_phase
    
    def _handle_orchestration_shutdown(self, active_sessions: Set[Tuple[str, int]], pending_tasks: Deque[Tuple[str, int]]):
        """Handle orchestration shutdown.
        
        Args:
            active_sessions: Set of currently active sessions
            pending_tasks: Deque of pending tasks
        """
        if self.shutdown_requested:
            logger.warning(f"Orchestrator stopped by user. Active sessions: {len(active_sessions)}, Pending: {len(pending_tasks)}")
            self._cleanup_sessions()
    
    def _cleanup_and_report(self, total_tasks: int, created: int, completed: int, failed: int):
        """Delete queries based on config and print summary.
        
        Args:
            total_tasks: Total number of tasks
            created: Number of sessions created
            completed: Number of sessions completed
            failed: Number of sessions failed
        """
        delete_successful = self.config['execution'].get('delete_successful_queries', True)
        delete_failed = self.config['execution'].get('delete_failed_queries', False)
        if not self.dry_run:
            self._delete_queries(delete_successful, delete_failed)
        
        self._print_summary(total_tasks, created, completed, failed)
    
    def _determine_exit_code(self, total_tasks: int, created: int, failed: int) -> int:
        """Determine exit code based on execution results.
        
        Args:
            total_tasks: Total number of tasks
            created: Number of sessions created
            failed: Number of sessions failed
            
        Returns:
            Exit code: EXIT_SUCCESS, EXIT_CREATION_FAILURES, EXIT_EXECUTION_FAILURES, 
                      EXIT_BOTH_FAILURES, or EXIT_ERROR
        """
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
    
    def run(self) -> int:
        """Execute the orchestrator.
        
        Returns:
            Exit code: EXIT_SUCCESS, EXIT_CREATION_FAILURES, EXIT_EXECUTION_FAILURES, 
                      EXIT_BOTH_FAILURES, or EXIT_ERROR
        """
        tasks, total_tasks, max_concurrent, max_retries, max_failures = self._setup_orchestration()
        
        if self.dry_run:
            logger.info("[DRY RUN] Validation complete. Exiting.")
            return EXIT_SUCCESS

        created, completed, failed = 0, 0, 0
        active_sessions: Set[Tuple[str, int]] = set()
        pending_tasks: Deque[Tuple[str, int]] = deque(tasks)
        
        pq_info_map, map_version = self.session_mgr.controller_client.map_and_version()
        
        idle_iterations = 0
        last_progress_count = 0
        startup_phase = True
        
        while (pending_tasks or active_sessions) and not self.shutdown_requested:
            old_version = map_version
            pq_info_map, map_version = self.session_mgr.controller_client.map_and_version()
            logger.debug(f"Refreshed PQ info map at loop start (version: {old_version} → {map_version})")
            
            created_delta = self._launch_pending_sessions(
                pending_tasks, active_sessions, max_concurrent, max_retries, total_tasks, pq_info_map
            )
            created += created_delta
            
            completed_delta, failed_delta = self._process_active_sessions(active_sessions, pq_info_map, pending_tasks)
            completed += completed_delta
            failed += failed_delta
            
            total_failures = len(self.failed_sessions)
            if total_failures >= max_failures:
                logger.error(
                    f"Maximum failure limit reached ({total_failures}/{max_failures}). "
                    f"Aborting orchestration to prevent cascading errors. "
                    f"Check persistent query logs and configuration. "
                    f"Pending tasks: {len(pending_tasks)}, Active sessions: {len(active_sessions)}"
                )
                break
            
            idle_iterations, last_progress_count, startup_phase = self._update_progress_tracking(
                created, completed, failed, active_sessions, pending_tasks,
                idle_iterations, last_progress_count, startup_phase
            )
            
            if active_sessions or pending_tasks:
                time.sleep(1.0)
        
        self._handle_orchestration_shutdown(active_sessions, pending_tasks)
        self._cleanup_and_report(total_tasks, created, completed, failed)
        
        return self._determine_exit_code(total_tasks, created, failed)


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
