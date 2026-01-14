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
DEFAULT_CONCURRENT_SESSIONS = 50
DEFAULT_MAX_RETRIES = 3
DEFAULT_STATUS_TIMEOUT_SECONDS = 5
DEFAULT_RETRY_DELAY_SECONDS = 1
MAX_IDLE_ITERATIONS = 10  # Max iterations with no progress before warning


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
        # Define all expected sections
        expected_sections = {'name', 'deephaven', 'execution', 'replay', 'scheduler', 'dates', 'env'}
        
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
            'execution': ['worker_script', 'num_workers'],
            'replay': ['heap_size_gb'],
            'dates': ['start', 'end']
        }
        
        allowed_fields = {
            'deephaven': {'connection_url', 'auth_method', 'username', 'password', 'private_key_path'},
            'execution': {'worker_script', 'num_workers', 'max_concurrent_sessions', 'max_retries'},
            'replay': {
                'heap_size_gb', 'init_timeout_minutes',
                'replay_time', 'replay_speed', 'sorted_replay',
                'buffer_rows', 'replay_timestamp_columns',
                'script_language', 'jvm_profile'
            },
            'scheduler': {'calendar', 'start_time', 'stop_time', 'timezone', 'business_days'},
            'dates': {'start', 'end', 'weekdays_only'},
            'env': None  # env section can have arbitrary user-defined variables
        }
        
        # Validate each section
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
        
        # Log scheduler configuration status
        if 'scheduler' in config:
            logger.debug("Scheduler configuration found - PQ will use scheduled start/stop times")
        else:
            logger.debug("No scheduler configuration - PQ will run immediately without time constraints")
        
        # Validate numeric fields with bounds
        if 'replay_speed' in config['replay']:
            speed = config['replay']['replay_speed']
            if speed < 1.0:
                raise ValueError(f"replay_speed must be >= 1.0 for backtesting (got {speed})")
            if speed > 100.0:
                raise ValueError(f"replay_speed too high (got {speed}, max 100).")
        
        if 'heap_size_gb' in config['replay']:
            heap = config['replay']['heap_size_gb']
            if heap <= 0:
                raise ValueError(f"heap_size_gb must be > 0 (got {heap})")
            if heap > 512:
                raise ValueError(f"heap_size_gb too high (got {heap}, max 512)")
        
        if 'num_workers' in config['execution']:
            workers = config['execution']['num_workers']
            if workers <= 0:
                raise ValueError(f"num_workers must be > 0 (got {workers})")
            if workers > 1000:
                raise ValueError(f"num_workers too high (got {workers}, max 1000)")
        
        if 'max_concurrent_sessions' in config['execution']:
            concurrent = config['execution']['max_concurrent_sessions']
            if concurrent <= 0:
                raise ValueError(f"max_concurrent_sessions must be > 0 (got {concurrent})")
            if concurrent > 1000:
                raise ValueError(f"max_concurrent_sessions too high (got {concurrent}, max 1000)")
        
        # Validate script_language if specified
        if 'script_language' in config['replay']:
            lang = config['replay']['script_language']
            if lang not in ['Python', 'Groovy']:
                raise ValueError(f"script_language must be 'Python' or 'Groovy' (got '{lang}')")
        
        # Validate replay_timestamp_columns if specified
        if 'replay_timestamp_columns' in config['replay']:
            ts_cols = config['replay']['replay_timestamp_columns']
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
        
        # Validate scheduler section if present - all fields are required
        if 'scheduler' in config:
            required_scheduler_fields = {'calendar', 'start_time', 'stop_time', 'timezone', 'business_days'}
            actual_scheduler_fields = set(config['scheduler'].keys())
            missing_scheduler = required_scheduler_fields - actual_scheduler_fields
            if missing_scheduler:
                raise ValueError(f"scheduler section missing required fields: {', '.join(sorted(missing_scheduler))}")
            unexpected_scheduler = actual_scheduler_fields - required_scheduler_fields
            if unexpected_scheduler:
                raise ValueError(f"scheduler section has unexpected fields: {', '.join(sorted(unexpected_scheduler))}")
        
        # Validate date format
        try:
            datetime.strptime(config['dates']['start'], '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"dates.start must be in YYYY-MM-DD format: {e}")
        try:
            datetime.strptime(config['dates']['end'], '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"dates.end must be in YYYY-MM-DD format: {e}")
    
    def _handle_shutdown_signal(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.warning("\nShutdown signal received. Finishing current operations...")
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
        config_msg = PersistentQueryConfigMessage()
        
        # Basic fields
        config_msg.serial = -2**63
        config_msg.version = 1
        config_msg.configurationType = "ReplayScript"
        config_msg.name = f"replay_{self.config['name']}_{date.replace('-', '')}_{worker_id}"
        config_msg.owner = self.config['deephaven']['username']
        config_msg.enabled = True
        config_msg.heapSizeGb = self.config['replay']['heap_size_gb']
        config_msg.bufferPoolToHeapRatio = 0.25
        config_msg.detailedGCLoggingEnabled = True
        config_msg.scriptLanguage = self.config['replay'].get('script_language', 'Python')
        config_msg.jvmProfile = self.config['replay'].get('jvm_profile', 'Default')
        config_msg.workerKind = "DeephavenCommunity"
        
        # Initialization timeout (default: 60 seconds)
        init_timeout_minutes = self.config['replay'].get('init_timeout_minutes', 1)
        config_msg.timeoutNanos = int(init_timeout_minutes * 60 * 1_000_000_000)
        
        # Load script content (cached)
        if self.worker_script_content is None:
            worker_script_path = Path(self.config['execution']['worker_script'])
            
            # Validate and resolve worker script path
            if worker_script_path.is_absolute():
                worker_script = worker_script_path.resolve()
            else:
                worker_script = (self.config_path.parent / worker_script_path).resolve()
            
            # Security: Ensure script is under config directory or absolute trusted path
            if not worker_script_path.is_absolute():
                try:
                    worker_script.relative_to(self.config_path.parent)
                except ValueError:
                    raise ValueError(f"Worker script path escapes config directory: {worker_script}")
            
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
        
        # Replay-specific fields (JSON)
        replay_fields = {
            "replayTimeType": "fixed",
            "sortedReplay": self.config['replay'].get('sorted_replay', True),
            "replayTime": self.config['replay'].get('replay_time', '09:30:00'),
            "replayDate": date,
            "replaySpeed": self.config['replay'].get('replay_speed', 1.0),
        }
        config_msg.typeSpecificFieldsJson = json.dumps(replay_fields)
        
        # Environment variables
        env_vars = [
            f"SIMULATION_NAME={self.config['name']}",
            f"SIMULATION_DATE={date}",
            f"WORKER_ID={worker_id}",
            f"NUM_WORKERS={self.config['execution']['num_workers']}"
        ]
        
        for key, value in self.config['env'].items():
            env_vars.append(f"{key}={value}")
        
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
        
        # Scheduler parameters - only set if scheduler section exists
        if 'scheduler' in self.config:
            scheduling = GenerateScheduling.generate_daily_scheduler(
                start_time=self.config['scheduler']['start_time'],
                stop_time=self.config['scheduler']['stop_time'],
                time_zone=self.config['scheduler']['timezone'],
                business_days=self.config['scheduler']['business_days'],
                calendar=self.config['scheduler']['calendar'],
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
            
        except (ConnectionError, TimeoutError, ValueError) as e:
            logger.error(f"Failed to create session: date={date}, worker={worker_id}, error={e}")
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
            'running', 'completed', 'failed', 'other', or None if not found
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
        
        if self.session_mgr.controller_client.is_running(status):
            return 'running'
        elif self.session_mgr.controller_client.is_terminal(status):
            status_name = self.session_mgr.controller_client.status_name(status)
            if status_name == 'PQS_COMPLETED':
                return 'completed'
            else:
                return 'failed'
        else:
            logger.warning(f"Unexpected session status 'other' for session {session_key}")
            return 'other'
    
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
                    logger.error(f"Session failed: date={date}, worker={worker_id}, status={status_name}")
                else:
                    logger.error(f"Session failed: date={date}, worker={worker_id}")
        
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
            except Exception:
                return {}, map_version
    
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
        max_concurrent = self.config['execution'].get('max_concurrent_sessions', DEFAULT_CONCURRENT_SESSIONS)
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
            
            # Track progress to detect stalls
            current_progress = created + completed + failed
            if current_progress == last_progress_count:
                idle_iterations += 1
                if idle_iterations >= MAX_IDLE_ITERATIONS:
                    logger.warning(
                        f"No progress for {MAX_IDLE_ITERATIONS} iterations. "
                        f"Created: {created}, Active: {len(active_sessions)}, Pending: {len(pending_tasks)}, "
                        f"Completed: {completed}, Failed: {failed}"
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
