import os
import shutil
import asyncio
import aiofiles
import logging
import argparse
import subprocess
import pandas as pd
from glob import glob
from rucio.client import Client
from contextlib import asynccontextmanager

# Configure logging with timestamp and log level
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

class RucioClientPool:
    """
    A connection pool for Rucio clients to manage concurrent operations efficiently.
    Implements an async context manager pattern for safe client acquisition and release.
    """
    def __init__(self, size):
        """
        Initialize the client pool with a fixed number of Rucio clients.

        Args:
            size (int): Maximum number of concurrent Rucio clients in the pool
        """
        self.pool = asyncio.Queue(maxsize=size)
        for _ in range(size):
            self.pool.put_nowait(Client())

    @asynccontextmanager
    async def get_client(self):
        """
        Async context manager to safely acquire and release a Rucio client from the pool.

        Yields:
            Client: A Rucio client instance from the pool
        """
        client = await self.pool.get()
        try:
            yield client
        finally:
            await self.pool.put(client)

def init_proxy():
    """
    Initialize the VOMS proxy using certificate files from the secrets directory.

    This function:
    1. Validates the presence of exactly two .pem files (cert and key)
    2. Copies them to the current directory with standard names
    3. Sets appropriate permissions (400)
    4. Initializes the VOMS proxy
    5. Cleans up temporary certificate files

    Raises:
        ValueError: If certificate files are missing or incorrectly named
    """
    # Validate certs
    certs = glob('/etc/secrets/*.pem')
    if len(certs) != 2:
        raise ValueError('Only two pem files are expected')

    cert = ""
    key = ""
    for file in certs:
        if "cert" in file:
            cert = file
        elif "key" in file:
            key = file

    if not cert or not key:
        raise ValueError('Both cert and key files are expected')

    if 'userkey.pem' in certs and 'usercert.pem' in certs:
        raise ValueError('Only usercert.pem and userkey.pem are expected')

    # Copy files to current directory with standard names
    current_dir = os.getcwd()
    new_cert = os.path.join(current_dir, 'usercert.pem')
    new_key = os.path.join(current_dir, 'userkey.pem')

    shutil.copy2(cert, new_cert)
    shutil.copy2(key, new_key)

    # Set restricted read permissions
    os.chmod(new_cert, 0o400)
    os.chmod(new_key, 0o400)

    # Initialize VOMS proxy with a 192-hour validity period
    result = subprocess.run([
        'voms-proxy-init',
        '-voms', 'cms',
        '-rfc',
        '-valid', '192:00',
        '--cert', new_cert,
        '--key', new_key
    ], check=True, capture_output=True, text=True)

    if result.returncode != 0:
        raise ValueError(result.stderr)

    # Clean up temporary certificate files
    os.remove(new_cert)
    os.remove(new_key)

async def stuck_rule_task(client_pool, rule, reason, dry_run):
    """
    Asynchronously process a stuck replication rule by updating its state and adding a comment.

    Args:
        client_pool (RucioClientPool): Pool of Rucio clients for concurrent operations
        rule (str): The rule ID to be processed
        reason (str): Comment explaining why the rule was marked as stuck
        dry_run (bool): If True, only log actions without executing them
    """
    try:
        if not dry_run:
            async with client_pool.get_client() as client:
                client.update_replication_rule(rule, {'state':'STUCK', 'comment': reason})
                logging.info("Re-stucked Rule %s" % (rule))
        else:
            logging.info("Would re-stuck Rule %s" % (rule))
    except Exception as e:
        logging.error("Error set rule to stuck %s: %s" % (rule, e))

async def all_locks_ok_handler(file_path, dry_run=False):
    """
    Process rules with all locks in OK state by marking them as stuck for re-evaluation.

    Args:
        file_path (str): Path to file containing rule IDs
        dry_run (bool): If True, only log actions without executing them
    """
    tasks = []
    client_pool = RucioClientPool(size=10)
    reason = "Unsuspend tool: no pending locks, suspended rule to be re-evaluated"
    async with aiofiles.open(file_path, 'r') as f:
        async for line in f:
            task = asyncio.create_task(stuck_rule_task(client_pool, line.strip(), reason, dry_run))
            tasks.append(task)
    await asyncio.gather(*tasks)

async def declare_bad_replica_task(client_pool, record, reason, dry_run):
    """
    Declare a batch of replicas as bad in Rucio.

    Args:
        client_pool (RucioClientPool): Pool of Rucio clients for concurrent operations
        chunk (pd.DataFrame): Chunk of replica data to process
        reason (str): Reason for declaring replicas as bad
        dry_run (bool): If True, only log actions without executing them
    """
    try:
        name = record[0]
        rses = record[1].split(";")
        scope = "cms"
        if not dry_run:
            replicas = [{'scope': scope, 'name': name, 'rse': rse} for rse in rses]
            async with client_pool.get_client() as client:
                client.declare_bad_file_replicas(replicas, reason=reason, force=True)
                logging.info("Declared file %s as bad at %s" % (name, rses))
        else:
            logging.info("Would declare file %s as bad at %s" % (name, rses))
    except Exception as e:
        logging.error("Error declaring bad replicas %s at %s: %s" % (name, rses, e))

async def no_sources_handler(replicas_path, rules_path, dry_run=False):
    """
    Handle replicas with no available sources by declaring them as bad and marking associated rules as stuck.

    Args:
        replicas_path (str): Path to CSV file containing replica information
        rules_path (str): Path to file containing affected rule IDs
        dry_run (bool): If True, only log actions without executing them
    """
    bad_rep_tasks = []
    stuck_rules_tasks = []
    client_pool = RucioClientPool(size=10)
    reason = "Unsuspend tool: no sources error, unexistent available replica"

    # Load and prepare replica data
    df_replicas = pd.read_csv(replicas_path)
    df_replicas.columns = ["name", "rses"]

    # Load affected rule IDs
    rule_ids = []
    with open(rules_path) as f:
        rule_ids = f.read().splitlines()

    # Process replicas in chunks
    bad_rep_tasks = [asyncio.create_task(declare_bad_replica_task(client_pool, record, reason, dry_run)) for record in df_replicas.to_numpy()]
    await asyncio.gather(*bad_rep_tasks)

    # Process associated rules
    for id in rule_ids:
        task = asyncio.create_task(stuck_rule_task(client_pool, id.strip(), reason, dry_run))
        stuck_rules_tasks.append(task)

    await asyncio.gather(*stuck_rules_tasks)

if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', '-d', action='store_true', help='Perform a dry run')
    args = parser.parse_args()
    dry_run = args.dry_run

    loop = asyncio.get_event_loop()

    # Step 1: Run Spark job to analyze suspended rules
    logging.info('-----------------------------------Spark job-----------------------------------')
    logging.info("Starting suspended rules job")
    result = subprocess.run(['/unsuspend_tool/submit_suspended_rules_preparer.sh'], check=True, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("Error analyzing suspended rules: "+result.stderr)
        raise ValueError(result.stderr)

    # Step 2: Process rules with all locks OK
    logging.info('-----------------------------------All Locks OK Handler-----------------------------------')
    logging.info('Stucking suspended rules')
    init_proxy()
    loop.run_until_complete(all_locks_ok_handler('ok_suspended_rules.txt', dry_run=dry_run))

    # Step 3: Handle rules with no available sources
    logging.info('-----------------------------------No Sources Error Handler-----------------------------------')
    logging.info('Declaring bad replicas')
    loop.run_until_complete(no_sources_handler('no_sources_suspended_replicas.csv', 'no_sources_suspended_rules.txt', dry_run=dry_run))

    logging.info('-----------------------------------------------------------------------')