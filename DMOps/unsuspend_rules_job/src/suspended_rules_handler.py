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

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

class RucioClientPool:
    def __init__(self, size):
        self.pool = asyncio.Queue(maxsize=size)
        for _ in range(size):
            self.pool.put_nowait(Client())

    @asynccontextmanager
    async def get_client(self):
        client = await self.pool.get()
        try:
            yield client
        finally:
            await self.pool.put(client)

def init_proxy():
    """
    Validate the existence and names of the two PEM files in the /secrets/ directory,
    copy them to the current directory, set read permissions (400),
    and init proxy with the copied files.
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

    # Copy files to current directory
    current_dir = os.getcwd()
    new_cert = os.path.join(current_dir, 'usercert.pem')
    new_key = os.path.join(current_dir, 'userkey.pem')

    shutil.copy2(cert, new_cert)
    shutil.copy2(key, new_key)

    # Set read permissions (400) for both files
    os.chmod(new_cert, 0o400)
    os.chmod(new_key, 0o400)

    # Run voms-proxy-init with copied files
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

    os.remove(new_cert)
    os.remove(new_key)

async def stuck_rule_task(client_pool, rule, reason, dry_run):
    """
    Asynchronously unlocks and stuck rules from a queue. This is used for the suspended rules

    Args:
        queue (asyncio.Queue): The queue containing the stuck rules.
        dry_run (bool): Whether to perform a dry run or not.

    Returns:
        None
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

async def all_locks_ok_handler(file_path,dry_run=False):
    tasks = []
    client_pool = RucioClientPool(size=10)
    reason = "Unsuspend tool: no pending locks, suspended rule to be re-evaluated"
    async with aiofiles.open(file_path, 'r') as f:
        async for line in f:
            task = asyncio.create_task(stuck_rule_task(client_pool,line.strip(), reason, dry_run))
            tasks.append(task)
    await asyncio.gather(*tasks)

async def declare_bad_replica_task(client_pool, replicas, reason, dry_run):
    """
    Asynchronously declares bad replicas from a queue. This is used for the files with no available replicas

    Args:
        queue (asyncio.Queue): The queue containing the stuck rules.
        dry_run (bool): Whether to perform a dry run or not.

    Returns:
        None
    """

    try:
        if not dry_run:
            async with client_pool.get_client() as client:
                client.declare_bad_file_replicas(replicas,reason=reason)
                for replica in replicas:
                    logging.info("Declared file %s as bad at %s" % (replica['name'], replica['rse']))
        else:
            for replica in replicas:
                logging.info("Would declare file %s as bad at %s" % (replica['name'], replica['rse']))
    except Exception as e:
        logging.error("Error declaring bad replicas %s: %s" % (replicas, e))

async def no_sources_handler(file_path,dry_run=False):
    bad_rep_tasks = []
    stuck_rules_tasks = []
    client_pool = RucioClientPool(size=10)
    reason = "Unsuspend tool: no sources error, unexistent available replica"
    df = pd.read_csv(file_path)
    df.columns = ["name","rse","rule_id"]
    df["scope"] = "cms"
    chunk_size = 100
    num_chunks = (len(df) + chunk_size - 1) // chunk_size
    rule_ids = df["rule_id"].unique()
    df = df.drop(columns=["rule_id"])

    # Iterate over the chunks
    for i in range(num_chunks):
        # Get the chunk
        start = i * chunk_size
        end = min((i + 1) * chunk_size, len(df))
        chunk = df.iloc[start:end]

        # Convert the chunk to a list of dictionaries
        chunk_dict_list = chunk.to_dict('records')
        task = asyncio.create_task(declare_bad_replica_task(client_pool,chunk_dict_list, reason, dry_run))
        bad_rep_tasks.append(task)
    await asyncio.gather(*bad_rep_tasks)

    for id in rule_ids:
        task = asyncio.create_task(stuck_rule_task(client_pool,id, reason, dry_run))
        stuck_rules_tasks.append(task)

    await asyncio.gather(*stuck_rules_tasks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry_run', '-d', action='store_true', help='Perform a dry run')
    args = parser.parse_args()
    dry_run = args.dry_run

    loop = asyncio.get_event_loop()
    logging.info('-----------------------------------Spark job-----------------------------------')
    logging.info("Starting suspended rules job")
    result = subprocess.run(['/unsuspend_tool/submit_suspended_rules_preparer.sh'], check=True, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("Error analyzing suspended rules: "+result.stderr)
        raise ValueError(result.stderr)
    logging.info('-----------------------------------All Locks OK Handler-----------------------------------')

    logging.info('Stucking suspended rules')
    init_proxy()
    loop.run_until_complete(all_locks_ok_handler('ok_suspended_rules.txt', dry_run=dry_run))
    logging.info('-----------------------------------No Sources Error Handler-----------------------------------')

    logging.info('Declaring bad replicas')
    loop.run_until_complete(no_sources_handler('no_sources_suspended_files.csv', dry_run=dry_run))

    logging.info('-----------------------------------------------------------------------')