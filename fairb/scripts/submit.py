"""
Send multiple FAIR jobs across don_clusterio.
Author: Diego Ramírez González

See:
Wagner, A. S., Waite, L. K., Wierzba, M., Hoffstaedter, F., Waite, A. Q., Poldrack, B., ... & Hanke, M. (2022). FAIRly big: A framework for computationally reproducible processing of large-scale data. Scientific data, 9(1), 80.
"""

import os
from argparse import ArgumentParser
from pathlib import Path
import subprocess
import json
import sys

import datalad.api as dl
import pandas as pd
import numpy as np
from fairb.core import FairB


def write_script(job_name, fairb_path, job_root=None):
    """
    Write a bash script for fairb run of one job.
    """
    if job_root is None:
        job_root = Path(fairb_path) / 'code'
        job_root.mkdir(exist_ok=True)
     
    script_path = str(Path(job_root) / f'job-{job_name}.sh')
    
    fairb_path = str(Path(fairb_path).absolute())
    
    script = f"""
    #!/bin/bash
    
    fairb run --job_name {job_name} --fairb {fairb_path}
    """
    
    with open(script_path, 'w') as script_file:
        script_file.write(script)
    
    return script_path
    

def sendjob(queue, slots, vmem, h_rt, env_vars, script_path):
    "Submit job to the queue."
    
    # set defaults
    if h_rt is None:
        h_rt = '24:00:00'
    if slots < 1 or slots is None:
        slots = 1
    
    # basic features of command
    cmd = ['qsub', '-q', queue, '-pe', 'smp', str(slots)]
    
    if vmem is None:
        pass
    elif vmem >= 0:
        cmd += ['-l', f'h_vmem={vmem}M']
    
    cmd += ['-l', f'h_rt={h_rt}', '-cwd']
    
    # add path as an environmental variables
    cmd+= ['-v', f"PATH={os.getenv('PATH')}"]
    
    # add other environmental variables
    if isinstance(env_vars, str):
        env_vars = json.loads(env_vars)   
        for env_var_name, env_var_value in env_vars.items():
            cmd += ['-v', f'{env_var_name}={env_var_value}']

    # add script path as the last argument
    cmd+= [script_path]
    
    # run command
    subprocess.run(cmd)
    
    return cmd


def main(args):
    
    parser = ArgumentParser(
        description="Send fairb jobs that have been designed in the job_config file (SGE cluster only)."
    )
    
    # arguments number of jobs
    njobs = parser.add_mutually_exclusive_group()
    njobs.add_argument('-j','--jobs', nargs='+', help="Submit a given job by it's name if it's available.")
    njobs.add_argument('-n','--njobs', type=int, help="Submit a given number of available jobs.")
    njobs.add_argument('-a','--all', action='store_true', help="Submit all available jobs.")
    
    parser.add_argument('-c','--fairb', type=str, help="Path to the fairb project containing the fairb.json file. Defaults to the current working directory", default='.')
    args = parser.parse_args(args)
    

    # read fairb project    
    fairb_json = Path(args.fairb) / 'fairb.json'
    fairb_project = FairB.from_json(fairb_json)
    fairb_project.read_job_config()
    fairb_project.read_job_status()
        
    # get jobs
    available_jobs = fairb_project.get_available_jobs()
    
    if args.jobs:
        jobs = [job
                for job in args.jobs
                if job in available_jobs]
    else:
        if args.njobs:
            jobs = available_jobs[:args.njobs]
        else:
            jobs = available_jobs
                   
    job_config_df = (fairb_project.job_config_df
        .query("job_name.isin(@jobs)")
        .replace(np.nan, None)
        )
    
    # create lockfiles
    status_lockfile, push_lockfile = fairb_project._create_lockfiles()
    
    # create scripts and submit jobs
    for _index, job in job_config_df.iterrows():
        
        script_path = write_script(job['job_name'], args.fairb)
        
        
        sendjob(job['queue'], job['slots'], job['vmem'], job['h_rt'], job['env_vars'], script_path)



