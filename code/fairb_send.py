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

import datalad.api as dl
import pandas as pd
import numpy as np

def create_lockfiles(job_root):
    status_lockfile = (Path(job_root) / 'status_lockfile').absolute()
    push_lockfile = (Path(job_root) / 'push_lockfile').absolute()
    
    if not status_lockfile.exists():
        with open(status_lockfile, 'w') as lockfile:
            lockfile.write('')
        
    if not push_lockfile.exists():
        with open(push_lockfile, 'w') as lockfile:
            lockfile.write('')
    
    return str(status_lockfile), str(push_lockfile)

def create_status_csv(job_root):
    status_csv = (Path(job_root) / 'status.csv').absolute()
    if not status_csv.exists():
        status_df = pd.DataFrame({'job_name':[],'job_id':[],'host':[],'location':[],'req_disk_gb':[],'traceback':[],'status':[]})
        
        status_df.to_csv(status_csv, index=False)
    return str(status_csv)


def write_script(job_root, job_name, dl_cmd, container, commit, inputs, outputs, is_explicit, output_datasets, prereq_get, message, super_id, clone_target, push_target, ephemeral_location, req_disk_gb, status_lockfile, push_lockfile, status_csv):
    
    run_job_py = Path(job_root) / 'run_job.py'
    if not run_job_py.exists():
        raise Exception('run_job.py should be in the same directory as the config_job file.')
    else:
        run_job_py = str(run_job_py)
        
    script_path = str(Path(job_root) / f'job-{job_name}.sh')
    
    script = f"""
    #!/bin/bash
    
    python {run_job_py} --job_name {job_name} --dl_cmd '{dl_cmd}' --container {container} --commit {commit} --inputs {inputs} --outputs {outputs} --is_explicit {is_explicit} --output_datasets {output_datasets} --preget_inputs {prereq_get} --message {message} --super_ds_id {super_id} --clone_target {clone_target} --push_target {push_target} --ephemeral_location {ephemeral_location} --req_disk_gb {req_disk_gb} --status_lockfile {status_lockfile} --push_lockfile {push_lockfile} --status_csv {status_csv}
    """
    
    with open(script_path, 'w') as script_file:
        script_file.write(script)
    
    return script_path
    

def sendjob(queue, slots, vmem, h_rt, env_vars, script_path):
    
    
    if h_rt is None:
        h_rt = '24:00:00'
    if slots < 1 or slots is None:
        slots = 1
    
    slots=str(slots)
    vmem=str(vmem)
    
    cmd = ['qsub', '-q', queue, '-pe', 'smp', slots,  '-l', f'h_vmem={vmem}M', '-l', f'h_rt={h_rt}', '-cwd']
    
    
    if isinstance(env_vars, str):
        env_vars = json.loads(env_vars)   
        for env_var_name, env_var_value in env_vars.items():
            cmd += ['-v', f'{env_var_name}={env_var_value}']
    
    cmd+= ['-v', f"PATH={os.getenv('PATH')}"]

    cmd+= [script_path]
    
    subprocess.run(cmd)
    
    return cmd


if __name__ == '__main__':
    
    parser = ArgumentParser()
    parser.add_argument('-j','--jobs', nargs='+', help='<Required> Set flag', required=True)
    parser.add_argument('-c','--job_config', nargs=1, help='<Required> Set flag', required=True)
    args = parser.parse_args()
    
    jobs = args.jobs
    job_config = args.job_config[0]
    job_config = Path(job_config)
    job_root = str(Path(job_config.parent))
    
    if job_config.suffix == '.csv':
        job_config_df = pd.read_csv(job_config)
    elif job_config.suffix == '.tsv':
        job_config_df = pd.read_csv(job_config, sep='\t')
    else:
        raise Exception("Job config isn't a csv or tsv file.")
    
    job_config_cols = ['job_name','dl_cmd','container','commit','inputs','outputs','is_explicit','output_datasets','prereq_get','message','super_id','clone_target','push_target','ephemeral_location','req_disk_gb','queue','slots','vmem','h_rt','env_vars','batch']
    
    
    if [col for col in job_config_df.columns.to_list() if  col not in job_config_cols]:
        raise Exception("Not a valid job config file.")  
    
    if job_config_df['job_name'].shape[0] != len(job_config_df['job_name'].unique()):
         raise Exception("Job config has duplicated job names.")
    
    job_config_df = job_config_df.query("job_name.isin(@jobs)")
    
    job_config_df = job_config_df.replace(np.nan, None)
    
    
    status_lockfile, push_lockfile = create_lockfiles(job_root)
    
    status_csv = create_status_csv(job_root)
    


    for _index, job in job_config_df.iterrows():
        
        script_path = write_script(job_root, job['job_name'], job['dl_cmd'], job['container'], job['commit'], job['inputs'], job['outputs'], job['is_explicit'], job['output_datasets'], job['prereq_get'], job['message'], job['super_id'], job['clone_target'], job['push_target'], job['ephemeral_location'], job['req_disk_gb'], status_lockfile, push_lockfile, status_csv)
        
        
        
        sendjob(job['queue'], job['slots'], job['vmem'], job['h_rt'], job['env_vars'], script_path)