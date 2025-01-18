"""
Run job FAIRly in don_clusterio.
Author: Diego Ramírez González

See:
Wagner, A. S., Waite, L. K., Wierzba, M., Hoffstaedter, F., Waite, A. Q., Poldrack, B., ... & Hanke, M. (2022). FAIRly big: A framework for computationally reproducible processing of large-scale data. Scientific data, 9(1), 80.
"""

def main(args):

    from argparse import ArgumentParser
    import sys
    import os
    import shutil
    import subprocess
    from pathlib import Path
    import re
    from datetime import datetime

    from filelock import FileLock
    import datalad.api as dl
    import pandas as pd
    import numpy as np
    from fairb.core import FairB
    from fairb.utils.git import do_checkout, get_private_subdataset, git_add_remote, git_push


    parser = ArgumentParser()
    parser.add_argument('--job_name', type=str, help='Job name within job config file.', required=True)
    parser.add_argument('--fairb', type=str, help='Path to fairb project..', required=True)
    
    args = parser.parse_args(args)
    
    fairb = FairB.from_json(Path(args.fairb) / 'fairb.json')
    fairb.read_job_config()
    job_config = fairb.job_config_df.replace(np.nan, None).query("job_name == @args.job_name").iloc[0]
    
    job_name = args.job_name
    
    status_csv = fairb.job_status_file
    status_lockfile = fairb.status_lockfile
    super_ds_id = fairb.super_id
    clone_target = fairb.clone_target
    push_target = fairb.push_target
    push_lockfile = fairb.push_lockfile
    
    inputs = job_config.inputs
    outputs = job_config.outputs
    output_datasets = job_config.output_datasets
    preget_inputs = job_config.prereq_get
    
    if isinstance(inputs, str):
        inputs = inputs.split()
    if isinstance(outputs, str):
        outputs = outputs.split()
        
    if isinstance(output_datasets, str):
        output_datasets = output_datasets.split()
    else:
        output_datasets = []
        
    if isinstance(preget_inputs, str):
        preget_inputs = preget_inputs.split()
    else:
        preget_inputs = []
           
    is_explicit = job_config.is_explicit
    dl_cmd = job_config.dl_cmd
    commit = job_config.commit
    container = job_config.container
    message = job_config.message
    ephemeral_locations = job_config.ephemeral_location
    req_disk_gb = job_config.req_disk_gb

    job_id = os.getpid()  
    host = os.uname().nodename
    user= os.getenv('USER')
    
    if super_ds_id is None:
        raise Exception("No superdataset ID.")
    if dl_cmd is None:
        raise Exception("No datalad run command.")
    if clone_target is None:
        raise Exception("No clone target.")
    if push_target is None:
        raise Exception("No push target.")
    
    status_lock = FileLock(status_lockfile)
    push_lock = FileLock(push_lockfile)

    # Functions for disk space management
    def get_locations(location_list):
        """
        Return tmp and non_tmp locations from the list of location patterns.
        """
        
        tmp=[]
        not_tmp_patterns=[]
        not_tmp_locations=[]

        # tmp and non_tmp list
        for location in location_list:
            if location == '/tmp' or location == '/tmp/':
                tmp.append(location)
            else:
                not_tmp_patterns.append(location)
        
        # get non_tmp locations according to the non_tmp_patterns (the script can accept multiple location patterns)
        for not_tmp_pattern in not_tmp_patterns:
            
            for index, part in enumerate(Path(not_tmp_pattern).parts):
                if host in part:
                    break

            # node location
            mount_pattern = str(Path(*list(Path(not_tmp_pattern).parts[:index+1])))
            # location inside node
            after_pattern = str(Path(*list(Path(not_tmp_pattern).parts[index+1:])))
            
            # make sure those locations are within the node with the /etc/mtab file
            with open('/etc/mtab', 'r') as mtab:
                for line in mtab.readlines():
                    # which mount pattern is within the node
                    pattern = re.search(f'{mount_pattern} ', line)
                    # which directories (after mount pattern) are within that mount
                    if pattern:
                        pattern_glob = Path(pattern.group().strip()).glob(after_pattern) 
                    else:
                        continue
                    # after mount pattern could retrieve multiple locations
                    if pattern_glob: 
                        not_tmp_locations += [str(pg) for pg in pattern_glob]
        
        return tmp, not_tmp_locations


    def get_free_disk(location):
        """
        Return location's free disk space in gb.
        """
        
        _total, _used, free = shutil.disk_usage(location)
        # transform to gb
        return free // (2**30)


    def get_used_disk(location):
        """
        Return location's used disk space in gb.
        """
        
        _total, used, _free = shutil.disk_usage(location)
        # transform to gb
        return used // (2**30)


    def get_available_disk_resource(location, host, status_csv):
        """
        Return available disk space available (in gb).
        """
        
        total_req_disk_others_gb = (pd.read_csv(status_csv)
        .query("location == @location and status == 'ongoing' and host == @host")
        .assign(
            used_disk_gb = lambda df_: 
                df_['location'].apply(lambda x_: get_used_disk(x_)),
            req_disk_gb = lambda df_: 
                (df_['req_disk_gb'] - df_['used_disk_gb'])
        )
        .assign(
            req_disk_gb = lambda df_: 
                df_['req_disk_gb'].mask(df_['req_disk_gb'] < 0, 0)
        )
        ['req_disk_gb']
        .sum()
        )
            
        current_free_gb = get_free_disk(location)
        
        return current_free_gb - total_req_disk_others_gb


    def set_status(status_csv, job_name, job_id, req_disk_gb, host, location, job_dir, status, start):
        """
        Add a new job status.
        """
        
        status_df = pd.read_csv(status_csv)
        
        new_status = {
            'job_name':[job_name],
            'job_id':[job_id],
            'req_disk_gb':[req_disk_gb],
            'host':[host],
            'location':[location],
            'job_dir':[job_dir],
            'status':[status],
            'start':[start],
            'update':[None],
            'traceback':[None]
            }
        
        new_status = pd.DataFrame(new_status)
        
        status_df = pd.concat([status_df, new_status])
        
        status_df.to_csv(status_csv, index=False)
        
        return status_df


    def update_status(status_csv, job_name, job_id, host, location, status, update):
        """
        Update an existing job status.
        """
        
        status_df = pd.read_csv(status_csv)
        
        is_job = (
        (status_df['job_name'] == job_name) &
        (status_df['job_id'] == job_id) &
        (status_df['host'] == host) &
        (status_df['location'] == location) 
        )
        
        status_df = (status_df
        .assign(
            status = lambda df_: df_['status'].mask(is_job, status),
            update = lambda df_: df_['update'].mask(is_job, update)
            # traceback = lambda df_: df_['traceback'].mask(is_job, traceback)
            )
        )
        
        status_df.to_csv(status_csv, index=False)
        
        return status_df

    # cleanup and exception handling
    def cleanup(job_dir):
        subprocess.run(['chmod', '-R', '+w', job_dir])
        subprocess.run(['rm', '-rf', job_dir])

        
    #######################
    # Resource management #
    #######################
    if ephemeral_locations is None:
        ephemeral_locations = ['/tmp']
        
    tmp, not_tmp_locations = get_locations(ephemeral_locations)

    # manage available disk space
    if req_disk_gb is None:
        req_disk_gb = 0
    elif req_disk_gb < 0:
        req_disk_gb = 0
        
    with status_lock:
        
        found_location=False
        
        if tmp:
            tmp = '/tmp'
            available_disk = get_available_disk_resource(tmp, host, status_csv)
            if req_disk_gb < available_disk:
                found_location=True
                location=tmp
                
        
        elif not_tmp_locations and not found_location:
            not_tmp_df = (
                pd.DataFrame({'location':not_tmp_locations})
                .assign(available_disk = lambda df_: 
                    df_['location'].apply(lambda x_: get_available_disk_resource(x_, host, status_csv))
                    )
                .sort_values('free_space', ascending=False)
                )

            if req_disk_gb < not_tmp_df['available_disk'].iat[0]:
                found_location = True
                location = not_tmp_df['location'].iat[0]
                
                
        if found_location:
            job_dir = str(Path(location) / f'{job_name}_{user}')
            set_status(status_csv, job_name, job_id, req_disk_gb, host, location, job_dir, status='ongoing', start=datetime.today().strftime("%Y/%m/%d %H:%M:%S"))
        else:
            set_status(status_csv, job_name, job_id, req_disk_gb, host, location=None, job_dir=None, status='no-space', start=datetime.today().strftime("%Y/%m/%d %H:%M:%S"))
            raise Exception("Coulnd't find a place with enough disk space.")


    def excepthook(exctype, value, tb):
        
        try:
            cleanup(job_dir)
        except:
            pass
        
        # error_msg = f'{exctype} {value}'
        
        with status_lock:
            update_status(status_csv, job_name, job_id, host, location, status='error', update=datetime.today().strftime("%Y/%m/%d %H:%M:%S"))
            
        print('Type:', exctype)
        print('Value:', value)
        print('Traceback:', tb)


    sys.excepthook = excepthook

    ########################
    #        CLONE         #
    ########################

    print(host)

    # Clone input ria and create ephemeral dataset, then change directory to it

    # Set superdataset and subdatasets as private repositories, so that their output keys
    # and their ds uuids are not stored within the output_ria's git-annex branch.
    # Note: `git annex dead here` only prevents storing the ephemeral clone's file keys, but not its uuid. 
    
    # remove ria prefix if necessary
    try:
        clone_ria_prefix = re.search(r'ria\+\w+:\/{2}', clone_target).group()
        clone_target = clone_target.replace(clone_ria_prefix, '')
    except:
        # assume ria requires a file protocol if no protocol in the job_config
        clone_ria_prefix = 'ria+file://'
    try:
        push_target = re.sub(r'ria\+\w+:\/{2}', '', push_target)
    except:
        pass
        
    
    super_clone_target = f'{clone_ria_prefix}{clone_target}#{super_ds_id}'

    print("Cloning superdataset.")
    dl.clone(source=super_clone_target, path=job_dir, git_clone_opts=['-c annex.private=true'])
    print("Change working directory to superdataset clone.")
    os.chdir(job_dir)

    push_path = str(Path(push_target) / Path(super_ds_id[:3]) / Path(super_ds_id[3:]))
    
    print("Add git remote.")
    git_add_remote(push_path, 'cwd')

    ds = dl.Dataset(job_dir)
    sd = pd.DataFrame(ds.subdatasets())
    
    # input_datasets = sd.query('not gitmodule_name.isin(@output_datasets)')['gitmodule_name']

    # for input_dataset in input_datasets:
    #     dl.get(input_dataset, get_data=False)

    if output_datasets and not (pd.Series(output_datasets).isin(sd['gitmodule_name']).all()):
        raise Exception("Not all output datasets are found.")
    
    # Get output datasets if any.
    # Right now, this solution assumes output subdatasets don't have subdatasets themselves.
    # The next release of datalad should include the `--reckless private` option for both
    # clone and get. This will also have issues if the subdataset at the clone target is not at the same branch as the superdataset. 
    # If one doesn't mind storing an uuid for each job, then `git annex dead here` might be a better option for now if the above things are an issue.

    print("Clone output subdatasets if any.")
    for output_dataset in output_datasets:
        sd_id = sd.query("gitmodule_name == @output_dataset")['gitmodule_datalad-id'].iat[0]
        get_private_subdataset(clone_target, output_dataset, sd_id)
        
        push_path = str(Path(push_target) / Path(sd_id[:3]) / Path(sd_id[3:]))
        git_add_remote(push_path, output_dataset)
        
    
    if not Path('outputs').exists():
        Path('outputs').mkdir()
        
    # Checkout to job branch
    print("Checkout branch.")
    branch_name = f'{job_name}'
    for output_dataset in output_datasets:
        do_checkout(branch_name, output_dataset)
    do_checkout(branch_name, 'cwd')

    # Preget inputs
    for preget_input in preget_inputs:
        dl.get(preget_input)

    ###############################
    #       DATALAD RUN JOB       #
    ###############################
    print("Run command.")
    if message is None:
        message = branch_name
    

    if commit is not None:
        dl.rerun(
            revision=commit,
            explicit=is_explicit
        )
        
    elif container is not None:
        dl.containers_run(
            dl_cmd,
            container_name=container,
            inputs=inputs,
            outputs=outputs,
            message=message,
            explicit=is_explicit
        )
        
    else:
        dl.run(
            dl_cmd,
            inputs=inputs,
            outputs=outputs,
            message=message,
            explicit=is_explicit
        )

    ###############################
    #        PUSH RESULTS         #
    ###############################
        

    print("Push back results.")
    # push annex data
    dl.push(
        dataset='.',
        to='output_ria-storage',
    )

    for output_dataset in output_datasets:
        dl.push(
            dataset=output_dataset,
            to='output_ria-storage',
        )
        
    # push git data
    with push_lock:
        git_push('cwd')
        for output_dataset in output_datasets:
            git_push(output_dataset)
        
        

    ###############################
    #         CLEAN DISK          #
    ###############################

    print("Delete ephemeral clone.")
    cleanup(job_dir)

    with status_lock:
        
        update_status(status_csv, 
                      job_name, 
                      job_id, 
                      host, 
                      location, 
                      status='completed', 
                      update=datetime.today().strftime("%Y/%m/%d %H:%M:%S")
                      )

    print("Job completed succesfully.")