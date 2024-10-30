"""
Run job FAIRly in don_clusterio.
Author: Diego Ramírez González

See:
Wagner, A. S., Waite, L. K., Wierzba, M., Hoffstaedter, F., Waite, A. Q., Poldrack, B., ... & Hanke, M. (2022). FAIRly big: A framework for computationally reproducible processing of large-scale data. Scientific data, 9(1), 80.
"""

if __name__ == '__main__':

    from argparse import ArgumentParser
    import sys
    import os
    import shutil
    import subprocess
    from pathlib import Path
    import re
    from filelock import FileLock
    from datetime import datetime

    import datalad.api as dl
    import pandas as pd


    parser = ArgumentParser()
    parser.add_argument('--job_name', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--status_csv', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--status_lockfile', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--super_ds_id', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--clone_target', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--push_target', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--push_lockfile', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--inputs', nargs='+', help='<Required> Set flag', required=True)
    parser.add_argument('--outputs', nargs='+', help='<Required> Set flag', required=True)
    parser.add_argument('--output_datasets', nargs='+', help='<Required> Set flag', required=True)
    parser.add_argument('--preget_inputs', nargs='+', help='<Required> Set flag', required=True)
    parser.add_argument('--is_explicit', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--dl_cmd', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--commit', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--container', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--message', nargs=1, help='<Required> Set flag', required=True)
    parser.add_argument('--ephemeral_locations', nargs='+', help='<Required> Set flag', required=True)
    parser.add_argument('--req_disk_gb', nargs=1, help='<Required> Set flag', required=True)

    args = parser.parse_args()
    job_name = args.job_name[0]
    job_id = os.getpid()   
    status_csv = args.status_csv[0]
    status_lockfile = args.status_lockfile[0]
    super_ds_id = args.super_ds_id[0]
    clone_target = args.clone_target[0]
    push_target = args.push_target[0]
    push_lockfile = args.push_lockfile[0]
    inputs = args.inputs
    outputs = args.outputs
    output_datasets = args.output_datasets
    preget_inputs = args.preget_inputs
    is_explicit = bool(args.is_explicit[0])
    dl_cmd = args.dl_cmd[0]
    commit = args.commit[0]
    container = args.container[0]
    message = args.message[0]
    ephemeral_locations = args.ephemeral_locations
    req_disk_gb = args.req_disk_gb[0]

    host = os.uname().nodename
    user= os.getenv('USER')
    
    if job_name is None:
        job_name == 'test'
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


    def update_status(status_csv, job_name, job_id, host, location, status, update, traceback=None):
        """
        Update an existing job status.
        """
        
        status_df = pd.read_csv(status_csv)
        
        is_job = (
        (status_df['job_name'] == job_name) &
        (status_df['job_id'].astype('string') == job_id) &
        (status_df['host'] == host) &
        (status_df['location'] == location) 
        )
        
        status_df = (status_df
        .assign(
            status = lambda df_: df_['status'].mask(is_job, status),
            update = update,
            traceback = lambda df_: df_['traceback'].mask(is_job, traceback)
            )
        )
        
        status_df.to_csv(status_csv, index=False)
        
        return status_df


    # Functions for cloning and checking out
    def do_dead_annex(dpath='cwd'):
        """
        Set cwd as dead annex or submodules as dead annex.
        """
        if dpath == 'cwd':
            cmd = ['git', 'annex', 'dead', 'here']
        else: 
            cmd = ['git', 'submodule', 'foreach', '--recursive', 'git', 'annex', 'dead', 'here']
        subprocess.run(cmd)
        

    def do_checkout(job_name, dpath='cwd'):
        """
        Change to a job branch.
        """
        if dpath == 'cwd':
            cmd = ['git', 'checkout', '-b', job_name]
        else:
            cmd = ['git', '-C', dpath ,'checkout', '-b', job_name]
        
        subprocess.run(cmd)

        
    def get_private_subdataset(clone_target, sd_path, sd_id):
        # Assume clone_target is a RIA store
        clone_path = str(Path(clone_target) / Path(sd_id[:3]) / Path(sd_id[3:]))
        
        git_clone_command = ['git', 'clone', clone_path, sd_path]
        subprocess.run(git_clone_command)
        
        git_config_annex_private = ['git', '-C', sd_path, 'config', 'annex.private', 'true']
        subprocess.run(git_config_annex_private)
        
        git_annex_init = ['git', '-C', sd_path, 'annex', 'init']
        subprocess.run(git_annex_init)


    def git_add_remote(push_path, dpath='cwd'):
        if dpath == 'cwd':
            cmd = ['git', 'remote', 'add', 'outputstore', push_path]
        else:
            cmd = ['git', '-C', dpath, 'remote', 'add', 'outputstore', push_path]
            
        subprocess.run(cmd)

    def git_push(dpath='cwd'):
        if dpath == 'cwd':
            cmd = ['git', 'push', 'outputstore']
        else:
            cmd = ['git', '-C', dpath, 'push', 'outputstore']
        
        subprocess.run(cmd)


    # cleanup and exception handling
    def cleanup(job_dir):
        subprocess.run(['chmod', '-R', '+w', job_dir])
        subprocess.run(['rm', '-rf', job_dir])

    def excepthook(exctype, value, tb):
        
        with status_lock:
            update_status(status_csv, job_name, job_id, host, location, status='error', update=datetime.today().strftime("%Y/%m/%d %H:%M:%S"), traceback=tb)
            
        try:
            cleanup(job_dir)
        except:
            pass
        
        print('Type:', exctype)
        print('Value:', value)
        print('Traceback:', tb)


    sys.excepthook = excepthook
        
    #######################
    # Resource management #
    #######################
    if ephemeral_locations is None or None in ephemeral_locations:
        ephemeral_locations = ['/tmp']
        
    tmp, not_tmp_locations = get_locations(ephemeral_locations)

    # manage available disk space
    if req_disk_gb is None or req_disk_gb < 0:
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
            job_dir = str(Path(location) / f'job-{job_name}-{user}')
            set_status(status_csv, job_name, job_id, req_disk_gb, host, location, job_dir, status='ongoing', start=datetime.today().strftime("%Y/%m/%d %H:%M:%S"))
        else:
            set_status(status_csv, job_name, job_id, req_disk_gb, host, location, job_dir=None, status='no-space', start=datetime.today().strftime("%Y/%m/%d %H:%M:%S"))
            raise Exception("Coulnd't find a place with enough disk space.")


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

    dl.clone(source=super_clone_target, path=job_dir, git_clone_opts=['-c annex.private=true'])
    os.chdir(job_dir)

    push_path = str(Path(push_target) / Path(super_ds_id[:3]) / Path(super_ds_id[3:]))
    git_add_remote(push_path, 'cwd')

    ds = dl.Dataset(job_dir)
    sd = pd.DataFrame(ds.subdatasets())

    if output_datasets is None or None in output_datasets:
        output_datasets = []

    if output_datasets and  (pd.Series(output_datasets).isin(sd['gitmodule_name']).all()):
        raise Exception("Not all output datasets are found.")

    # Get output datasets if any.
    # Right now, this solution assumes output subdatasets don't have subdatasets themselves.
    # The next release of datalad should include the `--reckless private` option for both
    # clone and get. This will also have issues if the subdataset at the clone target is not at the same branch as the superdataset. 
    # If one doesn't mind storing an uuid for each job, then `git annex dead here` might be a better option for now if the above things are an issue.


    for output_dataset in output_datasets:
        sd_id = sd.query("gitmodule_name == @output_dataset")['gitmodule_datalad-id'].iat[0]
        get_private_subdataset(clone_target, output_dataset, sd_id)
        
        push_path = str(Path(push_target) / Path(sd_id[:3]) / Path(sd_id[3:]))
        git_add_remote(push_path, output_dataset)
        
        
    # Checkout to job branch
    branch_name = f'job-{job_name}'
    for output_dataset in output_datasets:
        do_checkout(output_dataset, branch_name)
    do_checkout(branch_name, 'cwd')

    # Preget inputs
    if preget_inputs is None or None in preget_inputs:
        preget_inputs = []
    for preget_input in preget_inputs:
        dl.get(preget_input)

    ###############################
    #       DATALAD RUN JOB       #
    ###############################

    if message is None:
        message = branch_name

    if commit:
        dl.rerun(
            revision=commit,
            explicit=is_explicit
        )
        
    elif container:
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
        

    # push annex data
    dl.push(
        path='.',
        to='output_ria-storage',
    )

    for output_dataset in output_datasets:
        dl.push(
            path=output_dataset,
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

    cleanup(job_dir)

    with status_lock:
        
        update_status(status_csv, 
                      job_name, 
                      job_id, 
                      host, 
                      location, 
                      status='completed', 
                      update=datetime.today().strftime("%Y/%m/%d %H:%M:%S"), 
                      traceback=None
                      )

    print("Job completed succesfully.")