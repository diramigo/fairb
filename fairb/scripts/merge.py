import os
from argparse import ArgumentParser
from pathlib import Path

import datalad.api as dl
import pandas as pd
from fairb.core import FairB
from fairb.utils.git import do_checkout, get_private_subdataset, git_add_remote, git_push, git_merge, git_annex_fsck, git_commit, git_add, datalad_push_data_nothing, git_commit_amend

def main(args):
    
    parser = ArgumentParser()
    parser.add_argument('-c', '--fairb', type=str, required=True)
    parser.add_argument('-m', '--move_files', action='store_true', required=False)
    parser.add_argument('--git_rm', nargs='+', type=str, required=False)
    parser.add_argument('--git_rm_except_one', action='store_true', required=False)
    args = parser.parse_args(args)
    
    # read fairb project
    fairb = FairB.from_json(Path(args.fairb) / 'fairb.json')
    fairb.read_job_config()
    fairb.read_job_status()
    job_branches = fairb.get_completed_jobs()
    remote_job_branches = ['remotes/origin/'+job_branch for job_branch in job_branches]
    
    # assert git_rm before creating clones
    ## TODO: empirically test git_rm argument
    if args.git_rm:
        for git_rm in args.git_rm:
            git_rm = git_rm.split()
            assert len(git_rm) == 2, Exception("each git rm must be a string containing: dataset_relative_path glob_pattern")
            if git_rm != '.':
                assert git_rm[0] in fairb.output_datasets, Exception("git rm output_dataset doesn't exist.")
    
    # create temporary output_ria clone (super and output datasets)
    clone_ria_prefix = 'ria+file://'
    tmp_output_ds = (Path(args.fairb) / 'tmp_output').absolute()

    super_clone_target = f'{clone_ria_prefix}{fairb.push_target}#{fairb.super_id}'
    dl.clone(source=super_clone_target, path=tmp_output_ds, git_clone_opts=['-c annex.private=true'])
    
    ds = dl.Dataset(tmp_output_ds)
    sd = pd.DataFrame(ds.subdatasets())
    
    os.chdir(tmp_output_ds)

    if fairb.output_datasets:
        for output_dataset in fairb.output_datasets:
            sd_id = sd.query("gitmodule_name == @output_dataset")['gitmodule_datalad-id'].iat[0]
            get_private_subdataset(fairb.push_target, output_dataset, sd_id)
            
    # if any git_rm, perform git rm + git commit --ammend --no-edit
    if args.git_rm:
        for git_rm_args in args.git_rm:
            git_rm_args = git_rm_args.split()
            git_rm(git_rm_args[1], git_rm_args[0])
            git_commit_amend(git_rm_args[0])
    
    # octopus merge branches of completed jobs (super and output datasets) to current batch branch
    # git push, git-annex fsck and datalad push --data nothing (super and output datasets)
    merge_branch = f'batch-{fairb.current_batch}'
    merge_message = f'merge {len(job_branches)} jobs from batch-{fairb.current_batch}'

    # output_subdatasets
    for output_dataset in fairb.output_datasets:
        do_checkout(merge_branch, output_dataset)
        git_merge(remote_job_branches, merge_message, output_dataset)
        git_push(output_dataset, repository='origin',set_upstream_branch_name=merge_branch)
        git_annex_fsck(output_dataset)
        datalad_push_data_nothing(output_dataset)
        
    # output_super_dataset
    if fairb.output_datasets:
        for job_branch in job_branches:
            do_checkout(job_branch, create_branch=False)
            git_add('outputs/*')
            git_commit(message='update submodules')
            git_push(repository='origin', force=True)
        
        do_checkout('master', create_branch=False)

    do_checkout(merge_branch)
    git_merge(remote_job_branches, merge_message)
    git_push(repository='origin',set_upstream_branch_name=merge_branch)
    git_annex_fsck()
    datalad_push_data_nothing()

    # if move_file, git-annex mv
    
    
    