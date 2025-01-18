from pathlib import Path
import subprocess

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
    

def do_checkout(branch_name, dpath='cwd', create_branch=True):
    """
    Create a new git branch.
    """
    cmd = ['git']
    if dpath != 'cwd':
        cmd += ['-C', dpath]
    cmd += ['checkout']
    if create_branch:
        cmd += ['-b']
    cmd += [branch_name]

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


def git_add_remote(push_path, dpath='cwd', repository='outputstore'):
    if dpath == 'cwd':
        cmd = ['git', 'remote', 'add', repository, push_path]
    else:
        cmd = ['git', '-C', dpath, 'remote', 'add', repository, push_path]
        
    subprocess.run(cmd)

def git_push(dpath='cwd', repository='outputstore', set_upstream_branch_name=None, force=False):
    cmd = ['git']
    if dpath != 'cwd':
        cmd += ['-C', dpath]
    cmd += ['push']
    if set_upstream_branch_name is not None:
        cmd += ['--set-upstream']
    cmd += [repository]
    if set_upstream_branch_name is not None:
        cmd += [set_upstream_branch_name]
    if force:
        cmd += ['--force']
        
    subprocess.run(cmd)
    
def git_rm(glob_pattern, dpath='cwd'):
    if dpath == 'cwd':
        cmd = ['git', 'rm', glob_pattern]
    else:
        cmd = ['git', '-C', dpath, 'rm', glob_pattern]
    
    subprocess.run(cmd)
    
def git_commit_amend(dpath='cwd'):
    if dpath == 'cwd':
        cmd = ['git', 'commit', '--amend', '--no-edit']
    else:
        cmd = ['git', '-C', dpath,  '--amend', '--no-edit']
    
    subprocess.run(cmd)
    
def git_commit(dpath='cwd', message='update submodules'):
    if dpath == 'cwd':
        cmd = ['git', 'commit', '-m', message]
    else:
        cmd = ['git', '-C', dpath, 'commit', '-m', message]
    
    subprocess.run(cmd)
    
def git_merge(branches:list, message:str, dpath='cwd'):
    branches_str = ''
    for branch in branches:
        branches_str += f'{branch} '
    
    if dpath == 'cwd':
        cmd = ['git', 'merge', '-m', message] + branches
    else:
        cmd = ['git', '-C', dpath, 'merge', '-m', message] + branches
    
    subprocess.run(cmd)
    
    
def git_annex_fsck(dpath='cwd', repository='output_ria-storage'):
    if dpath == 'cwd':
        cmd = ['git', 'annex', 'fsck', '--fast', '-f', repository]
    else:
        cmd = ['git', '-C', dpath, 'annex', 'fsck', '--fast', '-f', repository]

    subprocess.run(cmd)
    

def git_add(glob_pattern:str, dpath='cwd'):
    if dpath == 'cwd':
        cmd = ['git', 'add', glob_pattern]
    else:
        cmd = ['git', '-C', dpath, 'add', glob_pattern]
    
    subprocess.run(cmd)
    
def datalad_push_data_nothing(dpath='cwd'):
    cmd = ['datalad']
    if dpath != 'cwd':
        cmd += ['-C', dpath]
    cmd += ['push', '--data', 'nothing']
    
    subprocess.run(cmd)