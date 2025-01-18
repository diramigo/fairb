import json
from pathlib import Path
import pandas as pd

class InvalidFairBError(Exception):
    """An exception for trying to init a FairB instance from an invalid json."""
    pass

class InvalidJobConfigError(InvalidFairBError):
    """An exception for a job config file with the wrong header."""
    pass

class InvalidJobStatusFileError(InvalidFairBError):
    """An exception for a job status file with the wrong header."""
    pass

class JobConfigNotFoundError(FileNotFoundError):
    """An exception for a job config file that wasn't found."""
    pass

class JobStatusFileNotFoundError(FileNotFoundError):
    """An exception for a job status file that wasn't found."""
    pass

class DuplicatedJobsError(InvalidJobConfigError):
    """An exception for a job config with duplicated job names."""
    pass

class FairB():
    _JOB_CONFIG_DICT = {'job_name':[],'dl_cmd':[],'container':[],'commit':[],'inputs':[],'outputs':[],'is_explicit':[],'output_datasets':[],'prereq_get':[],'message':[],'super_id':[],'clone_target':[],'push_target':[],'ephemeral_location':[],'req_disk_gb':[],'queue':[],'slots':[],'vmem':[],'h_rt':[],'env_vars':[],'batch':[]}
    _JOB_STATUS_DICT = {'job_name':[],'job_id':[],'req_disk_gb':[],'host':[],'location':[],'job_dir':[],'status':[],'start':[],'update':[],'total_disk_gb':[],'traceback':[]}
    
    
    def __init__(self, project_name, super_id, absolute_path, input_datasets, output_datasets, container, clone_target, push_target, current_batch='0001', designs=[], job_config_file=None, job_status_file=None):
        """
        Create FairB instance.
        """
        
        self.project_name, self.super_id, self.absolute_path, self.input_datasets, self.output_datasets, self.container, self.clone_target, self.push_target, self.current_batch, self.designs = project_name, super_id, absolute_path, input_datasets, output_datasets, container, clone_target, push_target, current_batch, designs
        
        # job config
        if job_config_file is None:
            self.job_config_file = str(Path(absolute_path) / 'job_config.csv')
        else:
            self.job_config_file = job_config_file
        self.job_config_df = None
        
        # job status
        if job_status_file is None:
            self.job_status_file = str(Path(absolute_path) / 'job_status.csv')
            self._create_job_status()
        else:
            self.job_status_file = job_status_file
            
        # lockfiles
        self.status_lockfile, self.push_lockfile = self._create_lockfiles() 
        
        # fairb directory
        if not Path(absolute_path).exists():
            Path(absolute_path).mkdir()
        
        # code directory
        fairb_code_dpath = Path(absolute_path) / 'code'
        if not fairb_code_dpath.exists():
            Path(fairb_code_dpath).mkdir()
        
        
    @classmethod
    def from_json(cls, json_path):
        """
        Create a FairB instance from a valid json file.
        """
        
        with open(json_path, 'r') as json_file:
            json_dict = json.load(json_file)
        try:
            return FairB(**json_dict, job_status_file=Path(json_path).parent / 'job_status.csv')
        except:
            raise InvalidFairBError()
    
    def _dict(self):
        """
        FairB project as a dictionary.
        """
        
        return {'project_name':self.project_name, 'super_id':self.super_id, 'absolute_path':self.absolute_path, 'input_datasets':self.input_datasets, 'output_datasets':self.output_datasets, 'container':self.container, 'clone_target':self.clone_target, 'push_target':self.push_target, 'current_batch':self.current_batch, 'designs':self.designs}
    
    def __str__(self):
        return str(self._dict())
        
    def to_json(self, fairb_dpath=None):
        """
        Write FairB json.
        """
        if fairb_dpath is None:
            fairb_file = Path(self.absolute_path) / 'fairb.json'
        else:
            fairb_file = Path(fairb_dpath) / 'fairb.json'
        
        with open(fairb_file, 'w') as json_file:
            json.dump(self._dict(), json_file)
        return None
    
    def add_design(self, variable_definition, dl_cmd, inputs, outputs, is_explicit, prereq_get, message, ephemeral_location, req_disk_gb, queue, slots, vmem, h_rt, env_vars):
        """
        Add designs for fairb jobs.
        """
        self.designs.append({'variable_definition':variable_definition, 'dl_cmd':dl_cmd, 'inputs':inputs, 'outputs':outputs, 'is_explicit':is_explicit, 'prereq_get':prereq_get, 'message':message, 'ephemeral_location':ephemeral_location, 'req_disk_gb':req_disk_gb, 'queue':queue, 'slots':slots, 'vmem':vmem, 'h_rt':h_rt, 'env_vars':env_vars})
        return None
    
    def _create_job_config(self):
        """
        Create job config file.
        """
        if not Path(self.job_config_file).exists():
            pd.DataFrame(FairB._JOB_CONFIG_DICT).to_csv(self.job_config_file, index=False)
        return None
    
    def _create_job_status(self):
        """
        Create job status file.
        """
        if not Path(self.job_status_file).exists():
            self.job_status_df = pd.DataFrame(FairB._JOB_STATUS_DICT)
            self.job_status_df.to_csv(self.job_status_file, index=False)
        return None
    
    def _create_lockfiles(self):
        """
        Create lockfiles.
        """
        status_lockfile = (Path(self.absolute_path) / 'status_lockfile').absolute()
        push_lockfile = (Path(self.absolute_path) / 'push_lockfile').absolute()
        
        if not status_lockfile.exists():
            status_lockfile.touch()
            
        if not push_lockfile.exists():
            push_lockfile.touch()
        
        return str(status_lockfile), str(push_lockfile)
    
    def _is_job_config_valid(self, config_df):
        "Is the job config file valid."
        if not config_df.columns.isin(FairB._JOB_CONFIG_DICT.keys()).all():
            raise InvalidJobConfigError()
        if len(config_df) != len(config_df.drop_duplicates()):
            raise DuplicatedJobsError()
        return None
        
    def read_job_config(self):
        """Read and validate job config file and save to FairB instance."""
        
        if self.job_config_file is None:
            raise JobConfigNotFoundError()
        if not Path(self.job_config_file).exists():
            raise JobConfigNotFoundError()
        
        try:
            self.job_config_df = pd.read_csv(self.job_config_file, dtype={'batch':str})
        except:
            raise InvalidJobConfigError()
        self._is_job_config_valid(self.job_config_df)
        
        return None
    
    def _is_job_status_valid(self, status_df):
        "Is the job status file valid."
        if not status_df.columns.isin(FairB._JOB_STATUS_DICT.keys()).all():
            raise InvalidJobStatusFileError()
        return None
        
    def read_job_status(self):
        "Read and validate job status file and save to FairB instance."
        if self.job_status_file is None:
            raise JobStatusFileNotFoundError()
        if not Path(self.job_status_file).exists():
            raise JobStatusFileNotFoundError()
        
        try:
            self.job_status_df = pd.read_csv(self.job_status_file)
        except:
            raise InvalidJobStatusFileError()
        self._is_job_status_valid(self.job_status_df)
        
        return None
    
    def get_available_jobs(self):
        """Get job names that are not completed."""
        
        if self.job_status_df is None:
            self.job_status_df = self.read_job_status()
        
        not_available_jobs = self.job_status_df.query("status.isin(['ongoing', 'completed'])")['job_name'].to_list()
        available_jobs = self.job_config_df.query("not job_name.isin(@not_available_jobs)")['job_name'].to_list()
        
        return available_jobs
    
    def get_completed_jobs(self):
        """
        Get job names of current batch that are completed.
        """
        
        if self.job_status_df is None:
            self.job_status_df = self.read_job_status()
        if self.job_config_df is None:
            self.job_config_df = self.read_job_status()
        
        completed_jobs = (self.job_config_df
         .merge(
             self.job_status_df[['job_name', 'status']],
             on='job_name'
          )
         .query("status == 'completed' and batch == @self.current_batch")
         ['job_name']
         .to_list()
         )
        
        return completed_jobs