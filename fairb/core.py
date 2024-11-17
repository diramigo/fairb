import json
from pathlib import Path
import pandas as pd

class FairB():
    _JOB_CONFIG_DICT = {'job_name':[],'dl_cmd':[],'container':[],'commit':[],'inputs':[],'outputs':[],'is_explicit':[],'output_datasets':[],'prereq_get':[],'message':[],'super_id':[],'clone_target':[],'push_target':[],'ephemeral_location':[],'req_disk_gb':[],'queue':[],'slots':[],'vmem':[],'h_rt':[],'env_vars':[],'batch':[]}
    _JOB_STATUS_DICT = {'job_name':[],'job_id':[],'req_disk_gb':[],'host':[],'location':[],'job_dir':[],'status':[],'start':[],'update':[],'total_disk_gb':[],'traceback':[]}
    
    def __init__(self, project_name, super_id, absolute_path, input_datasets, output_datasets, container, clone_target, push_target, current_batch='0001', designs=[], job_config=None, job_status=None):
        self.project_name, self.super_id, self.absolute_path, self.input_datasets, self.output_datasets, self.container, self.clone_target, self.push_target, self.current_batch, self.designs = project_name, super_id, absolute_path, input_datasets, output_datasets, container, clone_target, push_target, current_batch, designs
        
        if job_config is None:
            self.job_config = Path(absolute_path) / 'job_config.csv'
        else:
            self.job_config = job_config
        
        if job_status is None:
            self.job_status = Path(absolute_path) / 'job_status.csv'
        else:
            self.job_status = job_status
            
        if not Path(absolute_path).exists():
            absolute_path.mkdir()
        
        fairb_code_dpath = Path(absolute_path) / 'code'
        if not fairb_code_dpath.exists():
            fairb_code_dpath.mkdir()
        
        
    @classmethod
    def from_json(cls, json_path):
        with open(json_path, 'r') as json_file:
            json_dict = json.load(json_file)
        return FairB(**json_dict)
    
    def _dict(self):
        return {'project_name':self.project_name, 'super_id':self.super_id, 'absolute_path':self.absolute_path, 'input_datasets':self.input_datasets, 'output_datasets':self.output_datasets, 'container':self.container, 'clone_target':self.clone_target, 'push_target':self.push_target, 'current_batch':self.current_batch, 'designs':self.designs}
    
    def __str__(self):
        return str(self._dict())
        
    def to_json(self, fairb_dpath=None):
        if fairb_dpath is None:
            fairb_file = Path(self.absolute_path) / 'fairb.json'
        else:
            fairb_file = Path(fairb_dpath) / 'fairb.json'
        
        with open(fairb_file, 'w') as json_file:
            json.dump(self._dict(), json_file)
        return None
    
    def add_design(self, variable_definition, dl_cmd, inputs, outputs, is_explicit, prereq_get, message, ephemeral_location, req_disk_gb, queue, slots, vmem, h_rt, env_vars):
        self.designs.append({'variable_definition':variable_definition, 'dl_cmd':dl_cmd, 'inputs':inputs, 'outputs':outputs, 'is_explicit':is_explicit, 'prereq_get':prereq_get, 'message':message, 'ephemeral_location':ephemeral_location, 'req_disk_gb':req_disk_gb, 'queue':queue, 'slots':slots, 'vmem':vmem, 'h_rt':h_rt, 'env_vars':env_vars})
        return None
    
    def _create_job_config(self):
        if not Path(self.job_config).exists():
            pd.DataFrame(FairB._JOB_CONFIG_DICT).to_csv(self.job_config, index=False)
        return None
    
    def _create_job_status(self):
        if not Path(self.job_status).exists():
            pd.DataFrame(FairB._JOB_STATUS_DICT).to_csv(self.job_status, index=False)
        return None
        
    
    