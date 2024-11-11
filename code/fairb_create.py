from argparse import ArgumentParser
import os
from pathlib import Path
import json

import datalad.api as dl
import pandas as pd


def main():

    parser = ArgumentParser(
        prog="fairb_create",
        description="Create a FAIRlyBIG project."
        )
    parser.add_argument(
        'super_dataset', 
        help='Path and name of the superdataset.',
        type=str
        )
    parser.add_argument(
        '--input_datasets', 
        nargs='+', 
        help='Paths or URLs to datalad input datasets.', 
        required=True
        )
    parser.add_argument(
        '--output_datasets', 
        nargs='+', 
        help='Names of output subdatasets.',
        required=False,
        default=[]
        )
    container_args = parser.add_argument_group()
    
    container_args.add_argument(
        '--container', 
        required=False,
        type=str,
        help='Path to container datalad dataset, apptainer image or URL (e.g. docker://user/project:version).'
        )
    container_args.add_argument(
        '--container_name', 
        required=False,
        type=str,
        help='Name of the containers (e.g. fmriprep-24-0-1)'
        )
    container_args.add_argument(
        '--call_fmt', 
        type=str, 
        required=False, 
        default="apptainer run -e {img} {cmd}"
        )

    args = parser.parse_args()
    super_dataset = args.super_dataset
    input_datasets = args.input_datasets
    output_datasets = args.output_datasets
    container_dataset = args.container
    container_name = args.container_name
    call_fmt = args.call_fmt
    
    
    # Create superdataset
    dl.create(super_dataset, cfg_proc='yoda')
    
    # Add input datasets
    inputs_root = (Path(super_dataset) / 'inputs').absolute()
    inputs_root.mkdir()
    
    for input_dataset in input_datasets:
        input_dataset_path = str(inputs_root / Path(input_dataset).stem)
        dl.clone(input_dataset, input_dataset_path, dataset=super_dataset)
        
    if container_dataset and container_name:
        input_container_dataset_path = str(inputs_root / 'containers')
        image_path = str(Path(input_container_dataset_path) / '.datalad' / 'environments' / container_name / 'image')
        dl.clone(container_dataset, input_container_dataset_path, dataset=super_dataset)
        dl.containers_add(container_name, call_fmt=call_fmt, image=image_path, dataset=str(Path(super_dataset).absolute()))

    # Add output datasets
    outputs_root = (Path(super_dataset) / 'outputs').absolute()
    outputs_root.mkdir()
    for output_dataset in output_datasets:
        output_dataset_path = str(outputs_root / output_dataset)
        dl.create(output_dataset_path, dataset=super_dataset)
        
    # Add .gitignore
    gitignore_path = Path(super_dataset) / '.gitignore'
    with open(gitignore_path, 'w') as gitignore_file:
        gitignore_file.write('.fairlybig')
    dl.save(dataset=super_dataset, message='Add .gitignore')
    
    # Get datalad id
    super_dataset_id = dl.Dataset(super_dataset).id
    
    # Create super_dataset's output and input ria
    output_ria_path = str((Path(super_dataset) / '.fairlybig' / 'output_ria').absolute())
    input_ria_path = str((Path(super_dataset) / '.fairlybig' / 'input_ria').absolute())
    
    dl.create_sibling_ria(
        f'ria+file://{output_ria_path}',
        name='output_ria',
        dataset=super_dataset,
        new_store_ok=True,
    )
    
    dl.create_sibling_ria(
        f'ria+file://{input_ria_path}',
        name='input_ria',
        dataset=super_dataset,
        new_store_ok=True,
    )
    
    # Push super_dataset 
    dl.push(dataset=super_dataset, to='output_ria')
    dl.push(dataset=super_dataset, to='input_ria')
    
    # Create output subdataset output and input ria
    if output_datasets:
        for output_dataset in output_datasets:
            
            output_dataset_path = str(outputs_root / output_dataset)
            
            dl.create_sibling_ria(
                f'ria+file://{output_ria_path}',
                name='output_ria',
                dataset=output_dataset_path
            )

            dl.create_sibling_ria(
                f'ria+file://{input_ria_path}',
                name='input_ria',
                dataset=output_dataset_path
            )
            
            dl.push(dataset=output_dataset_path, to='output_ria')
            dl.push(dataset=output_dataset_path, to='input_ria')
    
        output_datasets_string = ''
        for output_dataset in output_datasets:
            output_datasets_string += f'{output_dataset} '
        output_datasets_string = output_datasets_string.strip()
    else:
        output_datasets_string = None
    
    user = os.getenv('USER')
    
        
    config_dict ={
     'job_name':[None],
     'dl_cmd':[None],
     'container':[container_name],
     'inputs':[None],
     'outputs':[None],
     'is_explicit':[False],
     'output_datasets':[output_datasets_string],
     'prereq_get':[None],
     'message':[None],
     'super_id':super_dataset_id,
     'clone_target':[input_ria_path],
     'push_target':[output_ria_path],
     'ephemeral_location':["/tmp /misc/{host}[0-9]/"+user],
     'req_disk_gb':[None],
     'queue':['all.q'],
     'slots':[None],
     'vmem':[None],
     'h_rt':[None],
     'env_vars':[None],
     'batch':['001']
    }
    
    fairlybig_path = Path(super_dataset) / '.fairlybig'
    (fairlybig_path / 'code').mkdir()
    
    with open(str(fairlybig_path / 'fairb_config.json'), 'w') as json_file:
        json.dump(config_dict, json_file)
   
    
    # pd.DataFrame(job_config_dict).to_csv(str(fairlybig_path / 'code' / 'job_config.csv'), index=False)
    
if __name__ == '__main__':
    main()