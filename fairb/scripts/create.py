from argparse import ArgumentParser
import os
from pathlib import Path
import json
import re

import datalad.api as dl
import pandas as pd
from fairb.core import FairB


def main(args):

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
    parser.add_argument(
        '--project_name', 
        type=str, 
        help='Name of the project.',
        required=False,
        default='fairb'
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

    args = parser.parse_args(args)
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
    output_dataset_paths = []
    output_dataset_relpaths = []
    for output_dataset in output_datasets:
        
        if str(Path(output_dataset).parents[0]) == 'outputs':
            output_dataset_path =  str(Path(super_dataset).absolute() / output_dataset)
            output_dataset_relpaths.append(output_dataset)
        else:
            output_dataset_path = str(outputs_root / output_dataset)
            output_dataset_relpaths.append(str(Path('outputs') / output_dataset))
            
        dl.create(output_dataset_path, dataset=super_dataset)
        
        output_dataset_paths.append(output_dataset_path)



    # Add .gitignore
    gitignore_path = Path(super_dataset) / '.gitignore'
    with open(gitignore_path, 'w') as gitignore_file:
        gitignore_file.write('.fairb')
    dl.save(dataset=super_dataset, message='Add .gitignore')
    
    # Get datalad id
    super_dataset_id = dl.Dataset(super_dataset).id
    
    # Create super_dataset's output and input ria
    output_ria_path = str((Path(super_dataset) / '.fairb' / 'output_ria').absolute())
    input_ria_path = str((Path(super_dataset) / '.fairb' / 'input_ria').absolute())
    
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
    if output_dataset_paths:
        for output_dataset_path in output_dataset_paths:
            
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
    
    
    # Create fairb project
    fairb_path = Path(super_dataset) / '.fairb'
    fairb_project = FairB(args.project_name, super_dataset_id, str(fairb_path.resolve()), input_datasets, output_dataset_relpaths, container_name, input_ria_path, output_ria_path)
    fairb_project.to_json()
    
    
if __name__ == '__main__':
    main()