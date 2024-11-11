# Author: Diego Ramírez González
import json
from argparse import ArgumentParser
from pathlib import Path
import re
import functools
import operator

import datalad.api as dl
import pandas as pd
import numpy as np

def is_numeric(x):
    try:
        int(x)
        return True
    except:
        return False
    
def try_search(regex, val):
    try:
        return re.search(regex, val).group()
    except:
        return None
        
def get_random_seed():
    """
    Return a 9 digit random integer.
    """
    return np.random.randint(100_000_000, 999_999_999)

def create_command_df(variable_commands_string):
    """
    Extract the commands within each variable definition. 
    Return a dataframe where each row is a command.
    """
    
    command_value = {'drop':1, 'glob':1,'variable':1,'paste':1,'write':1,'replace':-1, 'is_in':-1, 'not_in':-1, 'grep':-1,'unique':-1,'multiply':-1, 'repeat':-1,'exists':-1}
    
    command_names = re.findall("(?<=\<!)\w+(?=\>)", variable_commands_string)
    commands =  re.sub('<!\w+>', '<!command>', variable_commands_string).split('<!command>')[1:]
    
    command_df = (pd.DataFrame({'command_name':command_names, 'command':commands})
    .assign(
        command = lambda df_: 
            df_['command'].str.strip().str.replace(r'(^\(|\)$)', '', regex=True),
        command_value = lambda df_: df_['command_name'].map(command_value)
    )
    )

    assert command_df['command_name'].isin(command_value.keys()).all(), "Used an invalid command within a variable definition."

    assert command_df['command_value'].is_monotonic_decreasing, "'drop', 'glob', 'variable', 'paste', and 'write' have to be the first action within each variable definition."

    assert command_df['command_value'].iat[0] >= 0, "Can't start variable definition with 'grep', 'unique', 'multiply', 'repeat' or 'exists'."

    assert command_df['command_value'].sum() < 2, "You can't use both 'drop', 'glob' and 'variable' within the same variable definition."
    
    if command_df['command_name'].iat[0] == 'drop':
        assert command_df['command_value'].shape[0] == 1, "You can't use commands after 'drop'."
    
    return command_df


def call_glob(cmd):
    """
    Return an ordered list of glob results relative to super dataset.
    """
    
    glob_characters = '[a-z,A-Z,0-9,\\,\/,\-,_,\.,\*,\[,\],\:,\+,\?,\!,\s]'
    assert super_dataset_path, "No known super dataset for globbing."
    
    try:
        globbing_list = re.search(f"{glob_characters}+", cmd).group().split()
    
        values = sorted(
            [str(path.relative_to(super_dataset_path)) 
            for globbing in globbing_list
            for path in Path(super_dataset_path).glob(globbing)]
            )
    except:
        raise Exception('Not a valid globbing pattern.')
    
    return values


def call_variable(cmd, variables):
    """
    Get the values from an existing variable.
    """
    variable_characters = '[a-z,A-Z,0-9,\-,_,]'
    try:
        variable_name = re.search(f"{variable_characters}+", cmd).group()
    except:
        raise Exception("Tried to call a variable with an invalid variable name.")
    
    try:
        values = variables[variable_name]
    except:
        raise Exception("Tried to call a non-existing variable.")
    
    return values


def call_paste(cmd, variables):
    """
    Paste existing variables within text.
    Returns a list of values.
    """
    
    paste_variables = list(set(re.findall('(?<=\{)[\w,_,-]+(?=\})', cmd)))
    
    assert pd.Series(paste_variables).isin(variables.keys()).all(), "Not all variables in 'paste' exist."
    
    values = []
    for row in pd.DataFrame(variables)[paste_variables].to_dict(orient='records'):
        values.append(cmd.format(**row))
    
    return values

def call_write(cmd):
    """
    Return a list of space-separeted values from a string.
    """
    return cmd.split()


def call_replace(cmd, values):
    """
    Replace a string pattern from existing values. 
    Can call existing variables within 'cmd' using double curly brackets.
    Returns a list of values.
    """
    
    
    n_spaces = len([character for character in cmd if character == ' '])
    assert n_spaces == 1, "Didn't find exactly one space within 'replace'. Command should be:('to_be_replaced' 'replacement')."
    
    string_detect = cmd.split()[0]
    string_replace = cmd.split()[1]            
    
    new_values = []
    for value in values:
        new_values.append(
            re.sub(string_detect, string_replace, value).replace('{{' , '{').replace('}}', '}')
            )
        
    values = new_values
        
    replacement_variables = re.findall("(?<={)\w+(?=})", '{subject}')
    if replacement_variables:
        if not pd.Series(replacement_variables).isin(variables.keys()).all():
            raise Exception("Not all variables inside <!replace> exist.")
        
        new_values = []
        for value, row_dict in zip(values, pd.DataFrame(variables).to_dict(orient='records')):
            new_values.append(value.format(**row_dict))
            
        values = new_values
        
    return values


def call_drop():
    """
    Returns None.
    """
    return None


def call_grep(cmd, values):
    """
    Return a string pattern from existing values using regular expressions.
    """
    values = [try_search(cmd, value) for value in values]
    assert not all(value is None for value in values), "Not a valid regex or no matches."

    return values


def call_repeat(cmd, values):
    "Repeat existing values."
    if isinstance(values, str):
        values = values.split()
    if is_numeric(cmd):
        n_elements = int(cmd)
    else:
        assert cmd in variables.keys(), "Tried to repeat by the length of a variable that doesn't exist."
        n_elements = len(variables[cmd])    
           
    values = [[value]*n_elements for value in values]
    values = functools.reduce(operator.iconcat, values, [])
    
    return values


def call_is_in(cmd, values):
    """
    Return values in common between two existing variables.
    """
    variable = cmd.strip()
    
    variable_characters = '[a-z,A-Z,0-9,\-,_,]'
    try:
        variable = re.search(f"{variable_characters}+", variable).group()
    except:
        raise Exception("Within 'is_in', tried to call a variable with an invalid variable name.")
    
    try:
        variable = pd.Series(variables[variable])
        values = pd.Series(values)
    except:
        raise Exception("Within 'is_in', tried to call a variable that doesn't exist.")
    
    return values[values.isin(variable)].to_list()


def call_not_in(cmd, values):
    """
    Return values not in common between two existing variables.
    """
    variable = cmd.strip()
    
    variable_characters = '[a-z,A-Z,0-9,\-,_,]'
    try:
        variable = re.search(f"{variable_characters}+", variable).group()
    except:
        raise Exception("Within 'not_in', tried to call a variable with an invalid variable name.")
    
    try:
        variable = pd.Series(variables[variable])
        values = pd.Series(values)
    except:
        raise Exception("Within 'not_in', tried to call a variable that doesn't exist.")
    
    return values[~values.isin(variable)].to_list()
    

def call_multiply(cmd, values):
    "Multiply existing values."
    if isinstance(values, str):
        values = values.split()
    if is_numeric(cmd):
        n_elements = int(cmd)
    else:
        assert cmd in variables.keys(), "Tried to multiply by the length of a variable that doesn't exist."
        n_elements = len(variables[cmd])    
           
    values = values*n_elements 
    
    return values

    
def call_unique(values):
    """
    Remove duplicated values, keep the first one and maintain the same order. 
    """
    return pd.Series(['pcc', 'acc', 'pcc']).drop_duplicates(keep='first').to_list()
    

def call_exists(values):
    """
    
    """
    assert super_dataset_path, "No known super dataset for globbing." 
    new_values = []
    for value in values:
        if (super_dataset_path / str(value)).is_symlink() or (super_dataset_path / str(value)).exists():
            new_values.append(value)
        else:
            new_values.append(None)
    
    values = new_values
    
    return values
    

def select_command(command_name, command, values=None, variables=None, current_variable_name=None):
    """
    Select one of the possible 9 commands and return the values.
    """
    match command_name:
        # first-postion commands
        case 'drop':
            return call_drop()
        
        case 'glob':
            return call_glob(command)

        case 'write':
            return call_write(command)
        
        case 'variable':
            assert variables, "Can't use 'variable' if no variable has been defined."
            return call_variable(command, variables)
        
        case 'paste':
            assert variables, "Can't use 'paste' if no variable has been defined."
            return call_paste(command, variables)
        
        # non-first commands    
        case 'replace':
            assert values, "Can't use 'replace' without existing values."
            return call_replace(command, values)
        
        case 'grep':
            assert values, "Can't use 'grep' without existing values."
            return call_grep(command, values)
        
        case 'is_in':
            assert values, "Can't use 'is_in' without existing values."
            return call_is_in(command, values)
        
        case 'not_in':
            assert values, "Can't use 'not_in' without existing values."
            return call_not_in(command, values)
        
        case 'multiply':
            assert values, "Can't use 'multiply' without existing values."
            return call_multiply(command, values)
        
        case 'repeat':
            assert values, "Can't use 'repeat' without existing values."
            return call_repeat(command, values)
        
        case 'unique':
            assert values, "Can't use 'unique' without existing values."
            return call_unique(values)
        
        case 'exists':
            assert values, "Can't use 'exists' without existing values."
            return call_exists(values)
            
        case _:
            raise Exception("Non-existing command selected.")
            

def main():
    
    parser = ArgumentParser(
        prog="fairb_create",
        description="Create an array of jobs for FAIR parallel processing."
        )
    
    fairlybig = parser.add_argument_group()
    job_definition = parser.add_argument_group()
    misc = parser.add_argument_group()
    job_resources = parser.add_argument_group()
    
    fairlybig.add_argument(
        '-c',
        '--path',
        type='str',
        default='.',
        help='Path to fairlybig project (must contain a valid config file).'
    )
    
    job_definition.add_argument(
        "variables",
        type=str,
        help="placeholder"
    )
    job_definition.add_argument(
        "dl_cmd",
        type=str,
        help="placeholder"
    )
    job_definition.add_argument(
        "job_name",
        type=str,
        help="placeholder",
        required=True
    )
    job_definition.add_argument(
        "--inputs",
        type=str,
        help="placeholder",
        required=False
    )
    job_definition.add_argument(
        "--outputs",
        type=str,
        help="placeholder",
        required=False
    )


    misc.add_argument(
        "--prereq_get",
        type=str,
        help="placeholder",
        required=False
    )
    # misc.add_argument(
    #     "--message",
    #     type=str,
    #     help="placeholder",
    #     required=False
    # )
    
    job_resources.add_argument(
        "--req_disk_gb",
        type=int,
        help="placeholder",
        required=False
    )

    job_resources.add_argument(
        "--queue",
        type=str,
        help="placeholder",
        required=True
    )

    job_resources.add_argument(
        "--slots",
        type=int,
        help="placeholder",
        required=False
    )
    
    job_resources.add_argument(
        "--vmem",
        type=int,
        help="placeholder",
        required=False
    )

    job_resources.add_argument(
        "--h_rt",
        type=str,
        help="placeholder",
        required=False
    )

    job_resources.add_argument(
        "--env_vars",
        type=str,
        help="placeholder",
        required=False
    )
    
    args = parser.parse_args()
    
    fairb_root = Path(args.path)
    assert fairb_root.exists(), "fairb project directory doesn't exist."
    
    fairb_config_path = fairb_root / 'fairb_config.json'
    assert fairb_config_path.exists(), "No fairb_config.json found."
    
    with open(fairb_config_path, 'r') as json_file:
        fairb_config = json.load(json_file)


    #      'job_name','dl_cmd','container','inputs',
    #  'outputs':[None]
    #  'is_explicit':[False],
    #  'output_datasets':[output_datasets_string],
    #  'prereq_get':[None],
    #  'message':[None],
    #  'super_id':super_dataset_id,
    #  'clone_target':[input_ria_path],
    #  'push_target':[output_ria_path],
    #  'ephemeral_location':["/tmp /misc/{host}[0-9]/"+user],
    #  'req_disk_gb':[None],
    #  'queue':['all.q'],
    #  'slots':[None],
    #  'vmem':[None],
    #  'h_rt':[None],
    #  'env_vars':[None],
    #  'batch':['001']
    # list(fairb_config.keys())
    
    
    
    super_dataset_path = Path('/misc/geminis2/ramirezd/test_bet/')
    variable_definition_string = "svs == <!glob>(inputs/mri-raw/sub-*/mrs/*acq-press*svs.nii.gz) ; subject == <!variable>(svs)<!grep>(sub-\w+); t1w == <!paste> (inputs/mri-raw/{subject}/anat/{subject}_T1w.nii.gz) ; t2w == <!variable>(t1w)<!replace>(T1w T2w)<!exists> ; ref == <!variable>(svs)<!replace>(svs(?=.nii.gz) ref)<!exists> "

    variable_definition_string = "voi == <!write>(acc pcc)<!multiply>5"

    # Create a tuple of variable-commands
    variable_command = []

    for index, variable_definition in enumerate(variable_definition_string.split(';')):
        variable_name = variable_definition.split('==')[0].strip()
        variable_commands_string = variable_definition.split('==')[1].strip()
        command_df = create_command_df(variable_commands_string)
        variable_command.append((index, variable_name, command_df))


    # Create a dictionary of variable-values
    variables = {}

    len_dict = {}
    max_len=0

    for index, variable_name, command_df in variable_command:
        values = []
        for command_row in command_df.itertuples():
            values = select_command(
                command_row.command_name,
                command_row.command,
                values,
                variables
                )
        
        # Drop variable if it's None and skip next steps
        if values is None: 
            try:
                del variables[variable_name]
                del len_dict[variable_name]
            except:
                pass
            
            continue
            
        variables[variable_name] = values
        len_dict[variable_name] = len(values)


    # Calculate the max length of each variable for broadcasting. 
    for variable_name, len_of_values in len_dict.items():
        if len_of_values > max_len:
            max_len = len_of_values
            max_len_variable = variable_name

    # Broadcast variables.
    if np.all(max_len % np.array(list(len_dict.values())) == 0):
        for key, value_list in variables.items():
            if key == max_len_variable:
                continue
            variables[key] = value_list * int(max_len / len(value_list))
    else:
        raise Exception("Can't broadcast variables.")
    
    dl_cmd = "bet inputs/mri_raw/{subject}/anat/{subject}_T1w.nii.gz outputs/bet/{subject}_T1w_bet.nii.gz --radom_seed <!random>"
    inputs = "inputs/mri_raw/{subject}/anat/{subject}_T1w.nii.gz"
    outputs = "outputs/bet/{subject}_T1w_bet.nii.gz"
    job_name = "{subject}_T1w_bet"
    job_dict = {'job_name':[], 'dl_cmd':[], 'inputs':[], 'outputs':[]}

    if ("<!random>" in dl_cmd) and not "{random_seed}" in dl_cmd:
        random_seeds=[]
        for i in range(max_len):
            random_seeds.append(get_random_seed())
        
        dl_cmd = re.sub("<!random>", "{random_seed}", dl_cmd)
        variables['random_seed'] = random_seeds

    for row_dict in pd.DataFrame(variables).dropna().to_dict(orient='records'):
        job_dict['job_name'].append(job_name.format(**row_dict))
        job_dict['dl_cmd'].append(dl_cmd.format(**row_dict))
        job_dict['inputs'].append(inputs.format(**row_dict))
        job_dict['outputs'].append(outputs.format(**row_dict))