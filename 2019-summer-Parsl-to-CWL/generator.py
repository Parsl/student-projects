#dbmanager.py
import logging
import threading
import queue
import os
import time

from parsl.dataflow.states import States
from parsl.providers.error import OptionalModuleMissing

try:
    import sqlalchemy as sa
    from sqlalchemy import Column, Text, Float, Integer, DateTime, PrimaryKeyConstraint
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import desc
except ImportError:
    _sqlalchemy_enabled = False
else:
    _sqlalchemy_enabled = True

try:
    from sqlalchemy_utils import get_mapper
except ImportError:
    _sqlalchemy_utils_enabled = False
else:
    _sqlalchemy_utils_enabled = True

WORKFLOW = 'workflow'    # Workflow table includes workflow metadata
TASK = 'task'            # Task table includes task metadata
STATUS = 'status'        # Status table includes task status
RESOURCE = 'resource'    # Resource table includes task resource utilization

from parsl.monitoring.db_manager import Database

################################ CWL Data Structure #####################################
#CWL classes for rendering purpose        
class cwl_File(object):
    def __init__(self,path=''):
        self.path = path
class cwl_string(str):
    def __init__(self,string=''):
        self.string=string
    pass
class cwl_array(list):
    def __init__(self, lst):
        self.list = lst
class cwl_CommandLineTool(object):
    def __init__(self, name, basecommand,arguments=[], inputs=[], outputs=[]):
        self.name = name
        self.basecommand = basecommand
        self.inputs = inputs
        self.outputs = outputs
        self.arguments = arguments

class cwl_step(object):
    def __init__(self, clt, inputs=[], outputs=[],step_idx = 0, indep=True, scatter = None):
        self.clt = clt
        self.inputs = inputs
        self.outputs = outputs
        self.scatter = scatter
        self.indep = indep
        self.step_idx = step_idx
        
class cwl_Workflow(object):
    def __init__(self, run_id, inputs=[], outputs=[], steps=[], requirements = [] ):
        self.requirements = requirements
        self.inputs = inputs
        self.input_count = 0
        self.outputs = outputs
        self.steps = steps
        self.run_id = run_id


################################  Rendering  #####################################
# Rendering functions
def render_cwl(filename,cwl_workflow, clt = []):
    render_clts(clt)
    render_workflow(filename, cwl_workflow)
    return

def render_clts(clt):
    for clt in clts:
        # Check for special commands: cat
        if clt.basecommand == 'cat':
            render_cat(clt)
            continue
        f = open(clt.name+'.cwl', "w+")
        #Write head and basecommand
        f.write("#!/usr/bin/env cwl-runner\n\ncwlVersion: v1.0\nclass: CommandLineTool\nbaseCommand: ")
        if clt.arguments==[]:
            f.write(clt.basecommand+'\n')
        else:
            f.write('['+clt.basecommand+',')
            for arg in clt.arguments:
                f.write(arg)
                if clt.arguments.index(arg)!= len(clt.arguments)-1:
                    f.write(',')
            f.write(']\n')
        # Write inputs
        f.write("inputs:\n")
        for i in range(len(clt.inputs)):
            f.write('  input_{}: \n    type: '.format(str(i)))
            if type(clt.inputs[i]) == cwl_File:
                f.write('File\n')
            elif type(clt.inputs[i]) == cwl_string or type(clt.inputs[i]) == str:
                f.write('string\n')
            if True: #i < len(clt.inputs)-len(clt.outputs):
                f.write('    inputBinding: \n      position: {}\n'.format(i))
        #Write outputs
        f.write("outputs:\n")
        for i in range(len(clt.outputs)):
            f.write("  output_{0}: \n    type: File\n    outputBinding: \n      glob: $(inputs.input_{1})\n".format(i, len(clt.inputs)-len(clt.outputs)+i))                
               
        f.close()
        
def render_cat(clt):
    f = open(clt.name+'.cwl', "w+")
    #Write head and basecommand
    f.write("#!/usr/bin/env cwl-runner\n\ncwlVersion: v1.0\nclass: CommandLineTool\nbaseCommand: ")
    if clt.arguments==[]:
        f.write(clt.basecommand+'\n')
    else:
        f.write('['+clt.basecommand+',')
        for arg in clt.arguments:
            f.write(arg)
            if clt.arguments.index(arg)!= len(clt.arguments)-1:
                f.write(',')
        f.write(']\n')
    # Write stdout
    # For cat, default option is to cast stdout in a file whose name is specified in the last input
    f.write('stdout: $(inputs.input_{})\n'.format(str(len(clt.inputs)-1)))
    # Write inputs
    f.write("inputs:\n")
    for i in range(len(clt.inputs)):
        f.write('  input_{}: \n    type: '.format(str(i)))
        if type(clt.inputs[i]) == cwl_File:
            f.write('File\n')
        elif type(clt.inputs[i]) == cwl_string or type(clt.inputs[i]) == str:
            f.write('string\n')
        if i < len(clt.inputs)-len(clt.outputs):
            f.write('    inputBinding: \n      position: {}\n'.format(i))
    #Write outputs
    f.write("outputs:\n")
    for i in range(len(clt.outputs)):
        f.write("  output_{0}: \n    type: File\n    outputBinding: \n      glob: $(inputs.input_{1})\n".format(i, len(clt.inputs)-len(clt.outputs)+i)) 
    f.close()

def render_workflow(filename, cwl_workflow):
    f= open(filename,"w+")
    f.write("#!/usr/bin/env cwl-runner\n\ncwlVersion: v1.0\nclass: Workflow\n\n")
    f.close()
    render_requirements(filename, cwl_workflow)
    render_inputs(filename, cwl_workflow)
    render_outputs(filename, cwl_workflow)
    render_steps(filename, cwl_workflow)
    return

def render_requirements(filename, cwl_workflow):
    f = open(filename, "a+")
    if cwl_workflow.requirements:
        f.write("requirements:\n")
    for requirement in cwl_workflow.requirements:
        f.write('  '+requirement+"\n")
    f.close()
    
def render_inputs(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("inputs:\n")
    if not cwl_workflow.inputs:
        f.write("  []\n")
    idx = 0
    for i in cwl_workflow.inputs:
        #idx = cwl_workflow.inputs.index(i)
        if type(i) == cwl_File:
            f.write("  input_{}: File\n".format(idx))
        elif type(i) == cwl_string:
            f.write("  input_{}: string\n".format(idx))
        elif type(i) == cwl_array or type(i) == list:
            if not i:
                continue
            elif type(i[0])==cwl_string:
                f.write("  input_{}: string[]\n".format(idx))
            elif type(i[0])==cwl_File:
                f.write("  input_{}: File[]\n".format(idx))
        idx+=1
    f.close()

def render_outputs(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("outputs:\n")
    if not cwl_workflow.outputs:
        f.write("  []\n")
    for i in cwl_workflow.outputs:
        idx = cwl_workflow.outputs.index(i)
        if type(i) == cwl_File:
            f.write("  output_{}:\n    type: File\n".format(idx))
            f.write("    outputSource: {}\n".format(i.path))
        else: #Assuming only FIle and File[] can exist
            for j in i:
                f.write("  output_{}:\n    type: File[]\n".format(idx))
                f.write("    outputSource: {}\n".format(j.path))            
    f.close()
    
def render_steps(filename, cwl_workflow):
    f = open(filename, "a+")
    f.write("\nsteps:\n")
    for step in cwl_workflow.steps:
        idx = cwl_workflow.steps.index(step)
        f.write('  step_'+str(step.step_idx)+':\n')
        f.write("    run: {}.cwl\n".format(step.clt.name))
        if step.scatter:
            f.write("    scatter: input_0\n")
        f.write("    in:\n")
        for i in range(len(step.clt.inputs)):
            f.write("      input_{0}: {1}\n".format(i,step.inputs[i]))
        f.write("    out:\n")
        render_step_output(f, step.outputs)
    f.close()
    return
    
def render_step_output(f, outputs):
    f.write("      [")
    for i in range(len(outputs)):
        f.write('{0}'.format(outputs[i]))
        if (i!=len(outputs)-1):
            f.write(',')
    f.write("]\n")


################################  Interpreting  #####################################
# Functions that extract information from Database: monitoring.db
# and fill in the class cwl_Workflow 
# before using it to generate a CWL workflow
def add_inputs(cwl_workflow):
    workflow_inputs = []
    input_instance = None
    #input_count = 0
    for step in cwl_workflow.steps:
        for i in range(len(step.clt.inputs)):
            if step.inputs[i][0] == 's':
                continue
            if type(step.clt.inputs[i])==cwl_string or type(step.clt.inputs[i])==str:
                input_instance = cwl_string('')
            elif type(step.clt.inputs[i])==cwl_File: # TODO: Consider when input is an array
                input_instance = cwl_File('')
            elif type(step.clt.inputs[i])==list or type(step.clt.inputs[i])==cwl_array:
                if type(step.clt.inputs[i][0])==cwl_string or type(step.clt.inputs[i][0])==str:
                    input_instance = [cwl_string('')]
                elif type(step.clt.inputs[i][0])==cwl_File: # TODO: Consider when input is an array
                    input_instance = [cwl_File('')]
            if step.scatter!=None:
                input_instance = [input_instance]
            workflow_inputs.append(input_instance)  
            #print("Input count: {}".format(input_count));input_count+=1
    cwl_workflow.inputs = workflow_inputs

def add_outputs(cwl_workflow):
    workflow_outputs =[]
    workflow_out_idx = 0
    for step in cwl_workflow.steps:
        step_out_idx = 0
        for i in range(len(step.clt.outputs)):
            #print(type(step.clt.outputs[i]))
            if type(step.clt.outputs[i])==cwl_File: 
                output_instance =cwl_File('step_{0}/output_{1}'.format(step.step_idx,step_out_idx))
                #print('{0}/output_{1}'.format(step.clt.name,out_idx))
                if step.scatter != None:
                    output_instance = [output_instance]
                workflow_outputs.append(output_instance)
            step_out_idx+=1
        workflow_out_idx+=1
    #print(workflow_outputs[0])
    cwl_workflow.outputs = workflow_outputs       

    
def add_indep_steps(tasks, db,  cwl_workflow, clts=[] ):
    # make independent tasks with the same task_func_name just one step by scatter
    # filter independent tasks
    indep_tasks_query = tasks.filter(db.Task.task_depends=='').filter(db.Task.run_id == cwl_workflow.run_id)
    # make a list of independent function names
    indep_task_func_name =[]
    idx = 0
    for task in indep_tasks_query:
        indep_task_func_name.append(task.task_func_name)
        func_name = task.task_func_name
        has_clt = False
        clt_for_func_name = None
        for clt in clts: #check for clt availability
            if clt.name == func_name:
                clt_for_func_name = clt
                has_clt = True
        if has_clt == True:
            inputs = []; outputs = []
            out_idx = 0
            for i in clt_for_func_name.inputs:
                inputs.append('input_{}'.format(idx))
                idx+=1
            for i in clt_for_func_name.outputs:
                outputs.append('output_{}'.format(out_idx))
                out_idx +=1
            step = cwl_step(clt_for_func_name, inputs=inputs, outputs = outputs, step_idx = task.task_id)
            cwl_workflow.steps.append(step)
            cwl_workflow.input_count += len(step.inputs)
        else:
            print("clt {} missing".format(func_name))

        
        
def add_dep_steps(tasks, db,  cwl_workflow, clts=[] ):
    # make independent tasks with the same task_func_name just one step by scatter
    # filter independent tasks
    dep_task_query = tasks.filter(db.Task.task_depends!='').filter(db.Task.run_id == cwl_workflow.run_id)
    # make a list of independent function names
    dep_task_func_name =[]
    depends_on =[]
    for task in dep_task_query:
        dep_task_func_name.append(task.task_func_name)

        func_name = task.task_func_name
        idx = dep_task_func_name.index(func_name)
        has_clt = False
        clt_for_func_name = None
        for clt in clts:
            if clt.name == func_name:
                clt_for_func_name = clt
                has_clt = True    
    
        if has_clt == True:
            inputs = []; outputs = []
            # Find their corresponding preceding tasks
            pre_task_ids = eval('['+task.task_depends+']')
            for i in range(len(clt_for_func_name.inputs)-len(clt_for_func_name.outputs)): # for each input in the command:
                #find if it has preceding tasks, determine the preceding step(task) id, and the required output index
                pre_task_id_and_out_idx = find_pre_task(interpret_task_inputs(task.task_inputs)[i], tasks) #[task_id, out_idx]
                inputs.append('step_{0}/output_{1}'.format(pre_task_id_and_out_idx[0],pre_task_id_and_out_idx[1]))
            for j in range(len(clt_for_func_name.outputs)):
                inputs.append('input_{}'.format(cwl_workflow.input_count))
                outputs.append('output_{}'.format(str(j)))
                cwl_workflow.input_count += 1
            step = cwl_step(clt_for_func_name, inputs=inputs, outputs = outputs,step_idx = task.task_id , indep=False, scatter=None)
            cwl_workflow.steps.append(step)
            
        else:
            print("clt '{}' missing".format(func_name))
    
def find_pre_task(input_string, pre_tasks_query):   
    pre_task_id_and_out_idx =['0','0']
    for pre_task in pre_tasks_query:
        task_outputs = eval(pre_task.task_outputs)
       # print(input_string, task_outputs)
        if input_string in task_outputs:
            pre_task_id_and_out_idx[0]=pre_task.task_id
            pre_task_id_and_out_idx[1]=task_outputs.index(input_string)
            break
    return pre_task_id_and_out_idx

# task_inputs interpreter only for DEPENDET tasks
def interpret_task_inputs(task_inputs):
    evaluated_inputs =[]
    try: #success if the inputs are string
        evaluated_inputs = eval(task_inputs)
#         for i in evaluated_inputs:
#             i = cwl_string(i)
    except: # this means inputs are files, and we need to modify task_input
        new_str = ''
        for s in task_inputs:
            if s == '[':
                new_str += "['"
            elif s == ',':
                new_str += "','" 
            elif s == ']':
                new_str += "']"
            elif s == ' ':
                continue
            else:
                new_str += s
        evaluated_inputs = eval(new_str)
#         for i in range(len(evaluated_inputs)):
#             evaluated_inputs[i] = cwl_File(evaluated_inputs[i])
    return evaluated_inputs



# User API
def find_run_id():
    print('Would you like me to work on the latest workflow? Type y for yes, n for no.')
    is_latest = input()
    if is_latest == 'n':
        print("What's the run_id of the workflow you want to reproduce?")
        requested_run_id = input()
        run_id_query = db.session.query(db.Workflow).filter(db.Workflow.run_id == requested_run_id)
        if run_id_query.count() == 0:
            print("Sorry. I can't find the run_id.")
            return None
        else:
            for workflow in run_id_query:
                run_id = workflow.run_id
    else:
        print("OK, I'll work on the latest workflow.")
        max_time_query = db.session.query(db.Workflow).filter().order_by(desc(db.Workflow.time_began)).limit(1)
        for workflow in max_time_query:
            run_id = workflow.run_id
    print('Run_id request valid...')
    return run_id
    
################################  Example  #####################################
# TODO: Prepare available cwl_CommandLineTools
echo1 = cwl_CommandLineTool('echo1', 'bash',[], [cwl_File(), cwl_string()], [cwl_File()])
echo2 = cwl_CommandLineTool('echo2', 'bash',[], [cwl_File(),cwl_string(),cwl_string(),cwl_string(),cwl_string(),cwl_string()],[cwl_File()])
untar = cwl_CommandLineTool('untar', 'tar',['xvf'], [cwl_File(),cwl_string(),cwl_string()],[cwl_File(),cwl_File()])
concat = cwl_CommandLineTool('concat','cat',[], [cwl_File(),cwl_File(),cwl_string()],[cwl_File()])
clts = [echo1, echo2, untar, concat]

# Query database to prepare rendering
db = Database()
# Find a workflow to reproduce according to the run_id
run_id = find_run_id()
# Make a query on Task table with selected run_id
Task = db.session.query(db.Task).filter(db.Task.run_id == run_id)

# Make an empty workflow with the specified run_id
auto_workflow_1 = cwl_Workflow(run_id, inputs=[], outputs=[],steps=[])
# Add elements to that workflow
add_indep_steps(Task, db, auto_workflow_1, clts)
add_dep_steps(Task, db, auto_workflow_1, clts)
add_inputs(auto_workflow_1)
add_outputs(auto_workflow_1)

# Render the workflow
render_cwl("auto_workflow.cwl", auto_workflow_1,clts)

