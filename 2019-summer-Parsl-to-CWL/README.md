# Parsl-to-CWL
This is a repository where I try to use Parsl's monitoring function to reproduce a Parsl program's progress in CWL

## Demo
To go through a whole procedure of how my code works, here are the steps.
1. Clone my repository to an environment which supports Python, Parsl, as well as CWL. 
2. Run the Python script named workflow.py, which contains definitions of several bash_apps and calls to them to produce a workflow. The outputs of different steps are stored in different `*.txt` files, and the result of the final step is stored in `result.txt`. Moreover, monitoring.db contains task information after running the script.
3. Run the Python script named generator.py, which will interpret monitoring.db and automatically write a CWL workflow that imitates the Parsl workflow and produces the same result.
4. To run the CWL workflow, type `cwl-runner auto_workflow.cwl auto_workflow.yml`.
## How to utilize this repository
1. `generator.py` is used to produce a CWL workflow, assuming a monitoring.db exists in the same directory as it does. 
2. To successfully run `generator.py`, you need to modify the script where `clt` is defined. You need to specify the cwl_CommandLineTools in the way that they do the same thing as bash_apps.
3. To run the CWL workflow, you need to write the CWL CommandLineTools as a `*.cwl` file so that when the workflow calls it, no error pops up.
