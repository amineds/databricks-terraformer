from click.testing import CliRunner
from databricks_terraformer.cli import cli
import re


git_repo = 'git@github.com:itaiw/export-repo.git'
source_profile = 'dr_source'

# databricks-terraformer -v debug instance-profiles export --hcl --profile demo -g git@github.com:itaiw/export-repo.git
# databricks-terraformer -v debug secret-scopes export --hcl --profile dr_source -g git@github.com:itaiw/export-repo.git
# databricks-terraformer -v debug secrets export --hcl --profile dr_source -g git@github.com:itaiw/export-repo.git
# databricks-terraformer -v debug secret-acls export --hcl --profile dr_source -g git@github.com:itaiw/export-repo.git

runs = {'instance-pools':
           {'args':None,
            'object_count':2,
            'pattern': 'Writing instance_pools to path'
            },
        'cluster-policies':
           {'args':None,
            'object_count':2,
            'pattern': 'Writing cluster_policies to path'
            },
        'jobs':
           {'args':None,
            'object_count':1,
            'pattern': 'Writing jobs to path'
            },
        'notebooks':
           {'args':["--notebook-path" ,"/Shared"],
            # notebooks count double, the hcl and the file
            'object_count':6,
            'pattern': 'Writing notebooks to path'
            },
        'dbfs':
           {'args':["--dbfs-path" ,"/databricks/init"],
            # DBFS count double, the hcl and the file
            'object_count':16,
            'pattern': 'Writing dbfs to path'
            },
       }

runner = CliRunner()

for run,params in runs.items():
    print(run)
    print(params)
    if params['args'] is None:
        result = runner.invoke(cli,args=[run, 'export', '--hcl', '--profile',source_profile, '-g', git_repo],
                               prog_name="databricks-terraformer")
    else:
        result = runner.invoke(cli, args=[run, 'export', '--hcl', '--profile', source_profile, '-g', git_repo,params['args'][0],params['args'][1]],
                               prog_name="databricks-terraformer")
    if result.exit_code != 0:
        print(result.stdout)
    assert result.exit_code == 0
    assert len(re.findall(params['pattern'], result.stdout)) == params['object_count'],\
        f"expor {run} found {len(re.findall(params['pattern'], result.stdout))} objects expected {params['object_count']}"
