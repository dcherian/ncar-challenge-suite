from contextlib import contextmanager

import os
import dask

import dask_jobqueue
import distributed

@contextmanager
def get_cluster(*, n_workers, env=None, config=None):
    
    print(config)
    
    with dask.config.set(config):
        cluster = dask_jobqueue.PBSCluster(
                cores=2,  # The number of cores you want
                memory="23GB",  # Amount of memory
                processes=1,  # How many processes
                queue="casper",  # The type of queue to utilize (/glade/u/apps/dav/opt/usr/bin/execcasper)
                local_directory="/glade/scratch/dcherian/tmp/dask/",  # Use your local directory
                resource_spec="select=1:ncpus=2:mem=23GB",  # Specify resources
                account="ncgd0011",  # Input your project ID here
                walltime="02:00:00",  # Amount of wall time
                interface="ib0",  # Interface to use
                job_script_prologue=[f"export {k}={v}" for k, v in env.items()],

            )
        client = distributed.Client(cluster)        
        print(client.dashboard_link)
        # client.run_on_scheduler(lambda dask_scheduler: setattr(dask_scheduler, "WORKER_SATURATION", config["distributed.scheduler.worker-saturation"]))
        print("asking for workers....")
        cluster.scale(n_workers)
        print("waiting for workers....")
        client.wait_for_workers(n_workers)
    
    yield cluster, client
    
    print("closing")
    client.close()
    cluster.close()

from functools import partial


def check_config(dask_scheduler, config):
    
    print(dask_scheduler.WORKER_SATURATION)
    assert str(dask_scheduler.WORKER_SATURATION) == config["distributed.scheduler.worker-saturation"], (dask_scheduler.WORKER_SATURATION, config["distributed.scheduler.worker-saturation"])