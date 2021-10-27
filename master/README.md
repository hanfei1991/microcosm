# Structure of Master

The master module consists of several components:

- Master Server (and HA Store)
- Scheduler
  - Collect and maintain the stats of executors. stats including
    - Workload of Executor, include grouroutine counts, memory usage, etc...
    - The status of Executor, such as `Running`, `Busy`, `Disconnected`, `Initializing`, `ShutDown`
  - Schedule Job and Tasks
    1. Receive the Schedule Job Request
    2. Store the Job/SubJob Information in `HA Store`
    3. Assign tasks to different executors, get `SubJob - Tasks - Executor` tuple.
    4. Dispatch the tasks, but not run.
       - If failed, go back to step 3.
       - If success, Update HA Store.
    5. Notify JobMaster the schedule result.
    6. Launch the tasks.
       - If failed, Update HA Strore and go back to step 3
       - If succeed, Update HA Store
- JobManager
  - Receive SubmitJob Request, Check the type of Job, Create the JobMaster.
  - JobMaster (per job)
    - Generate the Tasks for the job
    - Notify Scheduler to Schedule and Dispatch the Job.
    - Get the Schedule Result, and Start work goroutine to watch the status of tasks.
    - Notify Scheduelr to Launch the tasks.

## Some undecided problems

- How to implement KeepAlive? (grpc or etcd)
- How to identify Executor? (id or name)
- When should we use `p2p` server and `grpc` server? 

## Master Server

- provide grpc/http api, including
  - operate cluster
    - register/delete server
    - put heartbeat (keep alive)
  - user interface
    - submit job
    - update job
    - delete job

`Master Server` is always deployed as three instanses to ganrantee HA. Executor have to identify the owner address in time.

## HA Store

`Master Server` have to start a HA store to persistance the cluster info. 
