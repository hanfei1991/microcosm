# Structure of Master

## Structure

The master module consists of several components:

- Master Server.
  - provide grpc/http api, including
    - operate cluster
      - register/delete server
      - put heartbeat (keep alive)
    - user interface
      - submit job
      - update job
      - delete job
  - ExecutorManager
    - Handle Heartbeat
    - Maintain the stats of Executors
      - Notify Resource Manager Udpate the status infos
    - Once an executor is offline, it should notify the resource manager to reschedule all the tasks on it.
- ResourceManager
  - ResourceManager maintains the status of all executors and tasks.
  - It implements several interfaces, including:
    - GetResourceShapshot
    - ApplyNewTasks
    - UpdateExecutorStats
    - Register/UnRegister Executor
    - GetRescheduleTxn
  - The Cost Model is supposed to have two types
    - Expected Usage
    - Real Usage
  - The Occupied resources in `Resource Manager` should be `sum(max(expected usage, real usage)`. The real usage will be updated by hearbeat.
- JobManager
  - Receive SubmitJob Request, Check the type of Job, Create the JobMaster.
  - JobMaster (per job)
    - Generate the Tasks for the job
    - Schedule and Dispatch Tasks
      1. Receive the Dispatch Job Request
      2. Get `ResourceSnapshot` from Resource Manager
      3. Assign tasks to different executors, get `SubJob - Tasks - Executor` tuple.
      4. Dispatch the tasks, but not run.
         - If failed, go back to step 2.
         - If success, Update HA Store.
      5. Start working thread.
  - Periodically Check Reschedule
