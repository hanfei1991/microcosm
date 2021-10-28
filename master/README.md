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
- **ResourceManager**. It implements several interfaces, including:
  - GetResourceShapshot
  - ApplyNewTasks
  - UpdateExecutorStats
  - Register/UnRegister Executor
  - GetRescheduleTxn
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
      6. Launch the tasks.
         - If failed, Update HA Strore and go back to step 2
         - If succeed, Update HA Store
      7. Update Resource Manager
  - Periodically Check Reschedule

## Process of Reschedule

1. JobManager periodically ask `Resource Manager` if we need to reschedule.
2. If need, Resource Manager will return a `Reschedule Txn` to move a (or some) task in a Job and a `Resource Snapshot`.
3. Notify JobMaster to begin reschedule.
4. JobMaster pick an executor to execute.
5. Dispatch task to the executor
   - if failed, return to 4.
   - if succeed, Kill the original task.
6. Launch the new task.
   - if failed, clean the new task, return to 4.

## Some undecided problems

- How to design a unified rpc service ?
- How to classify the `Expected Used Resource` and `Real Used Resource` ?
